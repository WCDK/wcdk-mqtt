package com.wcdk.mqtt.cluster;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.util.concurrent.Queues;

/***
* 集群对等连接封装，持有该连接的出站汇聚点并负责报文编码写出。
* @author wcdk
***/
public class MqttClusterPeer {

    private static final Logger log = LoggerFactory.getLogger(MqttClusterPeer.class);

    /** 集群通道出站缓冲上限（帧数），防止对端节点消费过慢导致内存膨胀 */
    private static final int OUTBOUND_BUFFER_LIMIT = 8192;

    private final Connection connection;
    private final MqttClusterManager manager;
    private final ObjectMapper objectMapper;
    private final String localNodeId;
    /** 本节点主动拨号建立返回 true，被动接受返回 false */
    private final boolean dialed;
    /** 拨号目标地址 host:port，接受连接时为空 */
    private final String targetAddress;
    /** 出站汇聚点：有界缓冲，写满时断开该对等连接 */
    private final Sinks.Many<io.netty.buffer.ByteBuf> outboundSink =
            Sinks.many().unicast().onBackpressureBuffer(Queues.<io.netty.buffer.ByteBuf>get(OUTBOUND_BUFFER_LIMIT).get());

    /** 远端节点 ID，收到 HELLO 后填充 */
    private volatile String remoteNodeId;
    /** 远端公布的连接地址 host:port，收到 HELLO 后填充 */
    private volatile String advertisedAddress;
    /** 最后一次收到远端报文的时间 */
    private volatile long lastSeenMillis = System.currentTimeMillis();
    /** 是否已关闭 */
    private volatile boolean closed;

    MqttClusterPeer(Connection connection,
                    MqttClusterManager manager,
                    ObjectMapper objectMapper,
                    String localNodeId,
                    boolean dialed,
                    String targetAddress) {
        this.connection = connection;
        this.manager = manager;
        this.objectMapper = objectMapper;
        this.localNodeId = localNodeId;
        this.dialed = dialed;
        this.targetAddress = targetAddress;
    }

    public boolean isDialed() {
        return dialed;
    }

    public String targetAddress() {
        return targetAddress;
    }

    public String remoteNodeId() {
        return remoteNodeId;
    }

    public String advertisedAddress() {
        return advertisedAddress;
    }

    public long lastSeenMillis() {
        return lastSeenMillis;
    }

    public boolean isClosed() {
        return closed;
    }

    /***
    * 刷新最后活跃时间。
    * @author wcdk
    ***/
    void touch() {
        this.lastSeenMillis = System.currentTimeMillis();
    }

    void setRemoteIdentity(String remoteNodeId, String advertisedAddress) {
        this.remoteNodeId = remoteNodeId;
        this.advertisedAddress = advertisedAddress;
    }

    Sinks.Many<io.netty.buffer.ByteBuf> outboundSink() {
        return outboundSink;
    }

    Connection connection() {
        return connection;
    }

    /***
    * 发送一条集群事件，编码为一行 JSON 字节。
    * @author wcdk
    * @param type 事件类型
    * @param payload 事件负载
    * @return 入队成功返回 true
    ***/
    public boolean send(ClusterEventType type, Map<String, Object> payload) {
        if (closed) {
            return false;
        }
        ClusterEvent event = new ClusterEvent(type, localNodeId, payload);
        byte[] line;
        try {
            line = objectMapper.writeValueAsBytes(event);
        } catch (Exception ex) {
            log.warn("集群事件序列化失败，事件类型：{}，异常信息：{}", type, ex.getMessage());
            return false;
        }
        // 末尾补换行符作为帧分隔
        byte[] frame = java.util.Arrays.copyOf(line, line.length + 1);
        frame[line.length] = '\n';
        Sinks.EmitResult result = outboundSink.tryEmitNext(Unpooled.wrappedBuffer(frame));
        // 序列化包装被其他线程短暂持锁时重试，避免事件因并发竞争被丢弃
        for (int attempt = 0; attempt < 3 && result == Sinks.EmitResult.FAIL_NON_SERIALIZED; attempt++) {
            Thread.yield();
            result = outboundSink.tryEmitNext(Unpooled.wrappedBuffer(frame));
        }
        if (result == Sinks.EmitResult.FAIL_OVERFLOW) {
            log.warn("集群出站缓冲写满，断开对等连接，远端节点：{}，缓冲上限：{}帧",
                    remoteNodeId, OUTBOUND_BUFFER_LIMIT);
            close();
            return false;
        }
        if (result.isFailure()) {
            log.debug("集群事件写出失败，事件类型：{}，远端节点：{}，原因：{}", type, remoteNodeId, result);
            return false;
        }
        return true;
    }

    /***
    * 发送握手 HELLO 报文，配置了集群凭据时一并携带用于对端校验。
    * @author wcdk
    ***/
    void sendHello() {
        send(ClusterEventType.HELLO, helloPayload());
    }

    /***
    * 发送握手应答，凭据与 HELLO 保持一致。
    * @author wcdk
    ***/
    void sendHelloAck() {
        send(ClusterEventType.HELLO_ACK, helloPayload());
    }

    /***
    * 构造握手负载：节点身份加可选的集群认证凭据。
    * @author wcdk
    * @return 握手负载
    ***/
    private Map<String, Object> helloPayload() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("nodeId", localNodeId);
        payload.put("advertiseAddress", manager.advertisedAddress());
        String username = manager.clusterUsername();
        if (username != null) {
            payload.put("username", username);
            payload.put("password", manager.clusterPassword());
        }
        return payload;
    }

    /***
    * 发送心跳报文。
    * @author wcdk
    ***/
    void sendHeartbeat() {
        send(ClusterEventType.HEARTBEAT, Map.of("timestamp", System.currentTimeMillis()));
    }

    /***
    * 关闭本连接。
    * @author wcdk
    ***/
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            connection.dispose();
        } catch (RuntimeException ex) {
            log.debug("关闭集群连接异常，远端节点：{}，异常信息：{}", remoteNodeId, ex.getMessage());
        }
    }

    /***
    * 标记连接关闭（由读链路终止时调用）。
    * @author wcdk
    ***/
    void markClosed() {
        this.closed = true;
    }

    @Override
    public String toString() {
        return "MqttClusterPeer{remoteNodeId='" + remoteNodeId + "', dialed=" + dialed
                + ", address=" + advertisedAddress + "}";
    }
}
