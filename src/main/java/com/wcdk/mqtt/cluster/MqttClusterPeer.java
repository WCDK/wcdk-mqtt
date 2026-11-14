package com.wcdk.mqtt.cluster;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import lombok.Getter;
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
    @Getter
    private final boolean dialed;
    /** 拨号目标地址 host:port，接受连接时为空 */
    private final String targetAddress;
    /** 出站汇聚点：有界缓冲，写满时断开该对等连接 */
    private final Sinks.Many<io.netty.buffer.ByteBuf> outboundSink =
            Sinks.many().unicast().onBackpressureBuffer(Queues.<io.netty.buffer.ByteBuf>get(OUTBOUND_BUFFER_LIMIT).get());
    /**
     * 出站有序队列：JSON/Base64 编码在线程池执行，队列保证同一连接的发送顺序，
     * 避免编码工作占用 Reactor 事件循环。
     */
    private final SerialTaskQueue outboundQueue;

    /** 远端节点 ID，收到 HELLO 后填充 */
    private volatile String remoteNodeId;
    /** 远端公布的连接地址 host:port，收到 HELLO 后填充 */
    private volatile String advertisedAddress;
    /** 最后一次收到远端报文的时间 */
    private volatile long lastSeenMillis = System.currentTimeMillis();
    /** 是否已关闭 */
    @Getter
    private volatile boolean closed;
    /**
     * 出站写入串行化锁：unicast Sink 不保证多生产者线程安全，
     * 心跳、发布、会话同步、管理指令可能来自不同执行体，必须单写者化。
     */
    private final Object sendLock = new Object();

    MqttClusterPeer(Connection connection,
                    MqttClusterManager manager,
                    ObjectMapper objectMapper,
                    String localNodeId,
                    boolean dialed,
                    String targetAddress,
                    Executor outboundExecutor,
                    int queueCapacity) {
        this.connection = connection;
        this.manager = manager;
        this.objectMapper = objectMapper;
        this.localNodeId = localNodeId;
        this.dialed = dialed;
        this.targetAddress = targetAddress;
        this.outboundQueue = new SerialTaskQueue(outboundExecutor, queueCapacity,
                "peer-out-" + System.identityHashCode(this));
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
    * 事件先进入连接级有序队列，由共享线程池完成 JSON/Base64 编码后写出，
    * 因此返回 true 仅表示“已接受发送”，不表示对端已收到。
    * @author wcdk
    * @param type 事件类型
    * @param payload 事件负载
    * @return 入队成功返回 true
    ***/
    public boolean send(ClusterEventType type, Map<String, Object> payload) {
        return send(type, () -> payload);
    }

    /***
    * 发送一条集群事件，负载延迟到编码线程构造，避免 Base64/快照序列化占用调用线程。
    * @author wcdk
    * @param type 事件类型
    * @param payloadSupplier 负载构造器，执行时不得返回 null
    * @return 入队成功返回 true
    ***/
    public boolean send(ClusterEventType type, Supplier<Map<String, Object>> payloadSupplier) {
        if (closed || type == null || payloadSupplier == null) {
            return false;
        }
        boolean accepted = outboundQueue.submit(() -> encodeAndEmit(type, payloadSupplier));
        if (!accepted && !closed) {
            log.warn("集群出站队列已满，断开对等连接，远端节点：{}，队列任务数：{}",
                    remoteNodeId, outboundQueue.size());
            close();
        }
        return accepted;
    }

    /***
    * 在线程池中构造事件负载并编码为单行 JSON。
    * @author wcdk
    * @param type 事件类型
    * @param payloadSupplier 负载构造器
    ***/
    private void encodeAndEmit(ClusterEventType type, Supplier<Map<String, Object>> payloadSupplier) {
        if (closed) {
            return;
        }
        Map<String, Object> payload;
        try {
            payload = payloadSupplier.get();
        } catch (RuntimeException ex) {
            log.warn("集群事件负载构造失败，事件类型：{}，异常信息：{}", type, ex.getMessage());
            return;
        }
        ClusterEvent event = new ClusterEvent(type, localNodeId, payload);
        byte[] line;
        try {
            line = objectMapper.writeValueAsBytes(event);
        } catch (Exception ex) {
            log.warn("集群事件序列化失败，事件类型：{}，异常信息：{}", type, ex.getMessage());
            return;
        }
        // 末尾补换行符作为帧分隔
        byte[] frame = Arrays.copyOf(line, line.length + 1);
        frame[line.length] = '\n';
        emitFrame(type, Unpooled.wrappedBuffer(frame));
    }

    /***
    * 把编码后的帧写入出站汇聚点，失败时释放缓冲。
    * @author wcdk
    * @param type 事件类型
    * @param buffer 编码后的帧
    ***/
    private void emitFrame(ClusterEventType type, io.netty.buffer.ByteBuf buffer) {
        // 单写者串行化：unicast Sink 不保证多生产者线程安全
        synchronized (sendLock) {
            if (closed) {
                buffer.release();
                return;
            }
            Sinks.EmitResult result = outboundSink.tryEmitNext(buffer);
            // 串行化后仍偶发非序列化失败时短暂重试，避免事件因竞争被丢弃
            for (int attempt = 0; attempt < 10 && result == Sinks.EmitResult.FAIL_NON_SERIALIZED; attempt++) {
                Thread.yield();
                result = outboundSink.tryEmitNext(buffer);
            }
            if (result.isSuccess()) {
                return;
            }
            // 未入队的缓冲必须释放，避免包装内存泄漏
            buffer.release();
            if (result == Sinks.EmitResult.FAIL_OVERFLOW) {
                log.warn("集群出站缓冲写满，断开对等连接，远端节点：{}，缓冲上限：{}帧",
                        remoteNodeId, OUTBOUND_BUFFER_LIMIT);
                close();
            } else {
                log.warn("集群事件写出失败，事件类型：{}，远端节点：{}，原因：{}", type, remoteNodeId, result);
            }
        }
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
        outboundQueue.close();
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
        outboundQueue.close();
    }

    @Override
    public String toString() {
        return "MqttClusterPeer{remoteNodeId='" + remoteNodeId + "', dialed=" + dialed
                + ", address=" + advertisedAddress + "}";
    }
}
