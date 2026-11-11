package com.wcdk.mqtt.cluster;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.core.MqttBrokerMessage;
import com.wcdk.mqtt.core.MqttSession;
import com.wcdk.mqtt.core.MqttSessionListener;
import com.wcdk.mqtt.core.MqttSessionRegistry;
import com.wcdk.mqtt.core.MqttTopicMatcher;
import io.netty.channel.ChannelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.netty.tcp.TcpClient;

/***
* MQTT 集群管理器：所有节点对等，无主从之分，负责对等连接维护、心跳保活、事件路由与状态同步。
* @author wcdk
***/
@Component
public class MqttClusterManager implements SmartLifecycle, MqttSessionListener {

    private static final Logger log = LoggerFactory.getLogger(MqttClusterManager.class);

    private final MqttProperties properties;
    private final MqttSessionRegistry registry;
    private final ObjectMapper objectMapper;

    /** 对等节点按 nodeId 索引的活动连接 */
    private final ConcurrentMap<String, MqttClusterPeer> peers = new ConcurrentHashMap<>();
    /** 已知节点状态视图，断开后保留用于控制台展示 */
    private final ConcurrentMap<String, ClusterNodeState> nodeStates = new ConcurrentHashMap<>();
    /** 探测到的本机地址集合，避免自我连接反复重拨 */
    private final Set<String> selfAddresses = ConcurrentHashMap.newKeySet();

    private final MqttClusterServer clusterServer;

    private volatile String nodeId;
    private volatile String advertisedAddress;
    private volatile ScheduledExecutorService scheduler;
    private volatile boolean running;

    public MqttClusterManager(MqttProperties properties,
                              MqttSessionRegistry registry,
                              ObjectMapper objectMapper) {
        this.properties = properties;
        this.registry = registry;
        this.objectMapper = objectMapper;
        this.clusterServer = new MqttClusterServer(properties, this);
        // 注册为会话监听器，把本节点会话变更同步给对等节点
        registry.addSessionListener(this);
    }

    /***
    * 启动集群通道与对等拨号扫描。
    * @author wcdk
    ***/
    @Override
    public void start() {
        if (!properties.getCluster().isEnabled()) {
            log.info("集群功能已在配置中关闭，当前节点以单实例模式运行");
            return;
        }
        resolveNodeId();
        resolveAdvertisedAddress();
        clusterServer.bind();
        // 单守护线程周期扫描未覆盖的对等地址并重拨
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "mqtt-cluster-redial");
            thread.setDaemon(true);
            return thread;
        });
        long reconnectDelay = Math.max(1000L, properties.getCluster().getReconnectDelayMillis());
        scheduler.scheduleWithFixedDelay(this::redialSweep, reconnectDelay, reconnectDelay, TimeUnit.MILLISECONDS);
        running = true;
        // 启动后立即执行一轮拨号，不等待首个周期
        redialSweep();
        log.info("集群功能启动成功，节点ID：{}，集群端口：{}，公布地址：{}，对等节点数：{}",
                nodeId, clusterServer.port(), advertisedAddress, properties.getCluster().getPeers().size());
    }

    /***
    * 停止集群，关闭全部对等连接与拨号线程。
    * @author wcdk
    ***/
    @Override
    public void stop() {
        running = false;
        ScheduledExecutorService currentScheduler = scheduler;
        if (currentScheduler != null) {
            currentScheduler.shutdownNow();
            scheduler = null;
        }
        peers.values().forEach(MqttClusterPeer::close);
        peers.clear();
        nodeStates.values().forEach(state -> state.setConnected(false));
        clusterServer.dispose();
        log.info("集群功能已停止");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public String nodeId() {
        return nodeId;
    }

    public String advertisedAddress() {
        return advertisedAddress;
    }

    /***
    * 集群通道认证用户名，未配置返回 null（不启用认证）。
    * @author wcdk
    * @return 用户名
    ***/
    String clusterUsername() {
        String username = properties.getCluster().getUsername();
        return username == null || username.isBlank() ? null : username;
    }

    /***
    * 集群通道认证密码，未配置返回空串。
    * @author wcdk
    * @return 密码
    ***/
    String clusterPassword() {
        String password = properties.getCluster().getPassword();
        return password == null ? "" : password;
    }

    /***
    * 返回对等节点状态快照列表，供管理接口展示。
    * @author wcdk
    * @return 节点状态列表
    ***/
    public List<ClusterNodeState> nodes() {
        return new ArrayList<>(nodeStates.values());
    }

    public int connectedPeerCount() {
        int count = 0;
        for (MqttClusterPeer peer : peers.values()) {
            if (!peer.isClosed()) {
                count++;
            }
        }
        return count;
    }

    /***
    * 入站集群连接处理（被动接受的连接）。
    * @author wcdk
    * @param inbound 入站通道
    * @param outbound 出站通道
    * @return 读写组合 Mono
    ***/
    public Mono<Void> handleConnection(NettyInbound inbound, NettyOutbound outbound) {
        return wire(inbound, outbound, false, null);
    }

    /***
    * 建立单条对等连接的读写链路：先发握手，读入按行解析事件，结束后注销对等节点。
    * @author wcdk
    * @param inbound 入站通道
    * @param outbound 出站通道
    * @param dialed 是否为本节点主动拨号
    * @param targetAddress 拨号目标地址 host:port
    * @return 读写组合 Mono
    ***/
    Mono<Void> wire(NettyInbound inbound, NettyOutbound outbound, boolean dialed, String targetAddress) {
        Connection[] holder = new Connection[1];
        inbound.withConnection(connection -> holder[0] = connection);
        Connection connection = holder[0];
        MqttClusterPeer peer = new MqttClusterPeer(connection, this, objectMapper, nodeId, dialed, targetAddress);

        // 心跳靠写空闲触发，超时靠读空闲断链，均无需额外订阅
        long heartbeatInterval = Math.max(1000L, properties.getCluster().getHeartbeatIntervalMillis());
        long nodeTimeout = Math.max(heartbeatInterval * 2, properties.getCluster().getNodeTimeoutMillis());
        connection.onWriteIdle(heartbeatInterval, peer::sendHeartbeat);
        connection.onReadIdle(nodeTimeout, () -> {
            log.warn("集群节点心跳超时断开连接，节点ID：{}，超时毫秒：{}", peer.remoteNodeId(), nodeTimeout);
            peer.close();
        });

        // 连接建立即发起握手
        peer.sendHello();

        Mono<Void> writeMono = outbound.sendObject(peer.outboundSink().asFlux())
                .then()
                .doFinally(signalType -> peer.close());
        Mono<Void> readMono = inbound.receive()
                .asString(StandardCharsets.UTF_8)
                .doOnNext(line -> handleLine(peer, line))
                .doFinally(signalType -> {
                    peer.outboundSink().tryEmitComplete();
                    onPeerClosed(peer);
                })
                .then();
        return Mono.when(writeMono, readMono);
    }

    /***
    * 解析并路由一行集群事件，解析失败仅告警不影响连接。
    * @author wcdk
    * @param peer 所属对等连接
    * @param line 一行 JSON 文本
    ***/
    private void handleLine(MqttClusterPeer peer, String line) {
        if (line == null || line.isBlank()) {
            return;
        }
        peer.touch();
        ClusterEvent event;
        try {
            event = objectMapper.readValue(line, ClusterEvent.class);
        } catch (Exception ex) {
            log.warn("集群事件解析失败，远端节点：{}，异常信息：{}", peer.remoteNodeId(), ex.getMessage());
            return;
        }
        if (event == null || event.type() == null) {
            return;
        }
        try {
            routeEvent(peer, event);
        } catch (RuntimeException ex) {
            log.warn("集群事件处理失败，事件类型：{}，远端节点：{}，异常信息：{}",
                    event.type(), peer.remoteNodeId(), ex.getMessage());
        }
    }

    /***
    * 按事件类型分发处理。
    * @author wcdk
    * @param peer 对等连接
    * @param event 事件
    ***/
    private void routeEvent(MqttClusterPeer peer, ClusterEvent event) {
        Map<String, Object> payload = event.payload();
        switch (event.type()) {
            case HELLO, HELLO_ACK -> handleHello(peer, payload, event.type());
            case HEARTBEAT -> {
                ClusterNodeState state = nodeStates.get(peer.remoteNodeId());
                if (state != null) {
                    state.setLastSeenAt(Instant.now());
                }
            }
            case PUBLISH -> handleClusterPublish(payload);
            case SESSION_UPSERT -> handleSessionUpsert(payload);
            case SESSION_CLAIM -> handleSessionClaim(payload);
            case SESSION_REMOVE -> handleSessionRemove(payload);
            case KICK -> handleKick(payload);
            case RETAINED_SYNC -> handleRetainedSync(payload);
            case OFFLINE_CLEAR -> {
                Object clientId = payload.get("clientId");
                if (clientId != null) {
                    registry.clearPendingOffline(clientId.toString());
                }
            }
            default -> log.debug("忽略未处理的集群事件类型：{}", event.type());
        }
    }

    /***
    * 处理握手报文：登记远端身份，按规则消解重复连接，仅对 HELLO 回复应答避免互相刷屏。
    * @author wcdk
    * @param peer 对等连接
    * @param payload 握手负载
    * @param eventType 事件类型
    ***/
    private void handleHello(MqttClusterPeer peer, Map<String, Object> payload, ClusterEventType eventType) {
        Object nodeIdValue = payload.get("nodeId");
        if (nodeIdValue == null || nodeIdValue.toString().isBlank()) {
            log.warn("集群握手缺少节点 ID，远端地址：{}", peer.advertisedAddress());
            peer.close();
            return;
        }
        if (!authenticatePeer(payload)) {
            log.warn("集群握手认证失败已拒绝连接，远端节点：{}，远端地址：{}",
                    nodeIdValue, peer.advertisedAddress());
            peer.close();
            return;
        }
        String remoteNodeId = nodeIdValue.toString();
        String advertised = payload.get("advertiseAddress") == null ? null : payload.get("advertiseAddress").toString();
        if (remoteNodeId.equals(nodeId)) {
            // 拨到自己：记录为自地址，避免重复重拨
            log.debug("检测到本机自我连接，已关闭，目标地址：{}", peer.targetAddress());
            if (peer.targetAddress() != null) {
                selfAddresses.add(peer.targetAddress());
            }
            peer.close();
            return;
        }
        boolean firstRegistration = peer.remoteNodeId() == null;
        peer.setRemoteIdentity(remoteNodeId, advertised);
        if (eventType == ClusterEventType.HELLO) {
            peer.sendHelloAck();
        }

        if (!registerPeer(peer)) {
            return;
        }
        // 新会话建立后同步本节点全量状态
        if (firstRegistration) {
            sendStateSync(peer);
        }
    }

    /***
    * 登记对等连接，重复连接按「节点 ID 较小方保留自己拨出的连接」消解。
    * @author wcdk
    * @param peer 待登记连接
    * @return 登记成功返回 true（可继续使用该连接）
    ***/
    private boolean registerPeer(MqttClusterPeer peer) {
        String remoteNodeId = peer.remoteNodeId();
        if (remoteNodeId == null) {
            return false;
        }
        MqttClusterPeer existing = peers.get(remoteNodeId);
        if (existing != null && existing != peer && !existing.isClosed()) {
            boolean keepSelfDialed = nodeId.compareTo(remoteNodeId) < 0;
            if (peer.isDialed() != keepSelfDialed) {
                log.info("发现重复的集群连接，丢弃后建立的一条，节点ID：{}，本方主动拨号：{}",
                        remoteNodeId, peer.isDialed());
                peer.close();
                return false;
            }
            log.info("发现重复的集群连接，替换既有连接，节点ID：{}", remoteNodeId);
            existing.close();
        }
        boolean replaced = peers.put(remoteNodeId, peer) != peer;
        ClusterNodeState state = nodeStates.computeIfAbsent(remoteNodeId, id -> {
            ClusterNodeState created = new ClusterNodeState();
            created.setNodeId(id);
            return created;
        });
        state.setAddress(peer.advertisedAddress());
        state.setConnected(true);
        state.setDialed(peer.isDialed());
        state.setLastSeenAt(Instant.now());
        if (state.getConnectedAt() == null || replaced) {
            state.setConnectedAt(Instant.now());
        }
        log.info("集群节点已连接，节点ID：{}，地址：{}，本方主动拨号：{}",
                remoteNodeId, peer.advertisedAddress(), peer.isDialed());
        return true;
    }

    /***
    * 对等连接终止时注销节点状态。
    * @author wcdk
    * @param peer 结束的连接
    ***/
    private void onPeerClosed(MqttClusterPeer peer) {
        peer.markClosed();
        String remoteNodeId = peer.remoteNodeId();
        if (remoteNodeId == null || !peers.remove(remoteNodeId, peer)) {
            return;
        }
        ClusterNodeState state = nodeStates.get(remoteNodeId);
        if (state != null) {
            state.setConnected(false);
        }
        log.info("集群节点连接断开，节点ID：{}", remoteNodeId);
    }

    /***
    * 向新连接同步保留消息与本地会话快照。
    * @author wcdk
    * @param peer 对等连接
    ***/
    private void sendStateSync(MqttClusterPeer peer) {
        List<MqttBrokerMessage> retained = registry.retainedStore().findAll();
        if (!retained.isEmpty()) {
            List<Map<String, Object>> items = new ArrayList<>();
            for (MqttBrokerMessage message : retained) {
                items.add(messagePayload(message));
            }
            peer.send(ClusterEventType.RETAINED_SYNC, Map.of("messages", items));
        }
        for (MqttSession session : registry.sessions()) {
            peer.send(ClusterEventType.SESSION_UPSERT, Map.of("snapshot", session.snapshot()));
        }
        log.debug("已向节点 {} 同步状态，保留消息：{}，本地会话：{}",
                peer.remoteNodeId(), retained.size(), registry.sessionCount());
    }

    /***
    * 处理来自集群的发布消息，仅本地投递不再回环广播。
    * @author wcdk
    * @param payload 事件负载
    ***/
    private void handleClusterPublish(Map<String, Object> payload) {
        MqttBrokerMessage message = decodeMessage(payload);
        if (message == null) {
            return;
        }
        Object target = payload.get("targetClientId");
        if (target != null && !target.toString().isBlank()) {
            deliverToClient(target.toString(), message);
            return;
        }
        if (!MqttTopicMatcher.isValidTopicName(message.topic())) {
            return;
        }
        registry.deliver(message);
    }

    /***
    * 点对点投递给指定客户端。
    * @author wcdk
    * @param clientId 目标客户端 ID
    * @param message 消息
    ***/
    private void deliverToClient(String clientId, MqttBrokerMessage message) {
        MqttSession session = registry.findByClientId(clientId);
        if (session == null) {
            return;
        }
        int subscriptionQos = session.maxSubscriptionQos(message.topic());
        int deliveryQos = subscriptionQos < 0
                ? message.qos()
                : MqttTopicMatcher.minQos(message.qos(), subscriptionQos);
        session.sendPublish(message, deliveryQos, false);
    }

    /***
    * 校验握手携带的集群凭据：本节点配置了用户名时要求对端提供一致的用户名与密码，
    * 未配置时放行（保持向后兼容，依赖网络隔离保障集群通道安全）。
    * @author wcdk
    * @param payload 握手负载
    * @return 认证通过返回 true
    ***/
    private boolean authenticatePeer(Map<String, Object> payload) {
        String expectedUsername = clusterUsername();
        if (expectedUsername == null) {
            return true;
        }
        Object username = payload.get("username");
        if (!expectedUsername.equals(username == null ? null : username.toString())) {
            return false;
        }
        Object password = payload.get("password");
        return clusterPassword().equals(password == null ? null : password.toString());
    }

    /***
    * 处理会话状态同步，更新远端会话快照。
    * @author wcdk
    * @param payload 事件负载
    ***/
    private void handleSessionUpsert(Map<String, Object> payload) {
        Object snapshotValue = payload.get("snapshot");
        if (snapshotValue == null) {
            return;
        }
        MqttSession.SessionSnapshot snapshot =
                objectMapper.convertValue(snapshotValue, MqttSession.SessionSnapshot.class);
        if (snapshot == null || snapshot.getClientId() == null) {
            return;
        }
        registry.putRemoteSnapshot(snapshot);
        if (snapshot.isOnline()) {
            // 会话已在属主节点上线，本节点的离线缓冲交由属主节点负责
            registry.clearPendingOffline(snapshot.getClientId());
        }
    }

    /***
    * 处理会话归属声明：同一客户端已接入其他节点，本节点释放本地会话副本，
    * 避免残留的持久会话继续缓冲离线消息，导致客户端下次接入时重复投递。
    * @author wcdk
    * @param payload 事件负载
    ***/
    private void handleSessionClaim(Map<String, Object> payload) {
        Object clientIdValue = payload.get("clientId");
        if (clientIdValue == null) {
            return;
        }
        String clientId = clientIdValue.toString();
        MqttSession local = registry.findByClientId(clientId);
        if (local != null) {
            if (local.isActive()) {
                log.info("客户端已接入其他节点，本节点断开本地连接，客户端ID：{}", clientId);
                local.closeConnection();
            }
            registry.releaseForClaim(clientId);
            log.debug("已释放本地会话副本，交由属主节点维护，客户端ID：{}", clientId);
        }
    }

    /***
    * 处理会话清理通知，删除远端快照与离线缓冲。
    * @author wcdk
    * @param payload 事件负载
    ***/
    private void handleSessionRemove(Map<String, Object> payload) {
        Object clientIdValue = payload.get("clientId");
        if (clientIdValue != null) {
            registry.removeRemoteSnapshot(clientIdValue.toString());
        }
    }

    /***
    * 处理管理端强制下线指令。
    * @author wcdk
    * @param payload 事件负载
    ***/
    private void handleKick(Map<String, Object> payload) {
        Object clientIdValue = payload.get("clientId");
        if (clientIdValue == null) {
            return;
        }
        MqttSession local = registry.findByClientId(clientIdValue.toString());
        if (local != null) {
            log.info("收到集群下线指令，断开客户端，客户端ID：{}", clientIdValue);
            local.closeConnection();
        }
    }

    /***
    * 合并对等节点同步来的保留消息。
    * @author wcdk
    * @param payload 事件负载
    ***/
    private void handleRetainedSync(Map<String, Object> payload) {
        Object messagesValue = payload.get("messages");
        if (!(messagesValue instanceof List<?> items)) {
            return;
        }
        List<MqttBrokerMessage> merged = new ArrayList<>();
        for (Object item : items) {
            MqttBrokerMessage message = decodeMessage(objectMapper.convertValue(item,
                    new TypeReference<LinkedHashMap<String, Object>>() {
                    }));
            if (message != null) {
                merged.add(message);
            }
        }
        registry.retainedStore().merge(merged);
    }

    /***
    * 广播消息到全部对等节点，接收方仅本地投递。
    * @author wcdk
    * @param message 消息
    ***/
    public void broadcastPublish(MqttBrokerMessage message) {
        if (!running || message == null) {
            return;
        }
        Map<String, Object> payload = messagePayload(message);
        for (MqttClusterPeer peer : peers.values()) {
            peer.send(ClusterEventType.PUBLISH, payload);
        }
    }

    /***
    * 将消息点对点转发到持有目标客户端的节点。
    * @author wcdk
    * @param clientId 目标客户端 ID
    * @param message 消息
    * @return 转发成功返回 true
    ***/
    public boolean forwardToClient(String clientId, MqttBrokerMessage message) {
        if (!running || clientId == null || message == null) {
            return false;
        }
        MqttSession.SessionSnapshot snapshot = registry.remoteSnapshots().get(clientId);
        String ownerNodeId = snapshot == null ? null : snapshot.getOwnerNodeId();
        Map<String, Object> payload = new LinkedHashMap<>(messagePayload(message));
        payload.put("targetClientId", clientId);
        if (ownerNodeId != null) {
            MqttClusterPeer owner = peers.get(ownerNodeId);
            if (owner != null && !owner.isClosed()) {
                return owner.send(ClusterEventType.PUBLISH, payload);
            }
            return false;
        }
        // 属主未知时广播给所有节点，由持有该客户端的节点消费
        boolean sent = false;
        for (MqttClusterPeer peer : peers.values()) {
            sent = peer.send(ClusterEventType.PUBLISH, payload) || sent;
        }
        return sent;
    }

    /***
    * 强制下线客户端，本节点持有则直接断开，否则经集群转发。
    * @author wcdk
    * @param clientId 客户端 ID
    * @return 已处理返回 true
    ***/
    public boolean kickClient(String clientId) {
        MqttSession local = registry.findByClientId(clientId);
        if (local != null) {
            local.closeConnection();
            return true;
        }
        if (!running) {
            return false;
        }
        MqttSession.SessionSnapshot snapshot = registry.remoteSnapshots().get(clientId);
        String ownerNodeId = snapshot == null ? null : snapshot.getOwnerNodeId();
        if (ownerNodeId != null) {
            MqttClusterPeer owner = peers.get(ownerNodeId);
            if (owner != null) {
                return owner.send(ClusterEventType.KICK, Map.of("clientId", clientId));
            }
            return false;
        }
        boolean sent = false;
        for (MqttClusterPeer peer : peers.values()) {
            sent = peer.send(ClusterEventType.KICK, Map.of("clientId", clientId)) || sent;
        }
        return sent;
    }

    @Override
    public void onSessionChanged(MqttSession session) {
        if (!running) {
            return;
        }
        Map<String, Object> payload = Map.of("snapshot", session.snapshot());
        for (MqttClusterPeer peer : peers.values()) {
            if (session.isActive()) {
                // 先声明归属让对端释放同名本地会话，再同步快照，
                // 保证对端解除占用后能正确登记远端快照
                peer.send(ClusterEventType.SESSION_CLAIM, Map.of("clientId", session.clientId()));
                peer.send(ClusterEventType.OFFLINE_CLEAR, Map.of("clientId", session.clientId()));
            }
            peer.send(ClusterEventType.SESSION_UPSERT, payload);
        }
    }

    @Override
    public void onSessionRemoved(MqttSession session) {
        if (!running) {
            return;
        }
        Map<String, Object> payload = Map.of("clientId", session.clientId());
        for (MqttClusterPeer peer : peers.values()) {
            peer.send(ClusterEventType.SESSION_REMOVE, payload);
        }
    }

    /***
    * 周期扫描配置中的对等地址，未被覆盖时重新拨号。
    * @author wcdk
    ***/
    private void redialSweep() {
        if (!running) {
            return;
        }
        List<String> configuredPeers = properties.getCluster().getPeers();
        if (configuredPeers == null) {
            return;
        }
        for (String address : configuredPeers) {
            if (address == null || address.isBlank()) {
                continue;
            }
            String normalized = address.trim();
            if (selfAddresses.contains(normalized) || isSelfAddress(normalized)) {
                continue;
            }
            if (isCovered(normalized)) {
                continue;
            }
            dial(normalized);
        }
    }

    /***
    * 判断指定对等地址是否已有活动连接覆盖。
    * @author wcdk
    * @param address 对等地址 host:port
    * @return 已覆盖返回 true
    ***/
    private boolean isCovered(String address) {
        for (MqttClusterPeer peer : peers.values()) {
            if (peer.isClosed()) {
                continue;
            }
            if (peer.isDialed() && address.equals(peer.targetAddress())) {
                return true;
            }
            if (address.equals(peer.advertisedAddress())) {
                return true;
            }
        }
        return false;
    }

    /***
    * 判断指定对等地址是否指向本节点自身：公布地址一致，或端口为本节点集群端口
    * 且主机为回环地址、绑定地址或公布主机（同机不同端口的对等节点不算自身）。
    * @author wcdk
    * @param address 对等地址 host:port
    * @return 属于本节点返回 true
    ***/
    private boolean isSelfAddress(String address) {
        if (address.equals(advertisedAddress)) {
            return true;
        }
        int separator = address.lastIndexOf(':');
        if (separator <= 0 || separator == address.length() - 1) {
            return false;
        }
        String host = address.substring(0, separator);
        int port;
        try {
            port = Integer.parseInt(address.substring(separator + 1));
        } catch (NumberFormatException ex) {
            return false;
        }
        if (port != properties.getCluster().getBindPort()) {
            return false;
        }
        return isLoopbackHost(host)
                || host.equals(properties.getCluster().getBindHost())
                || host.equals(properties.getCluster().getAdvertiseHost());
    }

    private boolean isLoopbackHost(String host) {
        return "127.0.0.1".equals(host) || "localhost".equalsIgnoreCase(host) || "::1".equals(host);
    }

    /***
    * 主动拨号连接对等节点，失败仅告警等待下一轮。
    * @author wcdk
    * @param address 对等地址 host:port
    ***/
    private void dial(String address) {
        int separator = address.lastIndexOf(':');
        if (separator <= 0 || separator == address.length() - 1) {
            log.warn("对等节点地址格式非法，应为 host:port，当前值：{}", address);
            return;
        }
        String host = address.substring(0, separator);
        int port;
        try {
            port = Integer.parseInt(address.substring(separator + 1));
        } catch (NumberFormatException ex) {
            log.warn("对等节点端口格式非法，当前值：{}", address);
            return;
        }
        if (isSelfAddress(address)) {
            selfAddresses.add(address);
            return;
        }
        try {
            TcpClient.create()
                    .host(host)
                    .port(port)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                    .handle((inbound, outbound) -> wire(inbound, outbound, true, address))
                    .connectNow(java.time.Duration.ofSeconds(5));
            log.info("已发起集群拨号，目标地址：{}", address);
        } catch (RuntimeException ex) {
            log.debug("集群拨号失败，目标地址：{}，异常信息：{}", address, ex.getMessage());
        }
    }

    /***
    * 编码消息为集群传输负载，负载使用 Base64。
    * @author wcdk
    * @param message 消息
    * @return 负载 Map
    ***/
    private Map<String, Object> messagePayload(MqttBrokerMessage message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("messageId", message.messageId());
        payload.put("topic", message.topic());
        payload.put("payload", Base64.getEncoder().encodeToString(message.payload()));
        payload.put("qos", message.qos());
        payload.put("retained", message.retained());
        return payload;
    }

    private MqttBrokerMessage decodeMessage(Map<String, Object> payload) {
        if (payload == null) {
            return null;
        }
        Object topicValue = payload.get("topic");
        if (topicValue == null || topicValue.toString().isBlank()) {
            return null;
        }
        byte[] body;
        Object payloadValue = payload.get("payload");
        if (payloadValue == null || payloadValue.toString().isBlank()) {
            body = new byte[0];
        } else {
            try {
                body = Base64.getDecoder().decode(payloadValue.toString());
            } catch (IllegalArgumentException ex) {
                log.warn("集群消息负载解码失败，主题：{}，异常信息：{}", topicValue, ex.getMessage());
                return null;
            }
        }
        String messageId = payload.get("messageId") == null ? null : payload.get("messageId").toString();
        int qos = payload.get("qos") == null ? 0 : ((Number) payload.get("qos")).intValue();
        boolean retained = Boolean.TRUE.equals(payload.get("retained"));
        return new MqttBrokerMessage(messageId, topicValue.toString(), body, qos, retained);
    }

    /***
    * 解析或生成本节点 ID。
    * @author wcdk
    ***/
    private void resolveNodeId() {
        String configured = properties.getCluster().getNodeId();
        if (configured != null && !configured.isBlank()) {
            this.nodeId = configured.trim();
            return;
        }
        String generated = "node-" + java.util.UUID.randomUUID().toString().substring(0, 8);
        properties.getCluster().setNodeId(generated);
        this.nodeId = generated;
        log.info("未配置 node-id，已自动生成节点 ID：{}", generated);
    }

    /***
    * 推断本节点对外公布的地址。
    * @author wcdk
    ***/
    private void resolveAdvertisedAddress() {
        String configured = properties.getCluster().getAdvertiseHost();
        String host = configured;
        if (host == null || host.isBlank()) {
            String bindHost = properties.getCluster().getBindHost();
            if (bindHost != null && !"0.0.0.0".equals(bindHost) && !"::".equals(bindHost)) {
                host = bindHost;
            } else {
                try {
                    host = InetAddress.getLocalHost().getHostAddress();
                } catch (Exception ex) {
                    host = "127.0.0.1";
                }
            }
            if (configured == null || configured.isBlank()) {
                log.info("未配置 advertise-host，已推断集群公布地址：{}（多节点部署建议显式配置）", host);
            }
        }
        this.advertisedAddress = host + ":" + properties.getCluster().getBindPort();
    }
}
