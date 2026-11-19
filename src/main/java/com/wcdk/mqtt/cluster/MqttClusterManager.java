package com.wcdk.mqtt.cluster;

import java.io.File;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.core.MqttBrokerMessage;
import com.wcdk.mqtt.core.MqttRetainedStore;
import com.wcdk.mqtt.core.MqttSession;
import com.wcdk.mqtt.core.MqttSessionListener;
import com.wcdk.mqtt.core.MqttSessionRegistry;
import com.wcdk.mqtt.core.MqttTopicMatcher;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
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

    /** 新节点全量同步时每批下发的会话/保留消息数量，降低帧数与缓冲压力 */
    private static final int STATE_SYNC_BATCH_SIZE = 100;

    /** 集群编解码线程池默认线程数，配置为 0 时按 CPU 自动计算并限制在 2–4 */
    private static final int DEFAULT_CODEC_THREADS = Math.max(2,
            Math.min(4, Runtime.getRuntime().availableProcessors()));

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
    /** 入站事件解析/路由线程池，避免 JSON/Base64 解码占用集群事件循环 */
    private volatile ExecutorService inboundExecutor;
    /** 出站负载构造/JSON 编码线程池，避免编码占用 MQTT 与集群事件循环 */
    private volatile ExecutorService outboundExecutor;
    private volatile boolean running;
    /** 上一次记录的多数派状态，用于只在状态翻转时输出日志 */
    private volatile boolean quorumAvailable = true;
    /** 对等节点暂不可达时按节点暂存的发布消息，重连后补发，避免跨节点消息静默丢失 */
    private final Map<String, Deque<MqttBrokerMessage>> pendingPeerPublishes = new ConcurrentHashMap<>();
    /** 因缓冲容量不足丢弃的跨节点发布消息数 */
    private final AtomicLong droppedClusterPublishes = new AtomicLong();
    /** 集群服务端/客户端 TLS 上下文，未启用 TLS 时为空 */
    private volatile SslContext clusterServerSslContext;
    private volatile SslContext clusterClientSslContext;

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
        if (properties.getCluster().isRequireAuth() && clusterUsername() == null) {
            throw new IllegalStateException("集群已启用但未配置认证凭据：请设置 wcdk.mqtt.cluster.username/password，"
                    + "或在确认网络已隔离后显式设置 wcdk.mqtt.cluster.require-auth=false");
        }
        initTlsContexts();
        initClusterExecutors();
        clusterServer.bind(clusterServerSslContext);
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
        int expected = expectedNodeCount();
        if (properties.getCluster().isQuorumEnabled() && expected % 2 == 0) {
            log.warn("集群固定成员数为偶数（{}），多数派阈值为 {}，任一节点离线即失去写能力，建议使用奇数成员",
                    expected, quorumSize());
        }
        quorumAvailable = hasQuorum();
        log.info("集群功能启动成功，节点ID：{}，集群端口：{}，公布地址：{}，对等节点数：{}，固定成员数：{}，多数派阈值：{}，当前可达：{}",
                nodeId, clusterServer.port(), advertisedAddress, properties.getCluster().getPeers().size(),
                expected, quorumSize(), reachableNodeCount());
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
        pendingPeerPublishes.clear();
        nodeStates.values().forEach(state -> state.setConnected(false));
        clusterServer.dispose();
        clusterServerSslContext = null;
        clusterClientSslContext = null;
        shutdownExecutor(inboundExecutor);
        inboundExecutor = null;
        shutdownExecutor(outboundExecutor);
        outboundExecutor = null;
        log.info("集群功能已停止");
    }

    /***
    * 初始化集群通道 TLS 上下文：配置信任 CA 时服务端强制校验客户端证书，实现双向认证。
    * 未启用 TLS 时两个上下文均为空，连接保持明文。
    * @author wcdk
    ***/
    private void initTlsContexts() {
        if (!properties.getCluster().isTlsEnabled()) {
            return;
        }
        File certFile = requireSslFile(properties.getCluster().getTlsCertPath(), "tls-cert-path");
        File keyFile = requireSslFile(properties.getCluster().getTlsKeyPath(), "tls-key-path");
        String trustPath = properties.getCluster().getTlsTrustCertPath();
        File trustFile = trustPath == null || trustPath.isBlank() ? null : requireSslFile(trustPath, "tls-trust-cert-path");
        try {
            SslContextBuilder serverBuilder = SslContextBuilder.forServer(certFile, keyFile);
            if (trustFile != null) {
                // 配置互信 CA 时要求客户端证书，防止任意节点接入集群端口
                serverBuilder.trustManager(trustFile).clientAuth(ClientAuth.REQUIRE);
            }
            clusterServerSslContext = serverBuilder.build();
            SslContextBuilder clientBuilder = SslContextBuilder.forClient()
                    .keyManager(certFile, keyFile);
            if (trustFile != null) {
                clientBuilder.trustManager(trustFile);
            }
            clusterClientSslContext = clientBuilder.build();
            log.info("集群通道 TLS 已启用，双向认证：{}", trustFile != null);
        } catch (Exception ex) {
            throw new IllegalStateException("集群通道 TLS 上下文初始化失败：" + ex.getMessage(), ex);
        }
    }

    /***
    * 校验 TLS 证书文件存在，缺失时快速失败避免以明文误启动。
    * @author wcdk
    * @param path 文件路径
    * @param name 配置项名称
    * @return 证书文件
    ***/
    private File requireSslFile(String path, String name) {
        if (path == null || path.isBlank()) {
            throw new IllegalStateException("集群通道已启用 TLS，但未配置 " + name);
        }
        File file = new File(path.trim());
        if (!file.isFile()) {
            throw new IllegalStateException("集群通道 TLS 文件不存在，配置项：" + name + "，路径：" + file.getAbsolutePath());
        }
        return file;
    }

    /***
    * 初始化集群编解码线程池：入站与出站分离，避免大批量编码阻塞入站事件处理。
    * @author wcdk
    ***/
    private void initClusterExecutors() {
        int configured = properties.getCluster().getCodecThreads();
        int workers = configured > 0 ? configured : DEFAULT_CODEC_THREADS;
        inboundExecutor = Executors.newFixedThreadPool(workers, daemonThreadFactory("mqtt-cluster-inbound"));
        outboundExecutor = Executors.newFixedThreadPool(workers, daemonThreadFactory("mqtt-cluster-outbound"));
        log.info("集群编解码线程池已启动，每池线程数：{}", workers);
    }

    private ThreadFactory daemonThreadFactory(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private void shutdownExecutor(ExecutorService executor) {
        if (executor != null) {
            executor.shutdownNow();
        }
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
    * 集群固定成员数（含本节点）。优先取配置的 expected-nodes，
    * 未配置时按 peers 非空数量 + 1 计算，避免依赖动态发现结果。
    * @author wcdk
    * @return 固定成员数
    ***/
    public int expectedNodeCount() {
        int configured = properties.getCluster().getExpectedNodes();
        if (configured > 0) {
            return configured;
        }
        int peers = 0;
        List<String> addresses = properties.getCluster().getPeers();
        if (addresses != null) {
            for (String address : addresses) {
                if (address != null && !address.isBlank()) {
                    peers++;
                }
            }
        }
        return peers + 1;
    }

    /***
    * 多数派阈值：固定成员数的过半加一。
    * @author wcdk
    * @return 多数派所需节点数
    ***/
    public int quorumSize() {
        return expectedNodeCount() / 2 + 1;
    }

    /***
    * 当前可达节点数（含本节点）。
    * @author wcdk
    * @return 可达节点数
    ***/
    public int reachableNodeCount() {
        return connectedPeerCount() + 1;
    }

    /***
    * 是否达成多数派。单实例或关闭集群、关闭多数派校验时恒为 true。
    * @author wcdk
    * @return 达成多数派返回 true
    ***/
    public boolean hasQuorum() {
        if (!properties.getCluster().isEnabled() || !properties.getCluster().isQuorumEnabled()) {
            return true;
        }
        return reachableNodeCount() >= quorumSize();
    }

    /***
    * 重新计算多数派状态，仅在翻转时输出中文日志。
    * @author wcdk
    ***/
    private void refreshQuorumState() {
        if (!properties.getCluster().isEnabled() || !properties.getCluster().isQuorumEnabled()) {
            quorumAvailable = true;
            return;
        }
        boolean current = hasQuorum();
        if (current == quorumAvailable) {
            return;
        }
        quorumAvailable = current;
        if (current) {
            log.info("集群已恢复多数派，写能力恢复，可达节点：{}，多数派阈值：{}，固定成员数：{}",
                    reachableNodeCount(), quorumSize(), expectedNodeCount());
        } else {
            log.warn("集群未达成多数派，本节点进入只读模式，拒绝新连接与持久写入，可达节点：{}，多数派阈值：{}，固定成员数：{}",
                    reachableNodeCount(), quorumSize(), expectedNodeCount());
        }
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
        Executor codecExecutor = outboundExecutor != null ? outboundExecutor : Runnable::run;
        MqttClusterPeer peer = new MqttClusterPeer(connection, this, objectMapper, nodeId, dialed, targetAddress,
                codecExecutor, properties.getCluster().getEventQueueCapacity());
        // 每个连接独立的有序队列：同一节点的入站事件保持顺序，JSON/Base64 解码在工作线程执行
        SerialTaskQueue inboundQueue = new SerialTaskQueue(
                inboundExecutor != null ? inboundExecutor : Runnable::run,
                properties.getCluster().getEventQueueCapacity(),
                "peer-in-" + System.identityHashCode(peer));

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
                .doOnNext(line -> {
                    if (line == null || line.isBlank()) {
                        return;
                    }
                    // 在事件循环上只刷新活跃时间，解析与路由交给有序工作队列
                    peer.touch();
                    if (!inboundQueue.submit(() -> handleLine(peer, line))) {
                        log.warn("集群入站处理队列已满，断开对等连接，远端节点：{}，队列任务数：{}",
                                peer.remoteNodeId(), properties.getCluster().getEventQueueCapacity());
                        peer.close();
                    }
                })
                .doFinally(signalType -> {
                    inboundQueue.close();
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
            case SESSION_BATCH -> handleSessionBatch(payload);
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
        // 新会话建立后同步本节点全量状态；快照构造与 Base64 编码放到工作线程，避免阻塞入站队列
        if (firstRegistration) {
            submitStateSync(peer);
        }
    }

    /***
    * 异步执行全量状态同步，避免 3 万级会话快照构造占用事件处理线程。
    * @author wcdk
    * @param peer 对等连接
    ***/
    private void submitStateSync(MqttClusterPeer peer) {
        ExecutorService executor = outboundExecutor;
        if (executor == null) {
            sendStateSync(peer);
            return;
        }
        try {
            executor.execute(() -> {
                if (!peer.isClosed()) {
                    sendStateSync(peer);
                }
            });
        } catch (RejectedExecutionException ex) {
            log.warn("集群状态同步任务提交失败，远端节点：{}，异常信息：{}", peer.remoteNodeId(), ex.getMessage());
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
        boolean keepSelfDialed = nodeId.compareTo(remoteNodeId) < 0;
        // 双向互拨时两个线程可能同时登记，用 compute 原子保证每个节点只保留一条连接
        MqttClusterPeer[] replacedHolder = new MqttClusterPeer[1];
        boolean[] acceptedHolder = new boolean[1];
        peers.compute(remoteNodeId, (id, existing) -> {
            if (existing != null && existing != peer && !existing.isClosed()) {
                if (peer.isDialed() != keepSelfDialed) {
                    // 按节点 ID 字典序保留约定方向的连接，丢弃当前连接
                    acceptedHolder[0] = false;
                    return existing;
                }
                replacedHolder[0] = existing;
            }
            acceptedHolder[0] = true;
            return peer;
        });
        if (!acceptedHolder[0]) {
            log.info("发现重复的集群连接，丢弃后建立的一条，节点ID：{}，本方主动拨号：{}",
                    remoteNodeId, peer.isDialed());
            peer.close();
            return false;
        }
        MqttClusterPeer replacedPeer = replacedHolder[0];
        if (replacedPeer != null) {
            log.info("发现重复的集群连接，替换既有连接，节点ID：{}", remoteNodeId);
            replacedPeer.close();
        }
        boolean replaced = replacedPeer != null;
        // 节点状态同样原子维护，避免并发登记互相覆盖
        nodeStates.compute(remoteNodeId, (id, existingState) -> {
            ClusterNodeState current = existingState == null ? new ClusterNodeState() : existingState;
            current.setNodeId(id);
            current.setAddress(peer.advertisedAddress());
            current.setConnected(true);
            current.setDialed(peer.isDialed());
            current.setLastSeenAt(Instant.now());
            if (current.getConnectedAt() == null || replaced) {
                current.setConnectedAt(Instant.now());
            }
            return current;
        });
        log.info("集群节点已连接，节点ID：{}，地址：{}，本方主动拨号：{}",
                remoteNodeId, peer.advertisedAddress(), peer.isDialed());
        refreshQuorumState();
        // 补发该节点断连期间积压的发布消息
        flushPendingPublishes(peer);
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
        refreshQuorumState();
    }

    /***
    * 向新连接同步保留消息与本地会话快照。
    * @author wcdk
    * @param peer 对等连接
    ***/
    private void sendStateSync(MqttClusterPeer peer) {
        List<MqttRetainedStore.RetainedEntry> retained = registry.retainedStore().entries();
        int retainedBatches = 0;
        for (int from = 0; from < retained.size(); from += STATE_SYNC_BATCH_SIZE) {
            int to = Math.min(from + STATE_SYNC_BATCH_SIZE, retained.size());
            List<Map<String, Object>> items = new ArrayList<>(to - from);
            for (MqttRetainedStore.RetainedEntry entry : retained.subList(from, to)) {
                items.add(messagePayload(entry.message(), entry.version()));
            }
            if (!peer.send(ClusterEventType.RETAINED_SYNC, Map.of("messages", items))) {
                log.debug("保留消息同步中止，远端节点：{}", peer.remoteNodeId());
                return;
            }
            retainedBatches++;
        }
        // 会话快照按批下发，避免 3 万级会话产生同等数量的帧并撑爆对端缓冲
        List<MqttSession.SessionSnapshot> batch = new ArrayList<>(STATE_SYNC_BATCH_SIZE);
        int sessionCount = 0;
        for (MqttSession session : registry.sessions()) {
            batch.add(session.snapshot());
            sessionCount++;
            if (batch.size() >= STATE_SYNC_BATCH_SIZE) {
                if (!peer.send(ClusterEventType.SESSION_BATCH, Map.of("snapshots", batch))) {
                    log.debug("会话快照同步中止，远端节点：{}", peer.remoteNodeId());
                    return;
                }
                batch = new ArrayList<>(STATE_SYNC_BATCH_SIZE);
            }
        }
        if (!batch.isEmpty()) {
            peer.send(ClusterEventType.SESSION_BATCH, Map.of("snapshots", batch));
        }
        log.debug("已向节点 {} 同步状态，保留消息：{}（{}批），本地会话：{}",
                peer.remoteNodeId(), retained.size(), retainedBatches, sessionCount);
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
        Long retainedVersion = parseVersion(payload.get("retainedVersion"));
        registry.deliver(message, retainedVersion);
    }

    /***
    * 解析事件负载中的版本号，缺失或非法时返回 null。
    * @author wcdk
    * @param value 负载值
    * @return 版本号或 null
    ***/
    private static Long parseVersion(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String text) {
            try {
                return Long.parseLong(text.trim());
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        return null;
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
        // C3：合并归属声明后，先释放本地副本再登记远端快照，避免同名会话双在线
        if (Boolean.TRUE.equals(payload.get("claim"))) {
            claimLocalSession(snapshot.getClientId());
        }
        registry.putRemoteSnapshot(snapshot);
        if (snapshot.isOnline()) {
            // 会话已在属主节点上线，本节点的离线缓冲交由属主节点负责
            registry.clearPendingOffline(snapshot.getClientId());
        }
    }

    /***
    * 处理批量会话快照同步（新节点加入时的全量状态下发）。
    * @author wcdk
    * @param payload 事件负载
    ***/
    private void handleSessionBatch(Map<String, Object> payload) {
        Object value = payload.get("snapshots");
        if (!(value instanceof List<?> items)) {
            return;
        }
        for (Object item : items) {
            try {
                MqttSession.SessionSnapshot snapshot =
                        objectMapper.convertValue(item, MqttSession.SessionSnapshot.class);
                if (snapshot == null || snapshot.getClientId() == null) {
                    continue;
                }
                registry.putRemoteSnapshot(snapshot);
                if (snapshot.isOnline()) {
                    registry.clearPendingOffline(snapshot.getClientId());
                }
            } catch (RuntimeException ex) {
                log.warn("批量会话快照解析失败，异常信息：{}", ex.getMessage());
            }
        }
    }

    /***
    * 释放本地同名会话副本，交由其他节点接管。
    * @author wcdk
    * @param clientId 客户端 ID
    ***/
    private void claimLocalSession(String clientId) {
        MqttSession local = registry.findByClientId(clientId);
        if (local == null) {
            return;
        }
        if (local.isActive()) {
            log.info("客户端已接入其他节点，本节点断开本地连接，客户端ID：{}", clientId);
            local.closeConnection();
        }
        registry.releaseForClaim(clientId);
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
        claimLocalSession(clientId);
        log.debug("已释放本地会话副本，交由属主节点维护，客户端ID：{}", clientId);
    }

    /***
    * 处理会话清理通知，删除远端快照与离线缓冲。
    * @author wcdk
    * @param payload 事件负载
    ***/
    private void handleSessionRemove(Map<String, Object> payload) {
        Object clientIdValue = payload.get("clientId");
        if (clientIdValue == null) {
            return;
        }
        Long version = parseVersion(payload.get("version"));
        Object ownerNodeId = payload.get("ownerNodeId");
        registry.removeRemoteSnapshot(clientIdValue.toString(),
                version == null ? 0L : version,
                ownerNodeId == null ? null : ownerNodeId.toString());
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
        List<MqttRetainedStore.RetainedEntry> merged = new ArrayList<>();
        for (Object item : items) {
            Map<String, Object> map = objectMapper.convertValue(item,
                    new TypeReference<LinkedHashMap<String, Object>>() {
                    });
            MqttBrokerMessage message = decodeMessage(map);
            if (message == null) {
                continue;
            }
            Long version = parseVersion(map.get("retainedVersion"));
            merged.add(new MqttRetainedStore.RetainedEntry(message, version == null ? 0L : version));
        }
        registry.retainedStore().merge(merged);
    }

    /***
    * 按远端订阅索引定向转发消息，仅发送给存在匹配订阅者（含离线持久会话）的节点，
    * 避免无订阅者时仍向全部对等节点广播造成的带宽放大。
    * @author wcdk
    * @param message 消息
    ***/
    public void broadcastPublish(MqttBrokerMessage message) {
        if (!running || message == null) {
            return;
        }
        Set<String> targetNodes = registry.remoteTargetNodes(message.topic());
        if (targetNodes.isEmpty()) {
            return;
        }
        // 负载构造（Base64）延迟到编码线程，多个对等节点共享同一份结果
        Supplier<Map<String, Object>> payload = new LazyPayloadSupplier(() -> messagePayload(message));
        for (String targetNodeId : targetNodes) {
            MqttClusterPeer peer = peers.get(targetNodeId);
            if (peer == null || peer.isClosed() || !peer.send(ClusterEventType.PUBLISH, payload)) {
                // 节点断开或出站队列已满：暂存到该节点缓冲，重连后补发，避免消息静默丢失
                bufferPendingPublish(targetNodeId, message);
            }
        }
    }

    /***
    * 暂存节点不可达期间的跨节点发布，缓冲区满时丢弃最旧消息并累计告警。
    * @author wcdk
    * @param nodeId 目标节点 ID
    * @param message 发布消息
    ***/
    void bufferPendingPublish(String nodeId, MqttBrokerMessage message) {
        if (nodeId == null || message == null) {
            return;
        }
        int capacity = Math.max(16, properties.getCluster().getEventQueueCapacity());
        Deque<MqttBrokerMessage> queue = pendingPeerPublishes.computeIfAbsent(nodeId, key -> new ArrayDeque<>());
        synchronized (queue) {
            while (queue.size() >= capacity) {
                queue.pollFirst();
                long dropped = droppedClusterPublishes.incrementAndGet();
                if (dropped == 1 || dropped % 1000 == 0) {
                    log.warn("跨节点发布缓冲已满，丢弃最旧消息，目标节点：{}，累计丢弃：{}，缓冲上限：{}",
                            nodeId, dropped, capacity);
                }
            }
            queue.addLast(message);
        }
    }

    /***
    * 节点重连后补发断连期间积压的跨节点发布；补发失败时保留剩余消息等待下次重连。
    * @author wcdk
    * @param peer 已登记的对等连接
    ***/
    private void flushPendingPublishes(MqttClusterPeer peer) {
        String nodeId = peer.remoteNodeId();
        if (nodeId == null) {
            return;
        }
        Deque<MqttBrokerMessage> queue = pendingPeerPublishes.get(nodeId);
        if (queue == null) {
            return;
        }
        int sent = 0;
        synchronized (queue) {
            while (!peer.isClosed()) {
                MqttBrokerMessage message = queue.peekFirst();
                if (message == null) {
                    break;
                }
                if (!peer.send(ClusterEventType.PUBLISH,
                        new LazyPayloadSupplier(() -> messagePayload(message)))) {
                    break;
                }
                queue.pollFirst();
                sent++;
            }
            if (queue.isEmpty()) {
                pendingPeerPublishes.remove(nodeId, queue);
            }
        }
        if (sent > 0) {
            log.info("已补发节点断连期间积压的跨节点消息，节点ID：{}，数量：{}", nodeId, sent);
        }
    }

    /***
    * 当前各节点待补发的跨节点消息总数，供控制台观测。
    * @author wcdk
    * @return 待补发消息数
    ***/
    public int pendingPeerPublishCount() {
        int total = 0;
        for (Deque<MqttBrokerMessage> queue : pendingPeerPublishes.values()) {
            synchronized (queue) {
                total += queue.size();
            }
        }
        return total;
    }

    /***
    * 累计因缓冲满丢弃的跨节点消息数，供控制台观测。
    * @author wcdk
    * @return 丢弃消息数
    ***/
    public long droppedClusterPublishCount() {
        return droppedClusterPublishes.get();
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
        Supplier<Map<String, Object>> payload = new LazyPayloadSupplier(() -> {
            Map<String, Object> value = new LinkedHashMap<>(messagePayload(message));
            value.put("targetClientId", clientId);
            return value;
        });
        if (ownerNodeId != null) {
            MqttClusterPeer owner = peers.get(ownerNodeId);
            if (owner != null && !owner.isClosed()) {
                if (owner.send(ClusterEventType.PUBLISH, payload)) {
                    return true;
                }
                log.warn("跨节点点对点转发失败，目标客户端：{}，属主节点：{}", clientId, ownerNodeId);
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

    /***
    * 会话状态变更通知：上线时把归属声明（claim）、离线缓冲清理与状态快照合并为单条
    * SESSION_UPSERT 事件，接收方在登记快照前先释放同名本地副本。
    * 相比原先 CLAIM + OFFLINE_CLEAR + UPSERT 三条事件，连接风暴下的集群事件量降低约 2/3。
    * @author wcdk
    * @param session 发生变更的会话
    ***/
    @Override
    public void onSessionChanged(MqttSession session) {
        if (!running) {
            return;
        }
        boolean active = session.isActive();
        // 会话快照构造在编码线程执行，避免订阅列表/离线队列序列化阻塞 MQTT 事件循环
        Supplier<Map<String, Object>> payload = new LazyPayloadSupplier(() -> {
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("snapshot", session.snapshot());
            if (active) {
                // claim=true 让对端先释放同名本地会话再登记远端快照；
                // 上线快照本身已携带 isOnline，对端据此清理离线缓冲，无需单独的 OFFLINE_CLEAR
                value.put("claim", Boolean.TRUE);
            }
            return value;
        });
        for (MqttClusterPeer peer : peers.values()) {
            peer.send(ClusterEventType.SESSION_UPSERT, payload);
        }
    }

    @Override
    public void onSessionRemoved(MqttSession session) {
        if (!running) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("clientId", session.clientId());
        // 携带删除版本，接收方据此丢弃迟到的旧快照
        payload.put("version", session.stateVersion());
        if (session.ownerNodeId() != null) {
            payload.put("ownerNodeId", session.ownerNodeId());
        }
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
            TcpClient client = TcpClient.create()
                    .host(host)
                    .port(port)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000);
            SslContext clientSsl = clusterClientSslContext;
            if (clientSsl != null) {
                client = client.secure(spec -> spec.sslContext(clientSsl));
            }
            client.handle((inbound, outbound) -> wire(inbound, outbound, true, address))
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
        // 保留消息携带本地已分配的版本，接收方按版本合并而不是按到达顺序覆盖
        long retainedVersion = message.retained() ? registry.retainedStore().versionOf(message.topic()) : 0L;
        return messagePayload(message, retainedVersion);
    }

    /***
    * 构造消息事件负载，保留消息附带对账版本。
    * @author wcdk
    * @param message 消息
    * @param retainedVersion 保留消息版本，非正数表示不携带
    * @return 负载 Map
    ***/
    private Map<String, Object> messagePayload(MqttBrokerMessage message, long retainedVersion) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("messageId", message.messageId());
        payload.put("topic", message.topic());
        payload.put("payload", Base64.getEncoder().encodeToString(message.payload()));
        payload.put("qos", message.qos());
        payload.put("retained", message.retained());
        if (retainedVersion > 0) {
            payload.put("retainedVersion", retainedVersion);
        }
        return payload;
    }

    /***
    * 延迟构造并缓存事件负载：多个对等连接共享同一份 Base64 结果，
    * 实际构造发生在编码线程，不占用调用方的事件循环。
    * @author wcdk
    ***/
    private static final class LazyPayloadSupplier implements Supplier<Map<String, Object>> {

        private final Supplier<Map<String, Object>> delegate;
        private volatile Map<String, Object> value;

        private LazyPayloadSupplier(Supplier<Map<String, Object>> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Map<String, Object> get() {
            Map<String, Object> current = value;
            if (current == null) {
                synchronized (this) {
                    current = value;
                    if (current == null) {
                        current = delegate.get();
                        value = current;
                    }
                }
            }
            return current;
        }
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
