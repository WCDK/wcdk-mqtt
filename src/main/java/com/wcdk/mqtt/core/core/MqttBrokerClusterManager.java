package com.wcdk.mqtt.core.core;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.wcdk.mqtt.bean.ClientSession;
import com.wcdk.mqtt.core.cluster.MqttClusterConfigSyncService;
import com.wcdk.mqtt.core.cluster.MqttClusterTcpChannel;
import com.wcdk.mqtt.core.config.MqttBrokerProperties;
import com.wcdk.mqtt.core.json.JSON;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.StringUtils;

/**
 * @auther WCDK
 * @date 2026/7/29
 * @version 1.0
 **/
public class MqttBrokerClusterManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MqttBrokerClusterManager.class);

    /** 集群事件类型：发布消息 */
    private static final String TYPE_PUBLISH = "PUBLISH";
    /** 集群事件类型：直接发送给客户端 */
    private static final String TYPE_DIRECT_CLIENT = "DIRECT_CLIENT";
    /** 集群事件类型：直接发送给会话 */
    private static final String TYPE_DIRECT_SESSION = "DIRECT_SESSION";
    /** 集群事件类型：踢出客户端 */
    private static final String TYPE_KICK_CLIENT = "KICK_CLIENT";
    /** 集群事件类型：投递确认 */
    private static final String TYPE_DELIVERY_ACK = "DELIVERY_ACK";
    /** 集群事件类型：客户端确认 */
    private static final String TYPE_CLIENT_ACK = "CLIENT_ACK";
    /** 集群事件类型：节点心跳 */
    private static final String TYPE_NODE_HEARTBEAT = "NODE_HEARTBEAT";
    /** 集群事件类型：节点离开 */
    private static final String TYPE_NODE_LEFT = "NODE_LEFT";
    /** 集群事件类型：声明客户端所有权 */
    private static final String TYPE_OWNER_CLAIM = "OWNER_CLAIM";
    /** 集群事件类型：释放客户端所有权 */
    private static final String TYPE_OWNER_RELEASE = "OWNER_RELEASE";
    /** 集群事件类型：会话状态更新 */
    private static final String TYPE_SESSION_UPSERT = "SESSION_UPSERT";
    /** 集群事件类型：保留消息更新 */
    private static final String TYPE_RETAINED_UPSERT = "RETAINED_UPSERT";
    /** 集群事件类型：保留消息删除 */
    private static final String TYPE_RETAINED_DELETE = "RETAINED_DELETE";
    /** 集群事件类型：添加离线消息 */
    private static final String TYPE_OFFLINE_APPEND = "OFFLINE_APPEND";
    /** 集群事件类型：清空离线消息 */
    private static final String TYPE_OFFLINE_CLEAR = "OFFLINE_CLEAR";
    /** 集群事件类型：节点注册 */
    private static final String TYPE_NODE_REGISTER = "NODE_REGISTER";
    /** 集群事件类型：节点注销 */
    private static final String TYPE_NODE_UNREGISTER = "NODE_UNREGISTER";

    /** Broker配置属性 */
    private final MqttBrokerProperties properties;
    /** 会话注册表，管理所有活跃会话 */
    private final MqttBrokerSessionRegistry sessionRegistry;
    /** 配置同步服务提供者 */
    private final org.springframework.beans.factory.ObjectProvider<MqttClusterConfigSyncService> configSyncServiceProvider;
    /** 当前节点ID，用于集群中唯一标识本节点 */
    private final String nodeId;
    /** 集群管理器运行状态标志 */
    private final AtomicBoolean running = new AtomicBoolean(false);
    /** 集群节点状态表，存储所有节点的状态信息 */
    private final ConcurrentMap<String, NodeState> nodes = new ConcurrentHashMap<>();
    /** 客户端所有权映射表，记录每个客户端所属的节点 */
    private final ConcurrentMap<String, String> clientOwners = new ConcurrentHashMap<>();
    /** 会话快照表，存储所有客户端会话的状态快照 */
    private final ConcurrentMap<String, ClientSession> sessionSnapshots = new ConcurrentHashMap<>();
    /** 保留消息表，存储集群中所有保留的消息 */
    private final ConcurrentMap<String, RetainedMessage> retainedMessages = new ConcurrentHashMap<>();
    /** 离线消息表，为其他节点的离线客户端存储消息 */
    private final ConcurrentMap<String, CopyOnWriteArrayList<OfflineMessage>> offlineMessages = new ConcurrentHashMap<>();
    /** 投递确认表，等待其他节点确认消息投递 */
    private final ConcurrentMap<String, String> deliveryAcks = new ConcurrentHashMap<>();
    /** 客户端确认表，等待客户端确认消息接收 */
    private final ConcurrentMap<String, String> clientAcks = new ConcurrentHashMap<>();
    /** 当前节点角色（主节点/从节点） */
    private final MqttBrokerProperties.NodeRole role;
    /** 主节点ID，用于从节点连接主节点 */
    private final String masterNodeId;

    private volatile MqttClusterTcpChannel clusterChannel;
    private volatile ScheduledExecutorService heartbeatExecutor;

    public MqttBrokerClusterManager(MqttBrokerProperties properties,
                                    MqttBrokerSessionRegistry sessionRegistry,
                                    org.springframework.beans.factory.ObjectProvider<MqttClusterConfigSyncService> configSyncServiceProvider) {
        this.properties = properties;
        this.sessionRegistry = sessionRegistry;
        this.configSyncServiceProvider = configSyncServiceProvider;
        this.nodeId = resolveNodeId(properties);
        this.role = properties.getCluster() != null ? properties.getCluster().getRole() : MqttBrokerProperties.NodeRole.SLAVE;
        this.masterNodeId = resolveMasterNodeId(properties);
    }

    public String nodeId() {
        return nodeId;
    }

    public boolean isClusterEnabled() {
        return clusterProperties().isEnabled();
    }

    public boolean isMaster() {
        return role == MqttBrokerProperties.NodeRole.MASTER;
    }

    public boolean isSlave() {
        return role == MqttBrokerProperties.NodeRole.SLAVE;
    }

    public String getMasterNodeId() {
        return masterNodeId;
    }

    public MqttBrokerProperties.NodeRole getRole() {
        return role;
    }

    public Map<String, NodeState> getNodes() {
        return nodes;
    }

    public Map<String, String> getClientOwners() {
        return clientOwners;
    }

    public Map<String, ClientSession> getSessionSnapshots() {
        return sessionSnapshots;
    }

    public Map<String, RetainedMessage> getRetainedMessages() {
        return retainedMessages;
    }

    public Map<String, CopyOnWriteArrayList<OfflineMessage>> getOfflineMessages() {
        return offlineMessages;
    }
    /**
     * 选择负载均衡目标节点
     * 主节点根据各节点会话数量选择负载最低的节点进行连接代理
     *
     * @return 最优目标节点状态，如果无法选择则返回空
     */
    public Optional<NodeState> selectLoadBalanceTarget() {
        MqttBrokerProperties.LoadBalance loadBalance = clusterProperties().getLoadBalance();
        // 仅主节点且启用负载均衡时才进行选择
        if (!isClusterEnabled() || !isMaster() || loadBalance == null || !loadBalance.isEnabled()) {
            return Optional.empty();
        }
        boolean includeMaster = loadBalance.isIncludeMaster();
        return nodes.values().stream()
                .filter(state -> state != null && StringUtils.hasText(state.getNodeId()))
                // 根据配置决定是否包含主节点
                .filter(state -> includeMaster || !nodeId.equals(state.getNodeId()))
                // 过滤掉不存活的节点
                .filter(state -> isNodeAlive(state.getNodeId()))
                // 确保节点有有效的MQTT服务地址
                .filter(state -> StringUtils.hasText(state.getMqttHost()) && state.getMqttPort() > 0)
                // 选择会话数最少的节点，如果会话数相同则按节点ID排序
                .min(Comparator.comparingInt(NodeState::getSessionCount)
                        .thenComparing(NodeState::getNodeId));
    }

    public boolean isLocalNode(String targetNodeId) {
        return StringUtils.hasText(targetNodeId) && nodeId.equals(targetNodeId.trim());
    }

    public boolean isNodeAlive(String targetNodeId) {
        if (nodeId.equals(targetNodeId)) {
            return true;
        }
        NodeState state = nodes.get(targetNodeId);
        return state != null && System.currentTimeMillis() - state.getUpdatedAt() <= nodeTimeoutMillis();
    }

    /**
     * 声明客户端会话所有权
     * 当客户端连接时，向集群广播声明该客户端的所有者节点
     * 如果该客户端已在其他节点连接，则踢出旧连接
     *
     * @param clientId 客户端ID
     * @return 声明成功返回true
     */
    public boolean claimClientSession(String clientId) {
        // 未启用集群或全局会话功能时直接返回
        if (!isClusterEnabled() || !clusterProperties().isGlobalSession() || !StringUtils.hasText(clientId)) {
            return true;
        }
        String resolvedClientId = clientId.trim();
        // 检查是否有其他节点拥有该客户端
        String previousOwner = clientOwners.get(resolvedClientId);
        if (StringUtils.hasText(previousOwner) && !nodeId.equals(previousOwner) && isNodeAlive(previousOwner)) {
            // 踢出旧节点上的客户端连接
            publishClusterEvent(kickClientEvent(resolvedClientId, previousOwner), false, false);
        }
        // 声明当前节点拥有该客户端
        clientOwners.put(resolvedClientId, nodeId);
        // 广播所有权声明事件
        ClusterEvent ownerEvent = new ClusterEvent();
        ownerEvent.setType(TYPE_OWNER_CLAIM);
        ownerEvent.setNodeId(nodeId);
        ownerEvent.setTargetClientId(resolvedClientId);
        ownerEvent.setCreatedAt(System.currentTimeMillis());
        publishClusterEvent(ownerEvent, false, false);
        return true;
    }

    public void releaseClientSession(MqttBrokerSession session) {
        if (!isClusterEnabled() || session == null || !StringUtils.hasText(session.clientId())) {
            return;
        }
        if (!session.cleanSession()) {
            return;
        }
        offlineMessages.remove(session.clientId());
        sessionSnapshots.remove(session.clientId());
        if (!clusterProperties().isGlobalSession()) {
            return;
        }
        clientOwners.remove(session.clientId(), nodeId);
        ClusterEvent event = new ClusterEvent();
        event.setType(TYPE_OWNER_RELEASE);
        event.setNodeId(nodeId);
        event.setTargetClientId(session.clientId());
        event.setCreatedAt(System.currentTimeMillis());
        publishClusterEvent(event, false, false);
    }

    /**
     * 更新会话状态快照
     * 将本地会话状态同步到集群中，供其他节点读取
     *
     * @param session 需要同步的会话对象
     */
    public void updateSessionSnapshot(MqttBrokerSession session) {
        // 未启用集群或会话无效时直接返回
        if (!isClusterEnabled() || session == null || !StringUtils.hasText(session.clientId())) {
            return;
        }
        // 将会话转换为客户端会话快照
        ClientSession clientSession = toClientSession(session);
        // 更新本地会话快照表
        sessionSnapshots.put(session.clientId(), clientSession);
        // 广播会话更新事件到集群
        ClusterEvent event = new ClusterEvent();
        event.setType(TYPE_SESSION_UPSERT);
        event.setNodeId(nodeId);
        event.setTargetClientId(session.clientId());
        event.setSessionJson(JSON.toJSONString(clientSession));
        event.setCreatedAt(System.currentTimeMillis());
        publishClusterEvent(event, false, false);
    }

    public void restoreSessionState(MqttBrokerSession session) {
        if (!isClusterEnabled() || session == null || session.cleanSession()) {
            return;
        }
        ClientSession clientSession = sessionSnapshots.get(session.clientId());
        if (clientSession != null) {
            session.restoreQosState(clientSession.getQosStateSnapshot());
        }
    }

    public void replayOfflineMessages(MqttBrokerSession session) {
        if (!isClusterEnabled() || !clusterProperties().isOfflineQueue() || session == null || session.cleanSession()) {
            return;
        }
        List<OfflineMessage> payloads = new ArrayList<>(offlineMessages.getOrDefault(session.clientId(), new CopyOnWriteArrayList<>()));
        offlineMessages.remove(session.clientId());
        broadcastOfflineClear(session.clientId());
        for (OfflineMessage offlineMessage : payloads) {
            try {
                MqttBrokerMessage message = toBrokerMessage(offlineMessage);
                session.sendPublish(message, MqttQoS.valueOf(offlineMessage.getDeliveryQos()), offlineMessage.isRetained());
            } catch (RuntimeException ex) {
                log.warn("重放MQTT离线集群消息失败，clientId={}", session.clientId(), ex);
            }
        }
        sessionRegistry.notifySessionUpdated(session);
    }

    public void sendDistributedRetained(MqttBrokerSession session, String topicFilter, MqttQoS subscriptionQos) {
        if (!isClusterEnabled() || !clusterProperties().isDistributedRetained() || session == null || !properties.isRetainedMessages()) {
            return;
        }
        for (RetainedMessage retainedMessage : retainedMessages.values()) {
            if (retainedMessage == null || !MqttTopicFilter.matches(topicFilter, retainedMessage.getTopic())) {
                continue;
            }
            MqttBrokerMessage message = toBrokerMessage(retainedMessage);
            MqttQoS deliveryQos = lowerQos(message.getQos(), subscriptionQos.value());
            session.sendPublish(message, deliveryQos, true);
        }
        sessionRegistry.notifySessionUpdated(session);
    }

    public void publish(MqttBrokerMessage message) {
        storeRetained(message, true);
        sessionRegistry.publish(message);
        enqueueOfflineMessages(message);
        publishClusterEvent(toPublishEvent(message), false, false);
    }

    /**
     * 发布消息给指定客户端
     * 如果客户端在本地则直接发送，否则通过集群转发
     *
     * @param clientId 目标客户端ID
     * @param message 要发送的消息
     * @return 发送成功返回true
     */
    public boolean publishToClient(String clientId, MqttBrokerMessage message) {
        // 尝试在本地投递
        if (deliverToClient(clientId, message, null)) {
            return true;
        }
        // 本地没有该客户端，通过集群转发
        ClusterEvent event = toDirectClientEvent(clientId, message);
        // 根据配置决定是否等待确认：需要启用集群ACK且QoS不为AT_MOST_ONCE时才等待
        boolean waitBrokerAck = clusterProperties().isDeliveryAckEnabled();
        boolean waitClientAck = clusterProperties().isClientAckEnabled() && message.getQos() != MqttQoS.AT_MOST_ONCE;
        return publishClusterEvent(event, waitBrokerAck, waitClientAck);
    }

    /**
     * 发布消息给指定会话地址
     * 如果会话在本地则直接发送，否则通过集群转发
     *
     * @param sessionUrl 目标会话地址
     * @param message 要发送的消息
     * @return 发送成功返回true
     */
    public boolean publishToSessionUrl(String sessionUrl, MqttBrokerMessage message) {
        // 尝试在本地投递
        if (deliverToSessionUrl(sessionUrl, message, null)) {
            return true;
        }
        // 本地没有该会话，通过集群转发
        ClusterEvent event = toDirectSessionEvent(sessionUrl, message);
        // 根据配置决定是否等待确认：需要启用集群ACK且QoS不为AT_MOST_ONCE时才等待
        boolean waitBrokerAck = clusterProperties().isDeliveryAckEnabled();
        boolean waitClientAck = clusterProperties().isClientAckEnabled() && message.getQos() != MqttQoS.AT_MOST_ONCE;
        return publishClusterEvent(event, waitBrokerAck, waitClientAck);
    }

    public void notifyClientAck(MqttBrokerSession.ClusterAckContext context, String clientId, int packetId, String ackType) {
        if (!isClusterEnabled() || context == null || !StringUtils.hasText(context.requestId())) {
            return;
        }
        ClusterEvent ackEvent = new ClusterEvent();
        ackEvent.setType(TYPE_CLIENT_ACK);
        ackEvent.setNodeId(nodeId);
        ackEvent.setRequestId(context.requestId());
        ackEvent.setTargetNodeId(context.sourceNodeId());
        ackEvent.setTargetClientId(clientId);
        ackEvent.setPacketId(packetId);
        ackEvent.setAckType(ackType);
        ackEvent.setCreatedAt(System.currentTimeMillis());
        publishClusterEvent(ackEvent, false, false);
    }

    @Override
    public synchronized void start() {
        if (!isClusterEnabled() || running.get()) {
            return;
        }
        if (clusterProperties().getChannel() == MqttBrokerProperties.Channel.GRPC) {
            log.warn("MQTT Broker gRPC集群通道未单独配置，回退到TCP通道");
        }
        clusterChannel = new MqttClusterTcpChannel(clusterProperties(), this::handleClusterEvent, configSyncServiceProvider);
        clusterChannel.start();
        running.set(true);
        startHeartbeat();
        log.info("MQTT Broker集群监听器已启动，nodeId={}, channel={}", nodeId, clusterProperties().getChannel());
    }

    @Override
    public synchronized void stop() {
        handleNodeLeft(nodeId);
        ClusterEvent leftEvent = new ClusterEvent();
        leftEvent.setType(TYPE_NODE_LEFT);
        leftEvent.setNodeId(nodeId);
        leftEvent.setCreatedAt(System.currentTimeMillis());
        publishClusterEvent(leftEvent, false, false);
        ScheduledExecutorService executor = heartbeatExecutor;
        if (executor != null) {
            executor.shutdownNow();
            heartbeatExecutor = null;
        }
        MqttClusterTcpChannel currentChannel = clusterChannel;
        if (currentChannel != null) {
            currentChannel.stop();
            clusterChannel = null;
        }
        nodes.remove(nodeId);
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE + 50;
    }

    private boolean publishClusterEvent(ClusterEvent event, boolean waitBrokerAck, boolean waitClientAck) {
        if (!isClusterEnabled()) {
            return false;
        }
        if ((waitBrokerAck || waitClientAck) && !StringUtils.hasText(event.getRequestId())) {
            event.setRequestId(UUID.randomUUID().toString());
        }
        MqttClusterTcpChannel currentChannel = clusterChannel;
        if (currentChannel != null) {
            currentChannel.publish(JSON.toJSONString(event));
        }
        if (!waitBrokerAck) {
            return true;
        }
        if (!waitAck(deliveryAcks, event.getRequestId())) {
            return false;
        }
        return !waitClientAck || waitAck(clientAcks, event.getRequestId());
    }

    private boolean waitAck(Map<String, String> ackStore, String requestId) {
        if (!StringUtils.hasText(requestId)) {
            return false;
        }
        long timeoutAt = System.currentTimeMillis() + Math.max(1, clusterProperties().getDeliveryAckTimeoutMillis());
        while (System.currentTimeMillis() < timeoutAt) {
            if (StringUtils.hasText(ackStore.remove(requestId))) {
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    /**
     * 处理集群事件
     * 接收并处理来自其他节点的集群事件消息
     *
     * @param payload 事件消息内容（JSON格式）
     */
    private void handleClusterEvent(String payload) {
        if (!StringUtils.hasText(payload)) {
            return;
        }
        try {
            ClusterEvent event = JSON.parseObject(payload, ClusterEvent.class);
            // 忽略本节点发出的事件或目标不是本节点的事件
            if (event == null || nodeId.equals(event.getNodeId())) {
                return;
            }
            if (StringUtils.hasText(event.getTargetNodeId()) && !nodeId.equals(event.getTargetNodeId())) {
                return;
            }
            // 处理状态类事件（心跳、所有权、会话等）
            handleStateEvent(event);
            // 确认类事件或纯状态事件无需进一步处理
            if (TYPE_DELIVERY_ACK.equals(event.getType()) || TYPE_CLIENT_ACK.equals(event.getType()) || isStateOnlyEvent(event.getType())) {
                return;
            }
            // 处理踢出客户端事件
            if (TYPE_KICK_CLIENT.equals(event.getType())) {
                kickLocalClient(event.getTargetClientId());
                ack(event, true);
                return;
            }
            // 处理消息投递事件
            MqttBrokerMessage message = toBrokerMessage(event);
            if (TYPE_PUBLISH.equals(event.getType())) {
                // 发布消息：存储保留消息并广播给订阅者
                storeRetained(message, false);
                sessionRegistry.publish(message);
            } else if (TYPE_DIRECT_CLIENT.equals(event.getType())) {
                // 直接发送给指定客户端
                ack(event, deliverToClient(event.getTargetClientId(), message, event));
            } else if (TYPE_DIRECT_SESSION.equals(event.getType())) {
                // 直接发送给指定会话
                ack(event, deliverToSessionUrl(event.getTargetSessionUrl(), message, event));
            }
        } catch (RuntimeException ex) {
            log.warn("处理MQTT Broker集群事件失败", ex);
        }
    }

    /**
     * 处理状态类集群事件
     * 更新本地集群状态信息，包括节点状态、所有权、会话快照等
     *
     * @param event 集群事件对象
     */
    private void handleStateEvent(ClusterEvent event) {
        String eventType = event.getType();
        // 处理投递确认事件
        if (TYPE_DELIVERY_ACK.equals(eventType) && StringUtils.hasText(event.getRequestId())) {
            deliveryAcks.put(event.getRequestId(), event.getNodeId());
        }
        // 处理客户端确认事件
        else if (TYPE_CLIENT_ACK.equals(eventType) && StringUtils.hasText(event.getRequestId())) {
            clientAcks.put(event.getRequestId(), event.getNodeId());
        }
        // 处理节点心跳事件：更新节点状态表
        else if (TYPE_NODE_HEARTBEAT.equals(eventType) && StringUtils.hasText(event.getNodeJson())) {
            NodeState state = JSON.parseObject(event.getNodeJson(), NodeState.class);
            if (state != null && StringUtils.hasText(state.getNodeId())) {
                state.setUpdatedAt(System.currentTimeMillis());
                nodes.put(state.getNodeId(), state);
            }
            pruneExpiredNodes();
        }
        // 处理节点离开事件：从节点表中移除该节点
        else if (TYPE_NODE_LEFT.equals(eventType)) {
            handleNodeLeft(event.getNodeId());
        }
        // 处理客户端所有权声明事件
        else if (TYPE_OWNER_CLAIM.equals(eventType) && StringUtils.hasText(event.getTargetClientId())) {
            clientOwners.put(event.getTargetClientId(), event.getNodeId());
        }
        // 处理客户端所有权释放事件
        else if (TYPE_OWNER_RELEASE.equals(eventType) && StringUtils.hasText(event.getTargetClientId())) {
            clientOwners.remove(event.getTargetClientId(), event.getNodeId());
        }
        // 处理会话状态更新事件：更新会话快照表
        else if (TYPE_SESSION_UPSERT.equals(eventType) && StringUtils.hasText(event.getSessionJson())) {
            ClientSession clientSession = JSON.parseObject(event.getSessionJson(), ClientSession.class);
            if (clientSession != null && StringUtils.hasText(clientSession.getClientId())) {
                sessionSnapshots.put(clientSession.getClientId(), clientSession);
            }
        }
        // 处理保留消息更新事件
        else if (TYPE_RETAINED_UPSERT.equals(eventType) && StringUtils.hasText(event.getTopic())) {
            retainedMessages.put(event.getTopic(), toRetainedMessage(event));
        }
        // 处理保留消息删除事件
        else if (TYPE_RETAINED_DELETE.equals(eventType) && StringUtils.hasText(event.getTopic())) {
            retainedMessages.remove(event.getTopic());
        }
        // 处理添加离线消息事件
        else if (TYPE_OFFLINE_APPEND.equals(eventType) && StringUtils.hasText(event.getTargetClientId())) {
            offlineMessages.computeIfAbsent(event.getTargetClientId(), key -> new CopyOnWriteArrayList<>()).add(toOfflineMessage(event));
        }
        // 处理清空离线消息事件
        else if (TYPE_OFFLINE_CLEAR.equals(eventType) && StringUtils.hasText(event.getTargetClientId())) {
            offlineMessages.remove(event.getTargetClientId());
        }
    }

    private boolean isStateOnlyEvent(String type) {
        return TYPE_NODE_HEARTBEAT.equals(type)
                || TYPE_NODE_LEFT.equals(type)
                || TYPE_OWNER_CLAIM.equals(type)
                || TYPE_OWNER_RELEASE.equals(type)
                || TYPE_SESSION_UPSERT.equals(type)
                || TYPE_RETAINED_UPSERT.equals(type)
                || TYPE_RETAINED_DELETE.equals(type)
                || TYPE_OFFLINE_APPEND.equals(type)
                || TYPE_OFFLINE_CLEAR.equals(type);
    }

    private void ack(ClusterEvent sourceEvent, boolean delivered) {
        if (!delivered || !StringUtils.hasText(sourceEvent.getRequestId())) {
            return;
        }
        ClusterEvent ackEvent = new ClusterEvent();
        ackEvent.setType(TYPE_DELIVERY_ACK);
        ackEvent.setNodeId(nodeId);
        ackEvent.setRequestId(sourceEvent.getRequestId());
        ackEvent.setTargetNodeId(sourceEvent.getNodeId());
        ackEvent.setCreatedAt(System.currentTimeMillis());
        publishClusterEvent(ackEvent, false, false);
    }

    private boolean deliverToClient(String clientId, MqttBrokerMessage message, ClusterEvent event) {
        if (!StringUtils.hasText(clientId)) {
            return false;
        }
        return sessionRegistry.findByClientId(clientId.trim())
                .filter(MqttBrokerSession::isActive)
                .map(session -> deliverToSession(session, message, event))
                .orElse(false);
    }

    private boolean deliverToSessionUrl(String sessionUrl, MqttBrokerMessage message, ClusterEvent event) {
        if (!StringUtils.hasText(sessionUrl)) {
            return false;
        }
        return sessionRegistry.findBySessionUrl(sessionUrl.trim())
                .filter(MqttBrokerSession::isActive)
                .map(session -> deliverToSession(session, message, event))
                .orElse(false);
    }

    private boolean deliverToSession(MqttBrokerSession session, MqttBrokerMessage message, ClusterEvent event) {
        MqttQoS qos = message.getQos();
        int packetId = session.sendPublish(message, qos, message.isRetained());
        if (event != null && packetId > 0) {
            session.bindClusterAckContext(packetId, event.getRequestId(), event.getNodeId());
        }
        sessionRegistry.notifySessionUpdated(session);
        return packetId >= 0;
    }

    private void kickLocalClient(String clientId) {
        if (!StringUtils.hasText(clientId)) {
            return;
        }
        sessionRegistry.findByClientId(clientId.trim()).ifPresent(session -> {
            session.markDisconnectedGracefully();
            session.close();
        });
    }

    private void storeRetained(MqttBrokerMessage message, boolean broadcast) {
        if (!properties.isRetainedMessages() || !clusterProperties().isDistributedRetained() || !message.isRetained()) {
            return;
        }
        if (message.payload().length == 0) {
            retainedMessages.remove(message.getTopic());
            if (broadcast) {
                publishClusterEvent(baseEvent(TYPE_RETAINED_DELETE, message), false, false);
            }
            return;
        }
        RetainedMessage retainedMessage = toRetainedMessage(baseEvent(TYPE_RETAINED_UPSERT, message));
        retainedMessages.put(message.getTopic(), retainedMessage);
        if (broadcast) {
            publishClusterEvent(baseEvent(TYPE_RETAINED_UPSERT, message), false, false);
        }
    }

    private void enqueueOfflineMessages(MqttBrokerMessage message) {
        if (!clusterProperties().isOfflineQueue() || message.getQos() == MqttQoS.AT_MOST_ONCE) {
            return;
        }
        for (ClientSession clientSession : sessionSnapshots.values()) {
            if (clientSession == null
                    || clientSession.isCleanSession()
                    || clientSession.isKeepAlive()
                    || nodeId.equals(clientSession.getNodeId())
                    || !matchesAnySubscription(clientSession.getSubscribeTopics(), message.getTopic())) {
                continue;
            }
            enqueueOfflineMessage(clientSession.getClientId(), message, clientSession.getQos());
        }
    }

    private void enqueueOfflineMessage(String clientId, MqttBrokerMessage message, int subscriptionQos) {
        if (!StringUtils.hasText(clientId)) {
            return;
        }
        OfflineMessage offlineMessage = new OfflineMessage();
        offlineMessage.setTopic(message.getTopic());
        offlineMessage.setPayload(Base64.getEncoder().encodeToString(message.payload()));
        offlineMessage.setQos(message.getQos().value());
        offlineMessage.setDeliveryQos(lowerQos(message.getQos(), subscriptionQos).value());
        offlineMessage.setRetained(false);
        offlineMessage.setNodeId(nodeId);
        offlineMessage.setCreatedAt(System.currentTimeMillis());
        CopyOnWriteArrayList<OfflineMessage> messages = offlineMessages.computeIfAbsent(clientId, key -> new CopyOnWriteArrayList<>());
        messages.add(offlineMessage);
        int overflow = messages.size() - Math.max(1, clusterProperties().getMaxOfflineMessages());
        for (int i = 0; i < overflow; i++) {
            messages.remove(0);
        }
        ClusterEvent event = toOfflineEvent(clientId, offlineMessage);
        publishClusterEvent(event, false, false);
    }

    private boolean matchesAnySubscription(List<String> topicFilters, String topic) {
        if (topicFilters == null || topicFilters.isEmpty()) {
            return false;
        }
        for (String topicFilter : topicFilters) {
            if (StringUtils.hasText(topicFilter) && MqttTopicFilter.matches(topicFilter, topic)) {
                return true;
            }
        }
        return false;
    }

    private void startHeartbeat() {
        heartbeat();
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "mqtt-broker-cluster-heartbeat-" + nodeId);
            thread.setDaemon(true);
            return thread;
        });
        long interval = Math.max(1, clusterProperties().getHeartbeatIntervalMillis());
        executor.scheduleAtFixedRate(this::heartbeatSafely, interval, interval, TimeUnit.MILLISECONDS);
        heartbeatExecutor = executor;
    }

    private void heartbeatSafely() {
        try {
            heartbeat();
        } catch (RuntimeException ex) {
            log.warn("MQTT Broker集群心跳失败，nodeId={}", nodeId, ex);
        }
    }

    /**
     * 发送心跳消息
     * 定期向集群广播当前节点状态，包括主机、端口、会话数等信息
     * 其他节点收到心跳后会更新该节点的状态信息
     */
    private void heartbeat() {
        pruneExpiredNodes();
        // 构建当前节点状态
        NodeState state = new NodeState();
        state.setNodeId(nodeId);
        state.setHost(clusterProperties().getBindHost());
        state.setPort(clusterProperties().getBindPort());
        state.setMqttHost(resolveMqttAdvertiseHost());
        state.setMqttPort(properties.getPort());
        // 计算当前活跃会话数
        state.setSessionCount((int) sessionRegistry.sessions().stream().filter(MqttBrokerSession::isActive).count());
        state.setRole(role);
        state.setMasterNodeId(masterNodeId);
        state.setUpdatedAt(System.currentTimeMillis());
        // 更新本地节点状态表
        nodes.put(nodeId, state);
        // 构建心跳事件并广播到集群
        ClusterEvent event = new ClusterEvent();
        event.setType(TYPE_NODE_HEARTBEAT);
        event.setNodeId(nodeId);
        event.setNodeJson(JSON.toJSONString(state));
        event.setCreatedAt(System.currentTimeMillis());
        publishClusterEvent(event, false, false);
    }

    private void pruneExpiredNodes() {
        List<String> expiredNodeIds = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (Map.Entry<String, NodeState> entry : nodes.entrySet()) {
            String currentNodeId = entry.getKey();
            NodeState state = entry.getValue();
            if (!StringUtils.hasText(currentNodeId) || nodeId.equals(currentNodeId) || state == null) {
                continue;
            }
            if (now - state.getUpdatedAt() > nodeTimeoutMillis()) {
                expiredNodeIds.add(currentNodeId);
            }
        }
        for (String expiredNodeId : expiredNodeIds) {
            handleNodeLeft(expiredNodeId);
        }
    }

    private void handleNodeLeft(String offlineNodeId) {
        if (!StringUtils.hasText(offlineNodeId)) {
            return;
        }
        String resolvedOfflineNodeId = offlineNodeId.trim();
        nodes.remove(resolvedOfflineNodeId);
        if (nodeId.equals(resolvedOfflineNodeId)) {
            return;
        }
        reassignNodeClients(resolvedOfflineNodeId);
    }

    private void reassignNodeClients(String offlineNodeId) {
        List<ClientSession> sessionsToReassign = sessionSnapshots.values().stream()
                .filter(session -> session != null && offlineNodeId.equals(session.getNodeId()))
                .sorted(Comparator.comparing(ClientSession::getClientId))
                .toList();
        List<String> ownerClientsToReassign = clientOwners.entrySet().stream()
                .filter(entry -> offlineNodeId.equals(entry.getValue()) && StringUtils.hasText(entry.getKey()))
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
        if (sessionsToReassign.isEmpty() && ownerClientsToReassign.isEmpty()) {
            return;
        }

        Map<String, Integer> sessionCounts = new HashMap<>();
        for (Map.Entry<String, NodeState> entry : nodes.entrySet()) {
            NodeState state = entry.getValue();
            if (state != null && StringUtils.hasText(entry.getKey()) && isNodeAlive(entry.getKey())) {
                sessionCounts.put(entry.getKey(), Math.max(0, state.getSessionCount()));
            }
        }
        sessionCounts.putIfAbsent(nodeId, (int) sessionRegistry.sessions().stream().filter(MqttBrokerSession::isActive).count());

        for (ClientSession clientSession : sessionsToReassign) {
            String targetNodeId = selectFailoverNode(sessionCounts).orElse(null);
            if (!StringUtils.hasText(targetNodeId)) {
                clientOwners.remove(clientSession.getClientId(), offlineNodeId);
                continue;
            }
            clientOwners.put(clientSession.getClientId(), targetNodeId);
            clientSession.setNodeId(targetNodeId);
            clientSession.setKeepAlive(false);
            clientSession.setSessionUrl(null);
            sessionCounts.computeIfPresent(targetNodeId, (key, value) -> value + 1);
        }

        for (String clientId : ownerClientsToReassign) {
            boolean sessionHandled = false;
            for (ClientSession session : sessionsToReassign) {
                if (clientId.equals(session.getClientId())) {
                    sessionHandled = true;
                    break;
                }
            }
            if (sessionHandled) {
                continue;
            }
            String targetNodeId = selectFailoverNode(sessionCounts).orElse(null);
            if (!StringUtils.hasText(targetNodeId)) {
                clientOwners.remove(clientId, offlineNodeId);
                continue;
            }
            clientOwners.put(clientId, targetNodeId);
            sessionCounts.computeIfPresent(targetNodeId, (key, value) -> value + 1);
        }
    }

    private Optional<String> selectFailoverNode(Map<String, Integer> sessionCounts) {
        return sessionCounts.entrySet().stream()
                .filter(entry -> StringUtils.hasText(entry.getKey()))
                .filter(entry -> isNodeAlive(entry.getKey()))
                .min(Map.Entry.<String, Integer>comparingByValue().thenComparing(Map.Entry.comparingByKey()))
                .map(Map.Entry::getKey);
    }

    private ClusterEvent kickClientEvent(String clientId, String targetNodeId) {
        ClusterEvent event = new ClusterEvent();
        event.setType(TYPE_KICK_CLIENT);
        event.setNodeId(nodeId);
        event.setTargetClientId(clientId);
        event.setTargetNodeId(targetNodeId);
        event.setCreatedAt(System.currentTimeMillis());
        return event;
    }

    private ClusterEvent toPublishEvent(MqttBrokerMessage message) {
        return baseEvent(TYPE_PUBLISH, message);
    }

    private ClusterEvent toDirectClientEvent(String clientId, MqttBrokerMessage message) {
        ClusterEvent event = baseEvent(TYPE_DIRECT_CLIENT, message);
        event.setTargetClientId(clientId);
        event.setTargetNodeId(resolveClientOwner(clientId));
        return event;
    }

    private ClusterEvent toDirectSessionEvent(String sessionUrl, MqttBrokerMessage message) {
        ClusterEvent event = baseEvent(TYPE_DIRECT_SESSION, message);
        event.setTargetSessionUrl(sessionUrl);
        return event;
    }

    private ClusterEvent baseEvent(String type, MqttBrokerMessage message) {
        ClusterEvent event = new ClusterEvent();
        event.setType(type);
        event.setNodeId(nodeId);
        event.setTopic(message.getTopic());
        event.setPayload(Base64.getEncoder().encodeToString(message.payload()));
        event.setQos(message.getQos().value());
        event.setRetained(message.isRetained());
        event.setCreatedAt(System.currentTimeMillis());
        return event;
    }

    private ClusterEvent toOfflineEvent(String clientId, OfflineMessage offlineMessage) {
        ClusterEvent event = new ClusterEvent();
        event.setType(TYPE_OFFLINE_APPEND);
        event.setNodeId(nodeId);
        event.setTargetClientId(clientId);
        event.setTopic(offlineMessage.getTopic());
        event.setPayload(offlineMessage.getPayload());
        event.setQos(offlineMessage.getQos());
        event.setDeliveryQos(offlineMessage.getDeliveryQos());
        event.setRetained(offlineMessage.isRetained());
        event.setCreatedAt(System.currentTimeMillis());
        return event;
    }

    private void broadcastOfflineClear(String clientId) {
        ClusterEvent event = new ClusterEvent();
        event.setType(TYPE_OFFLINE_CLEAR);
        event.setNodeId(nodeId);
        event.setTargetClientId(clientId);
        event.setCreatedAt(System.currentTimeMillis());
        publishClusterEvent(event, false, false);
    }

    private MqttBrokerMessage toBrokerMessage(ClusterMessage event) {
        byte[] payload = StringUtils.hasText(event.getPayload())
                ? Base64.getDecoder().decode(event.getPayload())
                : new byte[0];
        return new MqttBrokerMessage(event.getTopic(), payload, MqttQoS.valueOf(event.getQos()), event.isRetained());
    }

    private RetainedMessage toRetainedMessage(ClusterEvent event) {
        RetainedMessage retainedMessage = new RetainedMessage();
        retainedMessage.setTopic(event.getTopic());
        retainedMessage.setPayload(event.getPayload());
        retainedMessage.setQos(event.getQos());
        retainedMessage.setRetained(event.isRetained());
        retainedMessage.setNodeId(event.getNodeId());
        retainedMessage.setCreatedAt(event.getCreatedAt());
        return retainedMessage;
    }

    private OfflineMessage toOfflineMessage(ClusterEvent event) {
        OfflineMessage offlineMessage = new OfflineMessage();
        offlineMessage.setTopic(event.getTopic());
        offlineMessage.setPayload(event.getPayload());
        offlineMessage.setQos(event.getQos());
        offlineMessage.setDeliveryQos(event.getDeliveryQos());
        offlineMessage.setRetained(event.isRetained());
        offlineMessage.setNodeId(event.getNodeId());
        offlineMessage.setCreatedAt(event.getCreatedAt());
        return offlineMessage;
    }

    private ClientSession toClientSession(MqttBrokerSession session) {
        ClientSession clientSession = new ClientSession();
        clientSession.setClientId(session.clientId());
        clientSession.setNodeId(nodeId);
        clientSession.setCleanSession(session.cleanSession());
        clientSession.setKeepAlive(session.isActive());
        clientSession.setLastActivityTime(System.currentTimeMillis());
        clientSession.setSessionUrl(session.channel() == null || session.channel().remoteAddress() == null
                ? null
                : String.valueOf(session.channel().remoteAddress()));
        clientSession.setQos(session.subscriptions().values().stream().mapToInt(value -> value.value()).max().orElse(0));
        clientSession.setPublicTopics(session.publicTopics());
        clientSession.setSubscribeTopics(session.subscriptions().keySet().stream().sorted().toList());
        clientSession.setQosStateSnapshot(session.qosStateSnapshot());
        return clientSession;
    }

    private String resolveClientOwner(String clientId) {
        if (!StringUtils.hasText(clientId)) {
            return null;
        }
        String owner = clientOwners.get(clientId.trim());
        return isNodeAlive(owner) ? owner : null;
    }

    private String resolveMqttAdvertiseHost() {
        String host = properties.getHost();
        if (!StringUtils.hasText(host) || "0.0.0.0".equals(host) || "::".equals(host)) {
            return clusterProperties().getBindHost();
        }
        return host;
    }

    private long nodeTimeoutMillis() {
        long heartbeatInterval = Math.max(1, clusterProperties().getHeartbeatIntervalMillis());
        long configuredTimeout = Math.max(1, clusterProperties().getNodeTimeoutMillis());
        return Math.max(5000, Math.max(configuredTimeout, heartbeatInterval * 10));
    }

    private MqttBrokerProperties.Cluster clusterProperties() {
        return properties.getCluster() == null ? new MqttBrokerProperties.Cluster() : properties.getCluster();
    }

    private static MqttQoS lowerQos(MqttQoS publishQos, int subscriptionQos) {
        return MqttQoS.valueOf(Math.min(publishQos.value(), subscriptionQos));
    }

    private static String resolveNodeId(MqttBrokerProperties properties) {
        if (properties.getCluster() != null && StringUtils.hasText(properties.getCluster().getNodeId())) {
            return properties.getCluster().getNodeId().trim();
        }
        return "wcdk-mqtt-" + UUID.nameUUIDFromBytes((UUID.randomUUID() + ":" + System.nanoTime()).getBytes(StandardCharsets.UTF_8));
    }

    private static String resolveMasterNodeId(MqttBrokerProperties properties) {
        if (properties.getCluster() == null || properties.getCluster().getMaster() == null) {
            return null;
        }
        MqttBrokerProperties.MasterNode master = properties.getCluster().getMaster();
        if (!StringUtils.hasText(master.getHost())) {
            return null;
        }
        return master.getHost() + ":" + master.getPort();
    }

    private interface ClusterMessage {

        String getTopic();

        String getPayload();

        int getQos();

        boolean isRetained();
    }

    @Data
    public static class ClusterEvent implements ClusterMessage {

        private String type;

        private String nodeId;

        private String requestId;

        private String targetNodeId;

        private String topic;

        private String payload;

        private int qos;

        private int deliveryQos;

        private boolean retained;

        private String targetClientId;

        private String targetSessionUrl;

        private String sessionJson;

        private String nodeJson;

        private int packetId;

        private String ackType;

        private long createdAt;
    }

    @Data
    public static class RetainedMessage implements ClusterMessage {

        private String topic;

        private String payload;

        private int qos;

        private boolean retained;

        private String nodeId;

        private long createdAt;
    }

    @Data
    public static class OfflineMessage implements ClusterMessage {

        private String topic;

        private String payload;

        private int qos;

        private int deliveryQos;

        private boolean retained;

        private String nodeId;

        private long createdAt;
    }

    @Data
    public static class NodeState {

        private String nodeId;

        private String host;

        private int port;

        private String mqttHost;

        private int mqttPort;

        private int sessionCount;

        private MqttBrokerProperties.NodeRole role;

        private String masterNodeId;

        private long updatedAt;
    }
}
