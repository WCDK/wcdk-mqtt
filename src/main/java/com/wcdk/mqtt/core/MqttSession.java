package com.wcdk.mqtt.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

/***
* MQTT 客户端会话，保存订阅关系、QoS 待确认状态与离线消息队列。
* @author wcdk
***/
@Slf4j
public class MqttSession {

    /** 出站消息等待 PUBACK（QoS 1） */
    static final String STATE_WAITING_PUBACK = "WAITING_PUBACK";
    /** 出站消息等待 PUBREC（QoS 2 第一步） */
    static final String STATE_WAITING_PUBREC = "WAITING_PUBREC";
    /** 出站消息等待 PUBCOMP（QoS 2 第二步） */
    static final String STATE_WAITING_PUBCOMP = "WAITING_PUBCOMP";

    private final String clientId;
    private final boolean cleanSession;
    private final String username;
    private final String ownerNodeId;
    /** 客户端上报的 Keep Alive 秒数，0 表示不检查 */
    private volatile int keepAliveSeconds;
    /** MQTT 协议级别，3.x 会话生命周期沿用清理会话语义 */
    private volatile int protocolLevel = 4;
    /** MQTT5 会话过期秒数，旧协议与 0xFFFFFFFF 归一化为永不过期 */
    private volatile long sessionExpirySeconds = Long.MAX_VALUE;
    /** 最后一次断开连接的时间，用于 MQTT5 会话过期判断 */
    private volatile long disconnectedAtMillis;

    /** 当前连接句柄，离线时为 null */
    private volatile MqttConnectionHandle connectionHandle;
    /** 遗嘱消息，客户端异常断开时发布 */
    private volatile MqttBrokerMessage willMessage;
    /** 是否收到过规范的 DISCONNECT 报文 */
    private volatile boolean gracefulDisconnect;
    /** 最后活跃时间，用于 Keep Alive 超时判断 */
    private volatile long lastActivityTime;

    /** 订阅关系：主题过滤器 -> 订阅 QoS */
    private final Map<String, Integer> subscriptions = new ConcurrentHashMap<>();
    /** 本会话发布过的主题，用于管理接口展示 */
    private final Set<String> publishedTopics = ConcurrentHashMap.newKeySet();
    /** 出站待确认消息：包 ID -> 待确认消息 */
    private final Map<Integer, PendingOutbound> outboundPending = new ConcurrentHashMap<>();
    /** 入站 QoS 2 待发布消息：包 ID -> 消息 */
    private final Map<Integer, MqttBrokerMessage> inboundQos2 = new ConcurrentHashMap<>();
    /** 入站 QoS 1 已处理包 ID，用于重复报文判断 */
    private final Set<Integer> inboundQos1 = ConcurrentHashMap.newKeySet();
    /** 离线消息队列，仅非清理会话在离线时缓存 */
    private final Queue<MqttBrokerMessage> offlineQueue = new ConcurrentLinkedQueue<>();
    /** 离线队列最大容量 */
    private volatile int maxOfflineCapacity = 1000;
    /** 包 ID 发生器 */
    private final AtomicInteger packetIdSequence = new AtomicInteger();

    public MqttSession(String clientId, boolean cleanSession, String username, String ownerNodeId) {
        this.clientId = clientId;
        this.cleanSession = cleanSession;
        this.username = username;
        this.ownerNodeId = ownerNodeId;
        this.lastActivityTime = System.currentTimeMillis();
        this.disconnectedAtMillis = this.lastActivityTime;
    }

    public String clientId() {
        return clientId;
    }

    public boolean cleanSession() {
        return cleanSession;
    }

    public int protocolLevel() {
        return protocolLevel;
    }

    public void setProtocolLevel(int protocolLevel) {
        this.protocolLevel = protocolLevel;
    }

    public long sessionExpirySeconds() {
        return sessionExpirySeconds;
    }

    public void setSessionExpirySeconds(long sessionExpirySeconds) {
        this.sessionExpirySeconds = Math.max(0, sessionExpirySeconds);
    }

    /***
    * 判断会话断开后是否仍保留在注册表：MQTT 3.x 按清理会话语义，MQTT5 按会话过期秒数。
    * @author wcdk
    * @return 断开后仍保留返回 true
    ***/
    public boolean persistsAfterDisconnect() {
        return protocolLevel < 5 ? !cleanSession : sessionExpirySeconds > 0;
    }

    /***
    * 判断离线会话是否已超过 MQTT5 会话过期时间，在线会话永不过期。
    * @author wcdk
    * @param now 当前时间毫秒
    * @return 已过期返回 true
    ***/
    public boolean isSessionExpired(long now) {
        if (isActive() || sessionExpirySeconds == Long.MAX_VALUE) {
            return false;
        }
        return now - disconnectedAtMillis >= sessionExpirySeconds * 1000L;
    }

    public String username() {
        return username;
    }

    public String ownerNodeId() {
        return ownerNodeId;
    }

    public int keepAliveSeconds() {
        return keepAliveSeconds;
    }

    public void setKeepAliveSeconds(int keepAliveSeconds) {
        this.keepAliveSeconds = Math.max(0, keepAliveSeconds);
    }

    public MqttBrokerMessage willMessage() {
        return willMessage;
    }

    public void setWillMessage(MqttBrokerMessage willMessage) {
        this.willMessage = willMessage;
    }

    public long lastActivityTime() {
        return lastActivityTime;
    }

    public void touch() {
        this.lastActivityTime = System.currentTimeMillis();
    }

    public boolean isGracefulDisconnect() {
        return gracefulDisconnect;
    }

    public void markGracefulDisconnect() {
        this.gracefulDisconnect = true;
    }

    /***
    * 绑定新的客户端连接，重置优雅断开标记。
    * @author wcdk
    * @param handle 连接句柄
    ***/
    public void attach(MqttConnectionHandle handle) {
        this.connectionHandle = handle;
        this.gracefulDisconnect = false;
        touch();
    }

    /***
    * 解绑当前连接，会话转入离线状态并记录过期计时起点。
    * @author wcdk
    ***/
    public void detach() {
        this.connectionHandle = null;
        this.disconnectedAtMillis = System.currentTimeMillis();
    }

    public boolean isActive() {
        MqttConnectionHandle handle = connectionHandle;
        return handle != null && handle.isActive();
    }

    public String remoteAddress() {
        MqttConnectionHandle handle = connectionHandle;
        return handle == null ? null : handle.remoteAddress();
    }

    /***
    * 关闭当前连接。
    * @author wcdk
    ***/
    public void closeConnection() {
        MqttConnectionHandle handle = connectionHandle;
        if (handle != null) {
            handle.close();
        }
    }

    public Map<String, Integer> subscriptions() {
        return Map.copyOf(subscriptions);
    }

    public List<String> subscriptionFilters() {
        return subscriptions.keySet().stream().sorted().toList();
    }

    /***
    * 返回订阅过滤器的实时视图，供注册表重建路由索引使用（弱一致迭代，线程安全）。
    * @author wcdk
    * @return 过滤器集合实时视图
    ***/
    Set<String> subscriptionFilterNames() {
        return subscriptions.keySet();
    }

    /***
    * 按过滤器精确查询订阅 QoS，热路径使用，避免复制整个订阅表。
    * @author wcdk
    * @param topicFilter 主题过滤器
    * @return 订阅 QoS，未订阅该过滤器返回 null
    ***/
    public Integer subscriptionQos(String topicFilter) {
        return subscriptions.get(topicFilter);
    }

    public void subscribe(String topicFilter, int qos) {
        subscriptions.put(topicFilter, qos);
        touch();
    }

    public void unsubscribe(String topicFilter) {
        subscriptions.remove(topicFilter);
        touch();
    }

    public Set<String> publishedTopics() {
        return Set.copyOf(publishedTopics);
    }

    public void recordPublishedTopic(String topic) {
        if (topic != null && !topic.isBlank()) {
            publishedTopics.add(topic);
        }
    }

    /***
    * 计算命中该主题的所有订阅中的最高 QoS。
    * @author wcdk
    * @param topic 主题名称
    * @return 最高订阅 QoS，未命中返回 -1
    ***/
    public int maxSubscriptionQos(String topic) {
        int maxQos = -1;
        for (Map.Entry<String, Integer> entry : subscriptions.entrySet()) {
            if (!MqttTopicMatcher.maybeMatches(entry.getKey(), topic)) {
                continue;
            }
            if (MqttTopicMatcher.matches(entry.getKey(), topic)) {
                maxQos = Math.max(maxQos, entry.getValue());
            }
        }
        return maxQos;
    }

    /***
    * 判断入站 QoS 1 报文是否为重复投递。
    * @author wcdk
    * @param packetId 包 ID
    * @param duplicateFlag 报文 DUP 标记
    * @return 重复返回 true
    ***/
    public synchronized boolean isInboundQos1Duplicate(int packetId, boolean duplicateFlag) {
        if (duplicateFlag && inboundQos1.contains(packetId)) {
            return true;
        }
        inboundQos1.add(packetId);
        // 防止集合无限增长，超过阈值时整体重置
        if (inboundQos1.size() > 4096) {
            inboundQos1.clear();
        }
        return false;
    }

    /***
    * 保存入站 QoS 2 报文，等待 PUBREL 后再发布。
    * @author wcdk
    * @param packetId 包 ID
    * @param message 消息
    ***/
    public void saveInboundQos2(int packetId, MqttBrokerMessage message) {
        inboundQos2.putIfAbsent(packetId, message);
    }

    /***
    * 取出入站 QoS 2 待发布消息。
    * @author wcdk
    * @param packetId 包 ID
    * @return 消息，不存在返回 null
    ***/
    public MqttBrokerMessage removeInboundQos2(int packetId) {
        return inboundQos2.remove(packetId);
    }

    /***
    * 向客户端投递消息，按 QoS 决定是否进入待确认状态或离线队列。
    * @author wcdk
    * @param message 消息
    * @param qos 投递 QoS
    * @param retained 是否为保留消息
    * @return QoS 0 返回 0；入队返回 -1；否则返回分配的包 ID
    ***/
    public synchronized int sendPublish(MqttBrokerMessage message, int qos, boolean retained) {
        if (qos <= 0) {
            writeToConnection(message, 0, retained, 0, false);
            return 0;
        }
        if (!isActive()) {
            // 仅断开后仍保留的会话才缓存离线消息
            if (persistsAfterDisconnect()) {
                enqueueOffline(message, maxOfflineCapacity);
            }
            return -1;
        }
        int packetId = nextPacketId();
        String state = qos >= 2 ? STATE_WAITING_PUBREC : STATE_WAITING_PUBACK;
        outboundPending.put(packetId, new PendingOutbound(packetId, message, qos, retained, state,
                System.currentTimeMillis(), 0));
        writeToConnection(message, qos, retained, packetId, false);
        return packetId;
    }

    /***
    * QoS 1 收到 PUBACK 后确认出站消息。
    * @author wcdk
    * @param packetId 包 ID
    * @return 被确认的消息，未命中返回 null
    ***/
    public synchronized MqttBrokerMessage confirmOutboundQos1(int packetId) {
        PendingOutbound pending = outboundPending.get(packetId);
        if (pending != null && STATE_WAITING_PUBACK.equals(pending.state())) {
            outboundPending.remove(packetId);
            return pending.message();
        }
        return null;
    }

    /***
    * QoS 2 收到 PUBREC 后推进状态，已处于等待 PUBCOMP 的重复报文同样返回 true 以便重发 PUBREL。
    * @author wcdk
    * @param packetId 包 ID
    * @return 需要发送 PUBREL 返回 true
    ***/
    public synchronized boolean onOutboundQos2PubRec(int packetId) {
        PendingOutbound pending = outboundPending.get(packetId);
        if (pending == null) {
            return false;
        }
        if (STATE_WAITING_PUBCOMP.equals(pending.state())) {
            return true;
        }
        if (!STATE_WAITING_PUBREC.equals(pending.state())) {
            return false;
        }
        outboundPending.put(packetId, pending.withState(STATE_WAITING_PUBCOMP));
        return true;
    }

    /***
    * QoS 2 收到 PUBCOMP 后确认出站消息。
    * @author wcdk
    * @param packetId 包 ID
    * @return 被确认的消息，未命中返回 null
    ***/
    public synchronized MqttBrokerMessage confirmOutboundQos2(int packetId) {
        PendingOutbound pending = outboundPending.get(packetId);
        if (pending != null && STATE_WAITING_PUBCOMP.equals(pending.state())) {
            outboundPending.remove(packetId);
            return pending.message();
        }
        return null;
    }

    /***
    * 重连后重放所有出站待确认消息，QoS 2 已进入第二步的改发 PUBREL。
    * @author wcdk
    ***/
    public synchronized void replayPendingMessages() {
        if (!isActive()) {
            return;
        }
        for (PendingOutbound pending : new ArrayList<>(outboundPending.values())) {
            if (STATE_WAITING_PUBCOMP.equals(pending.state())) {
                writePubRel(pending.packetId());
                continue;
            }
            writeToConnection(pending.message(), pending.qos(), pending.retained(), pending.packetId(), true);
        }
    }

    /***
    * 冲刷离线消息队列。
    * @author wcdk
    ***/
    public synchronized void drainOfflineQueue() {
        if (!isActive()) {
            return;
        }
        MqttBrokerMessage queued;
        while ((queued = offlineQueue.poll()) != null) {
            int subscriptionQos = maxSubscriptionQos(queued.topic());
            if (subscriptionQos < 0) {
                continue;
            }
            sendPublish(queued, MqttTopicMatcher.minQos(queued.qos(), subscriptionQos), queued.retained());
        }
    }

    /***
    * 将消息放入离线队列并按容量裁剪。
    * @author wcdk
    * @param message 消息
    * @param maxCapacity 队列最大容量
    ***/
    public void enqueueOffline(MqttBrokerMessage message, int maxCapacity) {
        offlineQueue.add(message);
        int overflow = offlineQueue.size() - Math.max(1, maxCapacity);
        for (int i = 0; i < overflow; i++) {
            offlineQueue.poll();
        }
    }

    public int offlineQueueSize() {
        return offlineQueue.size();
    }

    public int maxOfflineCapacity() {
        return maxOfflineCapacity;
    }

    public void setMaxOfflineCapacity(int maxOfflineCapacity) {
        this.maxOfflineCapacity = Math.max(1, maxOfflineCapacity);
    }

    public List<MqttBrokerMessage> drainOfflineSnapshot() {
        List<MqttBrokerMessage> snapshot = new ArrayList<>(offlineQueue);
        offlineQueue.clear();
        return snapshot;
    }

    public void restoreOffline(List<MqttBrokerMessage> messages) {
        if (messages == null) {
            return;
        }
        for (MqttBrokerMessage message : messages) {
            offlineQueue.add(message);
        }
    }

    public int outboundPendingCount() {
        return outboundPending.size();
    }

    public int inboundQos2Count() {
        return inboundQos2.size();
    }

    public List<PendingOutbound> outboundPendingSnapshot() {
        return List.copyOf(outboundPending.values());
    }

    public List<Integer> inboundQos2PacketIds() {
        return List.copyOf(inboundQos2.keySet());
    }

    /***
    * 从既有会话迁移状态，用于同 clientId 重连时恢复订阅与 QoS 状态。
    * @author wcdk
    * @param previous 旧会话
    ***/
    public synchronized void restoreStateFrom(MqttSession previous) {
        subscriptions.clear();
        subscriptions.putAll(previous.subscriptions);
        publishedTopics.clear();
        publishedTopics.addAll(previous.publishedTopics);
        outboundPending.clear();
        outboundPending.putAll(previous.outboundPending);
        inboundQos2.clear();
        inboundQos2.putAll(previous.inboundQos2);
        offlineQueue.clear();
        offlineQueue.addAll(previous.offlineQueue);
        packetIdSequence.set(previous.packetIdSequence.get());
    }

    /***
    * 重投出站超时未确认的消息，超过最大重投次数后放弃并清理。
    * @author wcdk
    * @param timeoutMillis 单次确认超时毫秒
    * @param maxAttempts 最大重投次数
    ***/
    public synchronized void retransmitExpired(long timeoutMillis, int maxAttempts) {
        if (!isActive()) {
            return;
        }
        long now = System.currentTimeMillis();
        long baseTimeout = Math.max(1000L, timeoutMillis);
        int limit = Math.max(1, maxAttempts);
        for (PendingOutbound pending : new ArrayList<>(outboundPending.values())) {
            long deadline = baseTimeout * (pending.attempts() + 1L);
            if (now - pending.createdAt() <= deadline) {
                continue;
            }
            if (pending.attempts() >= limit) {
                outboundPending.remove(pending.packetId());
                log.warn("出站消息重投超限已放弃，客户端ID：{}，包ID：{}，主题：{}，重投次数：{}",
                        clientId, pending.packetId(), pending.message().topic(), pending.attempts());
                continue;
            }
            PendingOutbound updated = pending.withAttempt();
            outboundPending.put(updated.packetId(), updated);
            if (STATE_WAITING_PUBCOMP.equals(updated.state())) {
                writePubRel(updated.packetId());
            } else {
                writeToConnection(updated.message(), updated.qos(), updated.retained(), updated.packetId(), true);
            }
        }
    }

    /***
    * 生成 QoS 状态快照，供集群间同步会话状态。
    * @author wcdk
    * @return 快照对象
    ***/
    public synchronized SessionSnapshot snapshot() {
        SessionSnapshot snapshot = new SessionSnapshot();
        snapshot.setClientId(clientId);
        snapshot.setCleanSession(cleanSession);
        snapshot.setProtocolLevel(protocolLevel);
        snapshot.setSessionExpirySeconds(sessionExpirySeconds);
        snapshot.setUsername(username);
        snapshot.setOwnerNodeId(ownerNodeId);
        snapshot.setOnline(isActive());
        snapshot.setSubscriptions(new LinkedHashMap<>(subscriptions));
        snapshot.setPublishedTopics(new ArrayList<>(publishedTopics));
        snapshot.setPacketIdSequence(packetIdSequence.get());
        List<SnapshotMessage> pendingList = new ArrayList<>();
        for (PendingOutbound pending : outboundPending.values()) {
            SnapshotMessage item = new SnapshotMessage();
            item.setPacketId(pending.packetId());
            item.setTopic(pending.message().topic());
            item.setPayload(java.util.Base64.getEncoder().encodeToString(pending.message().payload()));
            item.setQos(pending.qos());
            item.setRetained(pending.retained());
            item.setState(pending.state());
            pendingList.add(item);
        }
        snapshot.setOutboundPending(pendingList);
        List<SnapshotMessage> inboundList = new ArrayList<>();
        for (Map.Entry<Integer, MqttBrokerMessage> entry : inboundQos2.entrySet()) {
            SnapshotMessage item = new SnapshotMessage();
            item.setPacketId(entry.getKey());
            item.setTopic(entry.getValue().topic());
            item.setPayload(java.util.Base64.getEncoder().encodeToString(entry.getValue().payload()));
            item.setQos(entry.getValue().qos());
            item.setRetained(entry.getValue().retained());
            inboundList.add(item);
        }
        snapshot.setInboundQos2(inboundList);
        return snapshot;
    }

    /***
    * 从快照恢复 QoS 状态，用于接管其他节点的会话。
    * @author wcdk
    * @param snapshot 快照对象
    ***/
    public synchronized void restoreSnapshot(SessionSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        subscriptions.clear();
        if (snapshot.getSubscriptions() != null) {
            subscriptions.putAll(snapshot.getSubscriptions());
        }
        publishedTopics.clear();
        if (snapshot.getPublishedTopics() != null) {
            publishedTopics.addAll(snapshot.getPublishedTopics());
        }
        outboundPending.clear();
        if (snapshot.getOutboundPending() != null) {
            for (SnapshotMessage item : snapshot.getOutboundPending()) {
                MqttBrokerMessage message = decodeMessage(item);
                if (message == null || item.getPacketId() <= 0) {
                    continue;
                }
                outboundPending.put(item.getPacketId(), new PendingOutbound(
                        item.getPacketId(), message, item.getQos(), item.isRetained(),
                        item.getState() == null ? STATE_WAITING_PUBACK : item.getState(),
                        System.currentTimeMillis(), 0));
            }
        }
        inboundQos2.clear();
        if (snapshot.getInboundQos2() != null) {
            for (SnapshotMessage item : snapshot.getInboundQos2()) {
                MqttBrokerMessage message = decodeMessage(item);
                if (message != null && item.getPacketId() > 0) {
                    inboundQos2.put(item.getPacketId(), message);
                }
            }
        }
        packetIdSequence.set(Math.max(0, snapshot.getPacketIdSequence()));
    }

    private static MqttBrokerMessage decodeMessage(SnapshotMessage item) {
        if (item.getTopic() == null || item.getTopic().isBlank()) {
            return null;
        }
        byte[] body;
        try {
            body = item.getPayload() == null || item.getPayload().isBlank()
                    ? new byte[0]
                    : java.util.Base64.getDecoder().decode(item.getPayload());
        } catch (IllegalArgumentException ex) {
            log.warn("会话快照负载解码失败，包ID：{}，异常信息：{}", item.getPacketId(), ex.getMessage());
            return null;
        }
        return new MqttBrokerMessage(null, item.getTopic(), body, item.getQos(), item.isRetained());
    }

    /***
    * 取出一个可用包 ID，回绕后跳过仍在等待确认的包 ID，
    * 避免覆盖未确认消息导致 PUBACK 误确认与消息丢失。
    * @author wcdk
    * @return 1 到 65535 之间的包 ID
    ***/
    private int nextPacketId() {
        int next = packetIdSequence.updateAndGet(current -> current >= 65535 ? 1 : current + 1);
        // 最多尝试一圈，全被占用时退化为覆盖（极端场景下由重投机制兜底）
        for (int attempts = 0; attempts < 65535 && outboundPending.containsKey(next); attempts++) {
            next = packetIdSequence.updateAndGet(current -> current >= 65535 ? 1 : current + 1);
        }
        return next;
    }

    private void writeToConnection(MqttBrokerMessage message, int qos, boolean retained, int packetId, boolean duplicate) {
        MqttConnectionHandle handle = connectionHandle;
        if (handle == null || !handle.isActive()) {
            return;
        }
        try {
            handle.send(message, qos, retained, packetId, duplicate);
        } catch (RuntimeException ex) {
            log.warn("向客户端写出消息失败，客户端ID：{}，主题：{}，异常信息：{}",
                    clientId, message.topic(), ex.getMessage());
        }
    }

    private void writePubRel(int packetId) {
        MqttConnectionHandle handle = connectionHandle;
        if (handle == null || !handle.isActive()) {
            return;
        }
        handle.sendPubRel(packetId);
    }

    /***
    * 出站待确认消息记录。
    * @author wcdk
    ***/
    public record PendingOutbound(
            int packetId,
            MqttBrokerMessage message,
            int qos,
            boolean retained,
            String state,
            long createdAt,
            int attempts) {

        PendingOutbound withState(String newState) {
            return new PendingOutbound(packetId, message, qos, retained, newState, createdAt, attempts);
        }

        PendingOutbound withAttempt() {
            return new PendingOutbound(packetId, message, qos, retained, state, createdAt, attempts + 1);
        }
    }

    /***
    * 会话状态快照，用于集群同步与管理接口。
    * @author wcdk
    ***/
    @lombok.Data
    public static class SessionSnapshot {
        private String clientId;
        private boolean cleanSession;
        /** MQTT 协议级别，缺省按 3.1.1 处理以兼容旧节点快照 */
        private int protocolLevel = 4;
        /** MQTT5 会话过期秒数，缺省永不过期，与旧节点清理会话语义一致 */
        private long sessionExpirySeconds = Long.MAX_VALUE;
        private String username;
        private String ownerNodeId;
        private boolean online;
        private Map<String, Integer> subscriptions = new LinkedHashMap<>();
        private List<String> publishedTopics = new ArrayList<>();
        private int packetIdSequence;
        private List<SnapshotMessage> outboundPending = new ArrayList<>();
        private List<SnapshotMessage> inboundQos2 = new ArrayList<>();

        /***
        * 判断快照对应会话断开后是否仍保留在对端注册表。
        * @author wcdk
        * @return 断开后仍保留返回 true
        ***/
        public boolean persistsAfterDisconnect() {
            return protocolLevel < 5 ? !cleanSession : sessionExpirySeconds > 0;
        }
    }

    /***
    * 快照中的消息条目，负载使用 Base64 编码。
    * @author wcdk
    ***/
    @lombok.Data
    public static class SnapshotMessage {
        private int packetId;
        private String topic;
        private String payload;
        private int qos;
        private boolean retained;
        private String state;
    }
}
