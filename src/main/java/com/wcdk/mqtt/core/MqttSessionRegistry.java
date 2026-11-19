package com.wcdk.mqtt.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.wcdk.mqtt.config.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
* MQTT 会话注册表，负责会话注册销毁、消息本地路由与离线消息缓冲。
* @author wcdk
***/
public class MqttSessionRegistry {

    private static final Logger log = LoggerFactory.getLogger(MqttSessionRegistry.class);

    private final MqttProperties properties;
    private final MqttAcl acl;
    private final MqttRetainedStore retainedStore;
    private final List<MqttPublishListener> publishListeners = new ArrayList<>();
    private final List<MqttSessionListener> sessionListeners = new ArrayList<>();

    /** 本节点在线或离线的本地会话 */
    private final Map<String, MqttSession> localSessions = new ConcurrentHashMap<>();
    /** 其他节点同步来的会话快照，用于跨节点路由与离线缓冲 */
    private final Map<String, MqttSession.SessionSnapshot> remoteSnapshots = new ConcurrentHashMap<>();
    /** 已删除远端快照的版本墓碑，避免陈旧的全量同步把会话重新写回 */
    private final Map<String, RemovedSnapshot> removedSnapshots = new ConcurrentHashMap<>();
    /** 已知但尚未接入本节点的持久会话离线缓冲 */
    private final Map<String, Queue<MqttBrokerMessage>> pendingOffline = new ConcurrentHashMap<>();

    /** 路由索引：过滤器首层字面量 → 该首层下的过滤器集合 */
    private final Map<String, Set<String>> literalFilterIndex = new ConcurrentHashMap<>();
    /** 路由索引：首层为 + 或 # 的通配过滤器集合 */
    private final Set<String> wildcardFirstLevelFilters = ConcurrentHashMap.newKeySet();
    /** 路由索引：过滤器 → 订阅该过滤器的客户端 ID 集合 */
    private final Map<String, Set<String>> filterSubscribers = new ConcurrentHashMap<>();
    /** 路由索引：客户端 ID → 已索引的过滤器集合，用于订阅变更与会话移除时的反向清理 */
    private final Map<String, Set<String>> clientFilterIndex = new ConcurrentHashMap<>();
    /** 路由索引维护锁：订阅变更低频，串行化增量重建以保证 diff 原子性，投递路径无锁读取 */
    private final Object routingIndexLock = new Object();
    /** 同一 clientId 的接入与移除串行化分片锁，避免同名客户端并发接入出现双在线 */
    private static final int REGISTRATION_LOCK_STRIPES = 64;
    private final Object[] registrationLocks = new Object[REGISTRATION_LOCK_STRIPES];

    /**
     * 远端订阅路由索引：主题过滤器 → 属主节点ID → 订阅计数。
     * 用于发布时仅向真正存在订阅者（含离线持久会话）的节点转发，避免集群全量广播。
     */
    private final Map<String, Map<String, Integer>> remoteFilterNodeCounts = new ConcurrentHashMap<>();
    /** 远端过滤器首层字面量索引：首层 → 过滤器集合 */
    private final Map<String, Set<String>> remoteLiteralFilters = new ConcurrentHashMap<>();
    /** 远端过滤器首层为 + 或 # 的通配过滤器集合 */
    private final Set<String> remoteWildcardFilters = ConcurrentHashMap.newKeySet();
    /** 客户端ID → 远端订阅索引条目，用于快照变更时做增量差集 */
    private final Map<String, RemoteSubscription> remoteClientSubscriptions = new ConcurrentHashMap<>();

    /** 概览统计缓存锁与结果，避免控制台高频轮询反复遍历全部会话与订阅。 */
    private final Object statisticsLock = new Object();
    private volatile Statistics statisticsCache;
    private volatile long statisticsCacheAtMillis;

    /** 远端会话订阅索引条目：属主节点与订阅过滤器集合。 */
    private record RemoteSubscription(String nodeId, Set<String> filters) {
    }

    /** 远端会话删除墓碑：版本号与记录时间，超过保留期后清理。 */
    private record RemovedSnapshot(long version, long removedAt) {
    }

    /** 会话删除墓碑保留时长，默认 7 天，可由集群配置覆盖 */
    private final long removedSnapshotTtlMillis;

    /** 概览统计结果：在线/离线会话数与订阅总数（含远端快照）。 */
    public record Statistics(int activeSessions, int offlineSessions, int totalSubscriptions) {
    }

    public MqttSessionRegistry(MqttProperties properties,
                               MqttAcl acl,
                               MqttRetainedStore retainedStore) {
        this.properties = properties;
        this.acl = acl;
        this.retainedStore = retainedStore;
        long ttlHours = properties.getCluster().getTombstoneTtlHours();
        this.removedSnapshotTtlMillis = Math.max(1L, ttlHours) * 60 * 60 * 1000L;
        for (int i = 0; i < registrationLocks.length; i++) {
            registrationLocks[i] = new Object();
        }
    }

    /***
    * 按 clientId 选择接入分片锁，保证同一客户端的注册/移除串行执行。
    * @author wcdk
    * @param clientId 客户端 ID
    * @return 分片锁
    ***/
    private Object registrationLock(String clientId) {
        int index = (clientId.hashCode() & 0x7fffffff) % REGISTRATION_LOCK_STRIPES;
        return registrationLocks[index];
    }

    /***
    * 注册消息发布监听器，用于消息日志、集群广播与控制台事件。
    * @author wcdk
    * @param listener 监听器
    ***/
    public void addPublishListener(MqttPublishListener listener) {
        if (listener != null) {
            synchronized (publishListeners) {
                publishListeners.add(listener);
            }
        }
    }

    /***
    * 注册会话生命周期监听器，用于集群同步与控制台事件。
    * @author wcdk
    * @param listener 监听器
    ***/
    public void addSessionListener(MqttSessionListener listener) {
        if (listener != null) {
            synchronized (sessionListeners) {
                sessionListeners.add(listener);
            }
        }
    }

    public MqttRetainedStore retainedStore() {
        return retainedStore;
    }

    public MqttAcl acl() {
        return acl;
    }

    /***
    * 注册本地会话，处理同 clientId 重连时的状态恢复与旧连接接管。
    * @author wcdk
    * @param session 新会话
    * @return 会话状态恢复成功返回 true
    ***/
    public boolean register(MqttSession session) {
        synchronized (registrationLock(session.clientId())) {
            boolean sessionPresent = false;
            MqttSession previous = localSessions.get(session.clientId());
            if (previous != null && previous != session) {
                // 同一客户端重复接入，直接断开旧连接（接管属异常断开，旧连接会按规范发布遗嘱）
                previous.closeConnection();
                if (!session.cleanSession()) {
                    session.restoreStateFrom(previous);
                    sessionPresent = true;
                }
            } else if (!session.cleanSession()) {
                // 本节点没有旧会话时，尝试从集群同步来的快照恢复
                MqttSession.SessionSnapshot snapshot = remoteSnapshots.get(session.clientId());
                if (snapshot != null && hasSessionState(snapshot)) {
                    session.restoreSnapshot(snapshot);
                    sessionPresent = true;
                }
            }
            localSessions.put(session.clientId(), session);
            // 本地已接管该客户端，清理远端快照并同步移除远端订阅索引
            remoteSnapshots.remove(session.clientId());
            removedSnapshots.remove(session.clientId());
            removeRemoteSnapshotIndex(session.clientId());
            session.setMaxOfflineCapacity(properties.getBroker().getMaxOfflineMessages());
            // 消息已知但会话尚未接入时缓冲的离线消息在这里一次性冲刷
            drainPendingOffline(session);
            notifySessionChanged(session);
            return sessionPresent;
        }
    }

    private boolean hasSessionState(MqttSession.SessionSnapshot snapshot) {
        return snapshot.isOnline()
                || (snapshot.getSubscriptions() != null && !snapshot.getSubscriptions().isEmpty())
                || (snapshot.getOutboundPending() != null && !snapshot.getOutboundPending().isEmpty());
    }

    private void drainPendingOffline(MqttSession session) {
        Queue<MqttBrokerMessage> buffered = pendingOffline.remove(session.clientId());
        if (buffered == null) {
            return;
        }
        MqttBrokerMessage message;
        while ((message = buffered.poll()) != null) {
            int subscriptionQos = session.maxSubscriptionQos(message.topic());
            if (subscriptionQos < 0) {
                continue;
            }
            session.enqueueOffline(message, session.maxOfflineCapacity());
        }
        log.debug("已冲刷离线缓冲消息，客户端ID：{}，缓冲数量：{}", session.clientId(), buffered.size());
    }

    /***
    * 释放本地会话持有的连接状态，持久会话保留为离线状态，清理会话从注册表移除。
    * @author wcdk
    * @param session 要释放的会话
    * @return 被释放的会话，会话已被其他连接接管时返回 null
    ***/
    public MqttSession remove(MqttSession session) {
        if (session == null) {
            return null;
        }
        MqttSession current = localSessions.get(session.clientId());
        if (current != session) {
            return null;
        }
        session.detach();
        if (session.persistsAfterDisconnect()) {
            // 持久会话保留注册信息，继续为它缓冲离线消息，到过期时间由巡检移除
            notifySessionChanged(session);
        } else {
            localSessions.remove(session.clientId(), session);
            pendingOffline.remove(session.clientId());
            removeRemoteSnapshot(session.clientId());
            notifySessionRemoved(session);
        }
        return session;
    }

    /***
    * 扫描并移除超过 MQTT5 会话过期时间的离线会话，同步清理其离线缓冲与远端快照。
    * @author wcdk
    * @return 本次过期移除的会话
    ***/
    public List<MqttSession> sweepExpiredSessions() {
        long now = System.currentTimeMillis();
        List<MqttSession> expired = new ArrayList<>();
        for (MqttSession session : localSessions.values()) {
            if (!session.isSessionExpired(now)) {
                continue;
            }
            MqttSession current = localSessions.get(session.clientId());
            if (current != session) {
                continue;
            }
            localSessions.remove(session.clientId(), session);
            pendingOffline.remove(session.clientId());
            removeRemoteSnapshot(session.clientId());
            session.detach();
            notifySessionRemoved(session);
            expired.add(session);
        }
        return expired;
    }

    /***
    * 重建指定会话的路由索引：与既有索引做增量差集，保留未变更的过滤器，
    * 避免"先清空再重建"的窗口内消息漏投递。
    * @author wcdk
    * @param session 会话
    ***/
    private void reindexSession(MqttSession session) {
        if (session == null) {
            return;
        }
        String clientId = session.clientId();
        Set<String> currentFilters = new HashSet<>(session.subscriptionFilterNames());
        synchronized (routingIndexLock) {
            Set<String> indexedFilters = clientFilterIndex.get(clientId);
            if (indexedFilters != null) {
                for (String filter : indexedFilters) {
                    if (!currentFilters.contains(filter)) {
                        removeSubscriber(clientId, filter);
                    }
                }
            }
            for (String filter : currentFilters) {
                if (indexedFilters == null || !indexedFilters.contains(filter)) {
                    addSubscriber(clientId, filter);
                }
            }
            if (currentFilters.isEmpty()) {
                clientFilterIndex.remove(clientId);
            } else {
                clientFilterIndex.put(clientId, currentFilters);
            }
        }
    }

    /***
    * 从路由索引移除指定客户端的全部订阅条目，会话销毁时调用。
    * @author wcdk
    * @param clientId 客户端 ID
    ***/
    private void removeFromRoutingIndex(String clientId) {
        if (clientId == null) {
            return;
        }
        synchronized (routingIndexLock) {
            Set<String> indexedFilters = clientFilterIndex.remove(clientId);
            if (indexedFilters == null) {
                return;
            }
            for (String filter : indexedFilters) {
                Set<String> clients = filterSubscribers.get(filter);
                if (clients == null) {
                    continue;
                }
                clients.remove(clientId);
                if (clients.isEmpty() && filterSubscribers.remove(filter, clients)) {
                    removeFromFirstLevelIndex(filter);
                }
            }
        }
    }

    /** 登记订阅关系，调用方必须持有 routingIndexLock。 */
    private void addSubscriber(String clientId, String filter) {
        filterSubscribers.computeIfAbsent(filter, key -> ConcurrentHashMap.newKeySet()).add(clientId);
        String firstLevel = MqttTopicMatcher.firstLevel(filter);
        if ("+".equals(firstLevel) || "#".equals(firstLevel)) {
            wildcardFirstLevelFilters.add(filter);
        } else {
            literalFilterIndex.computeIfAbsent(firstLevel, key -> ConcurrentHashMap.newKeySet()).add(filter);
        }
    }

    /** 移除单条订阅关系，最后一个订阅者离开时同步清理首层索引，调用方必须持有 routingIndexLock。 */
    private void removeSubscriber(String clientId, String filter) {
        Set<String> clients = filterSubscribers.get(filter);
        if (clients == null) {
            return;
        }
        clients.remove(clientId);
        if (clients.isEmpty() && filterSubscribers.remove(filter, clients)) {
            removeFromFirstLevelIndex(filter);
        }
    }

    private void removeFromFirstLevelIndex(String filter) {
        String firstLevel = MqttTopicMatcher.firstLevel(filter);
        if ("+".equals(firstLevel) || "#".equals(firstLevel)) {
            wildcardFirstLevelFilters.remove(filter);
            return;
        }
        Set<String> filters = literalFilterIndex.get(firstLevel);
        if (filters != null) {
            filters.remove(filter);
            if (filters.isEmpty()) {
                literalFilterIndex.remove(firstLevel, filters);
            }
        }
    }

    /***
    * 增量更新远端订阅索引：以客户端为单位做过滤器差集，避免全量重建。
    * 调用方无需持锁，方法内部使用路由索引锁串行化。
    * @author wcdk
    * @param snapshot 远端会话快照
    ***/
    private void indexRemoteSnapshot(MqttSession.SessionSnapshot snapshot) {
        if (snapshot == null || snapshot.getClientId() == null) {
            return;
        }
        String clientId = snapshot.getClientId();
        String nodeId = snapshot.getOwnerNodeId();
        Set<String> filters = new HashSet<>();
        if (snapshot.getSubscriptions() != null) {
            filters.addAll(snapshot.getSubscriptions().keySet());
        }
        synchronized (routingIndexLock) {
            RemoteSubscription previous = remoteClientSubscriptions.remove(clientId);
            if (previous != null) {
                removeRemoteFilters(previous);
            }
            if (nodeId == null || nodeId.isBlank() || filters.isEmpty()) {
                return;
            }
            remoteClientSubscriptions.put(clientId, new RemoteSubscription(nodeId, filters));
            for (String filter : filters) {
                addRemoteFilter(nodeId, filter);
            }
        }
    }

    /***
    * 移除指定客户端的远端订阅索引条目。
    * @author wcdk
    * @param clientId 客户端 ID
    ***/
    private void removeRemoteSnapshotIndex(String clientId) {
        if (clientId == null) {
            return;
        }
        synchronized (routingIndexLock) {
            RemoteSubscription previous = remoteClientSubscriptions.remove(clientId);
            if (previous != null) {
                removeRemoteFilters(previous);
            }
        }
    }

    private void addRemoteFilter(String nodeId, String filter) {
        Map<String, Integer> counts = remoteFilterNodeCounts.computeIfAbsent(filter,
                key -> new ConcurrentHashMap<>());
        counts.merge(nodeId, 1, Integer::sum);
        if (counts.size() == 1) {
            String firstLevel = MqttTopicMatcher.firstLevel(filter);
            if ("+".equals(firstLevel) || "#".equals(firstLevel)) {
                remoteWildcardFilters.add(filter);
            } else {
                remoteLiteralFilters.computeIfAbsent(firstLevel, key -> ConcurrentHashMap.newKeySet()).add(filter);
            }
        }
    }

    private void removeRemoteFilters(RemoteSubscription entry) {
        for (String filter : entry.filters()) {
            Map<String, Integer> counts = remoteFilterNodeCounts.get(filter);
            if (counts == null) {
                continue;
            }
            counts.computeIfPresent(entry.nodeId(), (key, value) -> value <= 1 ? null : value - 1);
            if (counts.isEmpty() && remoteFilterNodeCounts.remove(filter, counts)) {
                removeRemoteFromFirstLevelIndex(filter);
            }
        }
    }

    private void removeRemoteFromFirstLevelIndex(String filter) {
        String firstLevel = MqttTopicMatcher.firstLevel(filter);
        if ("+".equals(firstLevel) || "#".equals(firstLevel)) {
            remoteWildcardFilters.remove(filter);
            return;
        }
        Set<String> filters = remoteLiteralFilters.get(firstLevel);
        if (filters != null) {
            filters.remove(filter);
            if (filters.isEmpty()) {
                remoteLiteralFilters.remove(firstLevel, filters);
            }
        }
    }

    /***
    * 查询主题命中远端订阅的属主节点集合，供发布时定向转发。
    * 无远端订阅者时返回空集合，从而避免集群全量广播。
    * @author wcdk
    * @param topic 主题名称
    * @return 需要接收该消息的节点 ID 集合
    ***/
    public Set<String> remoteTargetNodes(String topic) {
        if (topic == null || topic.isEmpty()) {
            return Set.of();
        }
        Set<String> candidateFilters = new LinkedHashSet<>();
        Set<String> literal = remoteLiteralFilters.get(MqttTopicMatcher.firstLevel(topic));
        if (literal != null) {
            candidateFilters.addAll(literal);
        }
        // $ 开头的系统主题不能被首层通配（+ / #）过滤器命中
        if (topic.charAt(0) != '$' && !remoteWildcardFilters.isEmpty()) {
            candidateFilters.addAll(remoteWildcardFilters);
        }
        if (candidateFilters.isEmpty()) {
            return Set.of();
        }
        Set<String> nodes = new LinkedHashSet<>();
        for (String filter : candidateFilters) {
            if (!MqttTopicMatcher.matches(filter, topic)) {
                continue;
            }
            Map<String, Integer> counts = remoteFilterNodeCounts.get(filter);
            if (counts != null && !counts.isEmpty()) {
                nodes.addAll(counts.keySet());
            }
        }
        return nodes;
    }

    /***
    * 按路由索引收集主题命中的过滤器：字面量首层哈希命中 + 少量通配首层候选，再做完整匹配校验。
    * 相比对全部会话的全部订阅做 O(N×M) 扫描，仅遍历候选过滤器。
    * @author wcdk
    * @param topic 主题名称
    * @return 完整匹配通过的过滤器集合
    ***/
    private Set<String> matchedCandidateFilters(String topic) {
        Set<String> candidates = new LinkedHashSet<>();
        Set<String> literal = literalFilterIndex.get(MqttTopicMatcher.firstLevel(topic));
        if (literal != null) {
            candidates.addAll(literal);
        }
        // $ 开头的系统主题不能被首层通配（+ / #）过滤器命中
        if (topic.charAt(0) != '$' && !wildcardFirstLevelFilters.isEmpty()) {
            candidates.addAll(wildcardFirstLevelFilters);
        }
        candidates.removeIf(filter -> !MqttTopicMatcher.matches(filter, topic));
        return candidates;
    }

    /***
    * 将消息路由到本节点所有命中的订阅者，并为离线持久会话缓冲。
    * @author wcdk
    * @param message 消息
    * @return 本次投递命中的订阅过滤器（去重）
    ***/
    public List<String> deliver(MqttBrokerMessage message) {
        return deliver(message, null);
    }

    /***
    * 将消息路由到本节点所有命中的订阅者，并为离线持久会话缓冲。
    * 保留消息若携带集群版本，则按版本合并而不是覆盖本地当前值。
    * @author wcdk
    * @param message 消息
    * @param retainedVersion 保留消息版本，null 表示本地写入
    * @return 本次投递命中的订阅过滤器（去重）
    ***/
    public List<String> deliver(MqttBrokerMessage message, Long retainedVersion) {
        if (message == null || !MqttTopicMatcher.isValidTopicName(message.topic())) {
            return List.of();
        }
        if (properties.getBroker().isRetainedMessages()) {
            if (retainedVersion == null) {
                retainedStore.store(message);
            } else {
                retainedStore.store(message, retainedVersion);
            }
        }
        // 1. 索引定位命中的过滤器，再反查候选订阅者，避免遍历全部会话
        Set<String> matchedFilters = matchedCandidateFilters(message.topic());
        Set<String> candidateClients = new LinkedHashSet<>();
        for (String filter : matchedFilters) {
            Set<String> subscribers = filterSubscribers.get(filter);
            if (subscribers != null) {
                candidateClients.addAll(subscribers);
            }
        }
        // 2. 按会话聚合最高订阅 QoS，每个会话只投递一次
        Set<String> resultFilters = new LinkedHashSet<>();
        for (String clientId : candidateClients) {
            MqttSession session = localSessions.get(clientId);
            if (session == null) {
                continue;
            }
            int subscriptionQos = -1;
            for (String filter : matchedFilters) {
                Integer qos = session.subscriptionQos(filter);
                if (qos != null && qos > subscriptionQos) {
                    subscriptionQos = qos;
                }
            }
            if (subscriptionQos < 0) {
                // 索引残留（订阅已在未触发重建的路径上变更），以会话实际订阅为准
                continue;
            }
            if (!acl.canSubscribe(session.username(), session.clientId(), message.topic())) {
                continue;
            }
            for (String filter : matchedFilters) {
                if (session.subscriptionQos(filter) != null) {
                    resultFilters.add(filter);
                }
            }
            int deliveryQos = MqttTopicMatcher.minQos(message.qos(), subscriptionQos);
            session.sendPublish(message, deliveryQos, false);
        }
        // 为集群中已知的离线持久会话缓冲，等待其接入本节点或由属主节点投递
        if (properties.getBroker().isOfflineQueue() && message.qos() > 0) {
            bufferForRemoteSessions(message);
        }
        List<String> result = List.copyOf(resultFilters);
        for (MqttPublishListener listener : publishListenerSnapshot()) {
            listener.onPublish(message, result);
        }
        return result;
    }

    private List<MqttPublishListener> publishListenerSnapshot() {
        synchronized (publishListeners) {
            return List.copyOf(publishListeners);
        }
    }

    private List<MqttSessionListener> sessionListenerSnapshot() {
        synchronized (sessionListeners) {
            return List.copyOf(sessionListeners);
        }
    }

    private void bufferForRemoteSessions(MqttBrokerMessage message) {
        for (Map.Entry<String, MqttSession.SessionSnapshot> entry : remoteSnapshots.entrySet()) {
            MqttSession.SessionSnapshot snapshot = entry.getValue();
            if (snapshot == null || !snapshot.persistsAfterDisconnect() || snapshot.isOnline()) {
                continue;
            }
            if (localSessions.containsKey(entry.getKey())) {
                continue;
            }
            if (!matchesAnySubscription(snapshot.getSubscriptions(), message.topic())) {
                continue;
            }
            Queue<MqttBrokerMessage> queue = pendingOffline.computeIfAbsent(entry.getKey(),
                    key -> new ConcurrentLinkedQueue<>());
            queue.add(message);
            int maxCapacity = Math.max(1, properties.getBroker().getMaxOfflineMessages());
            int overflow = queue.size() - maxCapacity;
            for (int i = 0; i < overflow; i++) {
                queue.poll();
            }
        }
    }

    private boolean matchesAnySubscription(Map<String, Integer> subscriptions, String topic) {
        if (subscriptions == null || subscriptions.isEmpty()) {
            return false;
        }
        for (String filter : subscriptions.keySet()) {
            if (!MqttTopicMatcher.maybeMatches(filter, topic)) {
                continue;
            }
            if (MqttTopicMatcher.matches(filter, topic)) {
                return true;
            }
        }
        return false;
    }

    /***
    * 向新订阅者补发匹配的保留消息。
    * @author wcdk
    * @param session 订阅者会话
    * @param topicFilter 主题过滤器
    * @param subscriptionQos 订阅 QoS
    ***/
    public void sendRetained(MqttSession session, String topicFilter, int subscriptionQos) {
        if (!properties.getBroker().isRetainedMessages() || session == null) {
            return;
        }
        for (MqttBrokerMessage retained : retainedStore.findByFilter(topicFilter)) {
            if (!acl.canSubscribe(session.username(), session.clientId(), retained.topic())) {
                continue;
            }
            int deliveryQos = MqttTopicMatcher.minQos(retained.qos(), subscriptionQos);
            session.sendPublish(retained, deliveryQos, true);
        }
    }

    /***
    * 扫描 Keep Alive 超时的会话。
    * @author wcdk
    * @return 超时会话列表
    ***/
    public List<MqttSession> sweepIdleSessions() {
        List<MqttSession> expired = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (MqttSession session : localSessions.values()) {
            int keepAlive = session.keepAliveSeconds();
            if (keepAlive <= 0 || !session.isActive()) {
                continue;
            }
            // 规范要求 1.5 倍 Keep Alive 未收到报文即断开
            long timeoutMillis = keepAlive * 1500L;
            if (now - session.lastActivityTime() > timeoutMillis) {
                expired.add(session);
            }
        }
        return expired;
    }

    /***
    * 扫描出站超时未确认的消息用于重投。
    * @author wcdk
    * @param timeoutMillis 确认超时毫秒
    * @return 需要重投的会话列表
    ***/
    public List<MqttSession> sweepPendingOutbound(long timeoutMillis) {
        List<MqttSession> result = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (MqttSession session : localSessions.values()) {
            if (!session.isActive()) {
                continue;
            }
            for (MqttSession.PendingOutbound pending : session.outboundPendingSnapshot()) {
                if (now - pending.createdAt() > timeoutMillis) {
                    result.add(session);
                    break;
                }
            }
        }
        return result;
    }

    public Map<String, MqttSession> localSessions() {
        return localSessions;
    }

    public List<MqttSession> sessions() {
        return List.copyOf(localSessions.values());
    }

    public MqttSession findByClientId(String clientId) {
        return clientId == null ? null : localSessions.get(clientId);
    }

    public int activeSessionCount() {
        int count = 0;
        for (MqttSession session : localSessions.values()) {
            if (session.isActive()) {
                count++;
            }
        }
        return count;
    }

    /***
    * 返回按配置窗口缓存的概览统计，控制台高频轮询时最多每窗口遍历一次全部会话。
    * 统计包含远端会话快照，且使用 {@code subscriptionCount()} 避免复制订阅表。
    * @author wcdk
    * @return 概览统计
    ***/
    public Statistics statistics() {
        long cacheMillis = properties.getConsole().getOverviewCacheMillis();
        if (cacheMillis > 0) {
            Statistics cached = statisticsCache;
            if (cached != null && System.currentTimeMillis() - statisticsCacheAtMillis < cacheMillis) {
                return cached;
            }
        }
        synchronized (statisticsLock) {
            if (cacheMillis > 0) {
                Statistics cached = statisticsCache;
                if (cached != null && System.currentTimeMillis() - statisticsCacheAtMillis < cacheMillis) {
                    return cached;
                }
            }
            Statistics computed = computeStatistics();
            statisticsCache = computed;
            statisticsCacheAtMillis = System.currentTimeMillis();
            return computed;
        }
    }

    private Statistics computeStatistics() {
        int activeSessions = 0;
        int offlineSessions = 0;
        int totalSubscriptions = 0;
        for (MqttSession session : localSessions.values()) {
            if (session.isActive()) {
                activeSessions++;
            } else {
                offlineSessions++;
            }
            totalSubscriptions += session.subscriptionCount();
        }
        // 远端会话快照（集群同步来的），计入未被本节点占据的会话
        for (Map.Entry<String, MqttSession.SessionSnapshot> entry : remoteSnapshots.entrySet()) {
            MqttSession.SessionSnapshot snapshot = entry.getValue();
            if (snapshot == null || snapshot.getClientId() == null) {
                continue;
            }
            // 若本节点已有该 clientId 的本地会话，远端快照不重复计入
            if (localSessions.containsKey(entry.getKey())) {
                continue;
            }
            if (snapshot.isOnline()) {
                activeSessions++;
            } else {
                offlineSessions++;
            }
            totalSubscriptions += snapshot.subscriptionCount();
        }
        return new Statistics(activeSessions, offlineSessions, totalSubscriptions);
    }

    public int sessionCount() {
        return localSessions.size();
    }

    /***
    * 更新集群同步来的远端会话快照。
    * @author wcdk
    * @param snapshot 会话快照
    ***/
    public void putRemoteSnapshot(MqttSession.SessionSnapshot snapshot) {
        if (snapshot == null || snapshot.getClientId() == null) {
            return;
        }
        if (localSessions.containsKey(snapshot.getClientId())) {
            return;
        }
        // 删除墓碑优先：版本不高于墓碑的迟到快照直接丢弃
        RemovedSnapshot removed = removedSnapshots.get(snapshot.getClientId());
        if (removed != null) {
            if (snapshot.getStateVersion() > 0 && snapshot.getStateVersion() <= removed.version()) {
                log.debug("忽略版本过旧的远端会话快照，客户端ID：{}，快照版本：{}，删除版本：{}",
                        snapshot.getClientId(), snapshot.getStateVersion(), removed.version());
                return;
            }
            removedSnapshots.remove(snapshot.getClientId());
        }
        MqttSession.SessionSnapshot existing = remoteSnapshots.get(snapshot.getClientId());
        if (existing != null && !isSnapshotNewer(snapshot, existing)) {
            log.debug("忽略版本过旧的远端会话快照，客户端ID：{}，快照版本：{}，当前版本：{}",
                    snapshot.getClientId(), snapshot.getStateVersion(), existing.getStateVersion());
            return;
        }
        remoteSnapshots.put(snapshot.getClientId(), snapshot);
        // 同步维护远端订阅索引，供发布时按主题定向转发
        indexRemoteSnapshot(snapshot);
    }

    /***
    * 判断候选快照是否比现有快照更新：先比版本，版本相同再比属主节点 ID 字典序。
    * 无版本的旧节点快照仅在现有快照同样无版本时按到达顺序接受。
    * @author wcdk
    * @param candidate 候选快照
    * @param existing 现有快照
    * @return 候选更新返回 true
    ***/
    private static boolean isSnapshotNewer(MqttSession.SessionSnapshot candidate,
                                           MqttSession.SessionSnapshot existing) {
        long candidateVersion = candidate.getStateVersion();
        long existingVersion = existing.getStateVersion();
        if (candidateVersion <= 0) {
            return existingVersion <= 0;
        }
        int byVersion = Long.compare(candidateVersion, existingVersion);
        if (byVersion != 0) {
            return byVersion > 0;
        }
        return nullSafe(candidate.getOwnerNodeId()).compareTo(nullSafe(existing.getOwnerNodeId())) >= 0;
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    /***
    * 移除远端会话快照及其离线缓冲。
    * @author wcdk
    * @param clientId 客户端 ID
    ***/
    public void removeRemoteSnapshot(String clientId) {
        if (clientId == null) {
            return;
        }
        remoteSnapshots.remove(clientId);
        pendingOffline.remove(clientId);
        removeRemoteSnapshotIndex(clientId);
    }

    /***
    * 按删除事件携带的版本移除远端会话快照，丢弃迟到的旧删除指令。
    * @author wcdk
    * @param clientId 客户端 ID
    * @param version 删除事件版本，非正数表示旧节点无版本
    * @param ownerNodeId 删除发起节点 ID
    ***/
    public void removeRemoteSnapshot(String clientId, long version, String ownerNodeId) {
        if (clientId == null) {
            return;
        }
        MqttSession.SessionSnapshot existing = remoteSnapshots.get(clientId);
        if (existing != null && version > 0) {
            if (existing.getStateVersion() > version) {
                log.debug("忽略版本过旧的会话删除指令，客户端ID：{}，删除版本：{}，快照版本：{}",
                        clientId, version, existing.getStateVersion());
                return;
            }
            // 版本相同时与快照合并规则保持一致，按属主节点 ID 字典序裁决
            if (existing.getStateVersion() == version && existing.getOwnerNodeId() != null && ownerNodeId != null
                    && existing.getOwnerNodeId().compareTo(ownerNodeId) > 0) {
                log.debug("忽略版本相同的会话删除指令，客户端ID：{}，删除节点：{}，快照属主：{}",
                        clientId, ownerNodeId, existing.getOwnerNodeId());
                return;
            }
        }
        if (version > 0) {
            removedSnapshots.merge(clientId, new RemovedSnapshot(version, System.currentTimeMillis()),
                    (left, right) -> right.version() >= left.version() ? right : left);
        }
        removeRemoteSnapshot(clientId);
    }

    /***
    * 清理过期的远端会话删除墓碑，避免长期运行后墓碑无限增长。
    * @author wcdk
    * @param now 当前时间毫秒
    ***/
    public void purgeRemovedSnapshots(long now) {
        removedSnapshots.entrySet().removeIf(entry -> now - entry.getValue().removedAt() > removedSnapshotTtlMillis);
    }

    /***
    * 管理接口强制移除本地会话（含持久会话），调用前应先断开连接。
    * @author wcdk
    * @param clientId 客户端 ID
    * @return 本地存在该会话返回 true
    ***/
    public boolean forceRemove(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return false;
        }
        String id = clientId.trim();
        synchronized (registrationLock(id)) {
            MqttSession session = localSessions.remove(id);
            if (session == null) {
                return false;
            }
            session.detach();
            pendingOffline.remove(id);
            notifySessionRemoved(session);
            return true;
        }
    }

    /***
    * 释放被其他节点接管的本地会话副本：移除注册表条目与离线缓冲，
    * 使本节点不再为该客户端缓存消息，避免客户端再次接入时重复投递。
    * @author wcdk
    * @param clientId 客户端 ID
    * @return 本地存在该会话返回 true
    ***/
    public boolean releaseForClaim(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return false;
        }
        String id = clientId.trim();
        synchronized (registrationLock(id)) {
            MqttSession session = localSessions.remove(id);
            pendingOffline.remove(id);
            // 不触发 onSessionRemoved 广播（会话是迁移而非销毁），但必须清理本地路由索引
            removeFromRoutingIndex(id);
            if (session == null) {
                return false;
            }
            session.detach();
            // 不触发 onSessionRemoved 广播：会话并未销毁而是迁移，广播 SESSION_REMOVE
            // 会误删其他节点刚同步来的远端快照
            return true;
        }
    }

    /***
    * 清空指定客户端的离线缓冲，会话在其他节点上线时调用。
    * @author wcdk
    * @param clientId 客户端 ID
    ***/
    public void clearPendingOffline(String clientId) {
        if (clientId != null) {
            pendingOffline.remove(clientId);
        }
    }

    public Map<String, MqttSession.SessionSnapshot> remoteSnapshots() {
        return remoteSnapshots;
    }

    public int remoteSnapshotCount() {
        return remoteSnapshots.size();
    }

    public int pendingOfflineCount() {
        return pendingOffline.size();
    }

    /***
    * 通知全部会话监听器（订阅等状态变化时由协议层触发集群同步）。
    * @author wcdk
    * @param session 会话
    ***/
    public void notifySessionChanged(MqttSession session) {
        if (session != null) {
            // 状态变更先递增版本，保证对端按版本裁决时能识别本次变更
            session.bumpStateVersion();
            // 订阅等状态变化先重建路由索引，再广播给集群（索引重建不依赖监听器存在）
            reindexSession(session);
        }
        for (MqttSessionListener listener : sessionListenerSnapshot()) {
            try {
                listener.onSessionChanged(session);
            } catch (RuntimeException ex) {
                log.warn("会话变更监听器执行失败，客户端ID：{}，异常信息：{}",
                        session.clientId(), ex.getMessage());
            }
        }
    }

    private void notifySessionRemoved(MqttSession session) {
        if (session != null) {
            // 删除事件同样携带递增版本，避免对端迟到快照覆盖删除结果
            session.bumpStateVersion();
            removeFromRoutingIndex(session.clientId());
        }
        for (MqttSessionListener listener : sessionListenerSnapshot()) {
            try {
                listener.onSessionRemoved(session);
            } catch (RuntimeException ex) {
                log.warn("会话移除监听器执行失败，客户端ID：{}，异常信息：{}",
                        session.clientId(), ex.getMessage());
            }
        }
    }
}
