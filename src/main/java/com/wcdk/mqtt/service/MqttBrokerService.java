package com.wcdk.mqtt.service;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.wcdk.mqtt.cluster.MqttClusterManager;
import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.core.MqttBrokerMessage;
import com.wcdk.mqtt.core.MqttMessageLog;
import com.wcdk.mqtt.core.MqttSession;
import com.wcdk.mqtt.core.MqttSessionRegistry;
import com.wcdk.mqtt.core.MqttTopicMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

/***
* MQTT 发布与统计服务，统一本地投递与集群广播入口。
* @author wcdk
***/
@Service
public class MqttBrokerService {

    private static final Logger log = LoggerFactory.getLogger(MqttBrokerService.class);

    private final MqttProperties properties;
    private final MqttSessionRegistry registry;
    private final MqttMessageLog messageLog;
    private final ObjectProvider<MqttClusterManager> clusterManagerProvider;
    private final Instant startedAt = Instant.now();

    public MqttBrokerService(MqttProperties properties,
                             MqttSessionRegistry registry,
                             MqttMessageLog messageLog,
                             ObjectProvider<MqttClusterManager> clusterManagerProvider) {
        this.properties = properties;
        this.registry = registry;
        this.messageLog = messageLog;
        this.clusterManagerProvider = clusterManagerProvider;
    }

    /***
    * 发布消息到本节点并同步广播到集群对等节点。
    * 未达成多数派时拒绝写入，作为协议层闸门的最后一道防线。
    * @author wcdk
    * @param message 消息
    * @return 已接受写入返回 true
    ***/
    public boolean publish(MqttBrokerMessage message) {
        if (message == null || !MqttTopicMatcher.isValidTopicName(message.topic())) {
            log.warn("拒绝发布非法主题消息，主题：{}", message == null ? null : message.topic());
            return false;
        }
        if (!hasQuorum()) {
            MqttClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
            log.warn("集群未达成多数派，拒绝写入消息，主题：{}，QoS：{}，保留：{}，可达节点：{}，多数派阈值：{}",
                    message.topic(), message.qos(), message.retained(),
                    clusterManager == null ? 1 : clusterManager.reachableNodeCount(),
                    clusterManager == null ? 1 : clusterManager.quorumSize());
            return false;
        }
        // 本地投递（含保留消息与离线缓冲）
        registry.deliver(message);
        // 集群广播，对等节点收到后仅做本地投递，不再回环广播
        MqttClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        if (clusterManager != null && clusterManager.isRunning()) {
            clusterManager.broadcastPublish(message);
        }
        return true;
    }

    /***
    * 当前节点是否具备写能力（达成多数派或未启用集群）。
    * @author wcdk
    * @return 可写返回 true
    ***/
    public boolean hasQuorum() {
        MqttClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        if (clusterManager == null || !clusterManager.isRunning()) {
            return true;
        }
        return clusterManager.hasQuorum();
    }

    /***
    * 向指定客户端投递消息，本节点不存在该客户端时经集群转发。
    * @author wcdk
    * @param clientId 目标客户端 ID
    * @param message 消息
    * @return 投递成功返回 true
    ***/
    public boolean publishToClient(String clientId, MqttBrokerMessage message) {
        if (clientId == null || clientId.isBlank() || message == null) {
            return false;
        }
        MqttSession session = registry.findByClientId(clientId.trim());
        if (session != null && session.isActive()) {
            int subscriptionQos = session.maxSubscriptionQos(message.topic());
            int deliveryQos = subscriptionQos < 0
                    ? message.qos()
                    : MqttTopicMatcher.minQos(message.qos(), subscriptionQos);
            session.sendPublish(message, deliveryQos, false);
            return true;
        }
        MqttClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        if (clusterManager != null && clusterManager.isRunning()) {
            return clusterManager.forwardToClient(clientId.trim(), message);
        }
        return false;
    }

    /***
    * 生成 Broker 运行概览统计。
    * @author wcdk
    * @return 概览视图
    ***/
    public Overview overview() {
        // 概览统计按配置窗口缓存，避免高频轮询时反复遍历全部会话与订阅
        MqttSessionRegistry.Statistics statistics = registry.statistics();
        Overview overview = new Overview();
        overview.setStartedAt(startedAt);
        overview.setUptimeSeconds(java.time.Duration.between(startedAt, Instant.now()).toSeconds());
        overview.setMqttEnabled(properties.getBroker().isEnabled());
        overview.setMqttHost(properties.getBroker().getHost());
        overview.setMqttPort(properties.getBroker().getPort());
        overview.setTotalSessions(registry.sessionCount());
        overview.setActiveSessions(statistics.activeSessions());
        overview.setOfflineSessions(statistics.offlineSessions());
        overview.setRemoteSessions(registry.remoteSnapshotCount());
        overview.setTotalSubscriptions(statistics.totalSubscriptions());
        overview.setRetainedMessages(registry.retainedStore().size());
        overview.setBufferedOfflineClients(registry.pendingOfflineCount());
        overview.setMessageLogSize(messageLog.size());
        overview.setMessageLogCapacity(messageLog.capacity());
        MqttClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        overview.setClusterEnabled(clusterManager != null && clusterManager.isRunning());
        overview.setNodeId(clusterManager != null ? clusterManager.nodeId() : null);
        overview.setClusterPeerCount(clusterManager == null ? 0 : clusterManager.connectedPeerCount());
        return overview;
    }

    /***
    * 查询客户端会话列表。
    * @author wcdk
    * @return 会话列表
    ***/
    public List<MqttSession> sessions() {
        return registry.sessions();
    }

    /***
    * 查询最近消息。
    * @author wcdk
    * @param topicFilter 主题过滤器
    * @param qos QoS 过滤
    * @param from 起始时间
    * @param to 结束时间
    * @param limit 条数上限
    * @return 消息列表
    ***/
    public List<MqttMessageLog.LoggedMessage> messages(String topicFilter, Integer qos,
                                                       Instant from, Instant to, int limit) {
        return messageLog.query(topicFilter, qos, from, to, limit);
    }

    /***
    * 清理消息日志。
    * @author wcdk
    * @param topicFilter 主题过滤器
    * @param qos QoS 过滤
    * @param from 起始时间
    * @param to 结束时间
    * @return 删除条数
    ***/
    public int clearMessages(String topicFilter, Integer qos, Instant from, Instant to) {
        return messageLog.removeByCondition(topicFilter, qos, from, to);
    }

    /***
    * 按 ID 批量删除消息日志。
    * @author wcdk
    * @param ids 消息 ID 列表
    * @return 删除条数
    ***/
    public int clearMessagesByIds(List<String> ids) {
        return messageLog.removeByIds(ids);
    }

    /***
    * 运行概览视图。
    * @author wcdk
    ***/
    @lombok.Data
    public static class Overview {
        private Instant startedAt;
        private long uptimeSeconds;
        private boolean mqttEnabled;
        private String mqttHost;
        private int mqttPort;
        private int totalSessions;
        private int activeSessions;
        private int offlineSessions;
        private int remoteSessions;
        private int totalSubscriptions;
        private int retainedMessages;
        private int bufferedOfflineClients;
        private int messageLogSize;
        private int messageLogCapacity;
        private boolean clusterEnabled;
        private String nodeId;
        private int clusterPeerCount;
    }
}
