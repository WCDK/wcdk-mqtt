package com.wcdk.mqtt.core.cluster;

import com.wcdk.mqtt.core.config.MqttBrokerProperties;
import com.wcdk.mqtt.core.json.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @auther WCDK
 * @date 2026/8/3
 * @version 1.0
 **/
@Service
public class MqttClusterConfigSyncService {

    private static final Logger log = LoggerFactory.getLogger(MqttClusterConfigSyncService.class);

    /** 配置同步消息前缀，用于标识配置同步消息 */
    private static final String CONFIG_SYNC_PREFIX = "CONFIG_SYNC:";
    /** 配置请求消息，从节点向主节点请求配置 */
    private static final String CONFIG_REQUEST = "CONFIG_REQUEST";

    /** Broker配置属性，包含需要同步的配置项 */
    private final MqttBrokerProperties mqttBrokerProperties;

    public MqttClusterConfigSyncService(MqttBrokerProperties mqttBrokerProperties) {
        this.mqttBrokerProperties = mqttBrokerProperties;
    }

    /**
     * 构建配置同步消息
     * 将当前Broker的配置转换为JSON格式，用于主节点向从节点同步配置
     *
     * @return 配置同步消息字符串，包含CONFIG_SYNC:前缀
     */
    public String buildConfigSyncMessage() {
        ClusterConfig config = new ClusterConfig();
        // 同步基本Broker配置
        config.setMaxPayloadSize(mqttBrokerProperties.getMaxPayloadSize());
        config.setMaxClientIdLength(mqttBrokerProperties.getMaxClientIdLength());
        config.setAnonymous(mqttBrokerProperties.isAnonymous());
        config.setUsername(mqttBrokerProperties.getUsername());
        config.setPassword(mqttBrokerProperties.getPassword());
        config.setRetainedMessages(mqttBrokerProperties.isRetainedMessages());
        config.setPersistence(mqttBrokerProperties.isPersistence());
        
        // 同步集群特有配置
        MqttBrokerProperties.Cluster cluster = mqttBrokerProperties.getCluster();
        if (cluster != null) {
            config.setGlobalSession(cluster.isGlobalSession());
            config.setDistributedRetained(cluster.isDistributedRetained());
            config.setOfflineQueue(cluster.isOfflineQueue());
            config.setMaxOfflineMessages(cluster.getMaxOfflineMessages());
            config.setHeartbeatIntervalMillis(cluster.getHeartbeatIntervalMillis());
            config.setNodeTimeoutMillis(cluster.getNodeTimeoutMillis());
            config.setDeliveryAckTimeoutMillis(cluster.getDeliveryAckTimeoutMillis());
        }
        
        // 添加前缀并序列化为JSON
        return CONFIG_SYNC_PREFIX + JSON.toJSONString(config);
    }

    /**
     * 构建配置请求消息
     */
    public String buildConfigRequestMessage() {
        return CONFIG_REQUEST;
    }

    /**
     * 解析配置同步消息
     */
    public ClusterConfig parseConfigSyncMessage(String message) {
        if (message == null || !message.startsWith(CONFIG_SYNC_PREFIX)) {
            return null;
        }
        String json = message.substring(CONFIG_SYNC_PREFIX.length());
        return JSON.parseObject(json, ClusterConfig.class);
    }

    /**
     * 判断是否为配置请求消息
     */
    public boolean isConfigRequest(String message) {
        return CONFIG_REQUEST.equals(message);
    }

    /**
     * 应用同步的配置到本地
     * 从节点收到主节点的配置后，更新本地配置
     *
     * @param config 从主节点同步过来的配置对象
     */
    public void applySyncedConfig(ClusterConfig config) {
        if (config == null) {
            return;
        }
        
        log.info("应用来自主节点的同步配置");
        // 注意：host和port是本地监听配置，不能被主节点同步覆盖
        if (config.getMaxPayloadSize() > 0) {
            mqttBrokerProperties.setMaxPayloadSize(config.getMaxPayloadSize());
        }
        if (config.getMaxClientIdLength() > 0) {
            mqttBrokerProperties.setMaxClientIdLength(config.getMaxClientIdLength());
        }
        
        // 更新基本Broker配置
        mqttBrokerProperties.setAnonymous(config.isAnonymous());
        mqttBrokerProperties.setUsername(config.getUsername());
        mqttBrokerProperties.setPassword(config.getPassword());
        mqttBrokerProperties.setRetainedMessages(config.isRetainedMessages());
        mqttBrokerProperties.setPersistence(config.isPersistence());
        
        // 更新集群配置
        MqttBrokerProperties.Cluster cluster = mqttBrokerProperties.getCluster();
        if (cluster == null) {
            cluster = new MqttBrokerProperties.Cluster();
            mqttBrokerProperties.setCluster(cluster);
        }
        
        cluster.setGlobalSession(config.isGlobalSession());
        cluster.setDistributedRetained(config.isDistributedRetained());
        cluster.setOfflineQueue(config.isOfflineQueue());
        cluster.setMaxOfflineMessages(config.getMaxOfflineMessages());
        cluster.setHeartbeatIntervalMillis(config.getHeartbeatIntervalMillis());
        cluster.setNodeTimeoutMillis(config.getNodeTimeoutMillis());
        cluster.setDeliveryAckTimeoutMillis(config.getDeliveryAckTimeoutMillis());
        
        log.info("同步配置应用成功");
    }

    /**
     * 集群同步配置数据类
     * 包含需要在主从节点间同步的所有配置项
     */
    public static class ClusterConfig {
        /** 监听主机地址 */
        private String host;
        /** 监听端口 */
        private int port;
        /** 最大消息负载大小（字节） */
        private int maxPayloadSize;
        /** 最大客户端ID长度 */
        private int maxClientIdLength;
        /** 是否允许匿名连接 */
        private boolean anonymous;
        /** 是否支持保留消息 */
        private boolean retainedMessages;
        /** 是否启用持久化 */
        private boolean persistence;
        /** 是否启用全局会话（跨节点共享会话） */
        private boolean globalSession;
        /** 是否启用分布式保留消息 */
        private boolean distributedRetained;
        /** 是否启用离线消息队列 */
        private boolean offlineQueue;
        /** 离线消息队列最大消息数 */
        private int maxOfflineMessages;
        /** 心跳间隔（毫秒） */
        private long heartbeatIntervalMillis;
        /** 节点超时时间（毫秒） */
        private long nodeTimeoutMillis;
        /** 投递确认超时时间（毫秒） */
        private long deliveryAckTimeoutMillis;
        /** 认证用户名 */
        private String username;
        /** 认证密码 */
        private String password;


        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getMaxPayloadSize() {
            return maxPayloadSize;
        }

        public void setMaxPayloadSize(int maxPayloadSize) {
            this.maxPayloadSize = maxPayloadSize;
        }

        public int getMaxClientIdLength() {
            return maxClientIdLength;
        }

        public void setMaxClientIdLength(int maxClientIdLength) {
            this.maxClientIdLength = maxClientIdLength;
        }

        public boolean isAnonymous() {
            return anonymous;
        }

        public void setAnonymous(boolean anonymous) {
            this.anonymous = anonymous;
        }

        public boolean isRetainedMessages() {
            return retainedMessages;
        }

        public void setRetainedMessages(boolean retainedMessages) {
            this.retainedMessages = retainedMessages;
        }

        public boolean isPersistence() {
            return persistence;
        }

        public void setPersistence(boolean persistence) {
            this.persistence = persistence;
        }

        public boolean isGlobalSession() {
            return globalSession;
        }

        public void setGlobalSession(boolean globalSession) {
            this.globalSession = globalSession;
        }

        public boolean isDistributedRetained() {
            return distributedRetained;
        }

        public void setDistributedRetained(boolean distributedRetained) {
            this.distributedRetained = distributedRetained;
        }

        public boolean isOfflineQueue() {
            return offlineQueue;
        }

        public void setOfflineQueue(boolean offlineQueue) {
            this.offlineQueue = offlineQueue;
        }

        public int getMaxOfflineMessages() {
            return maxOfflineMessages;
        }

        public void setMaxOfflineMessages(int maxOfflineMessages) {
            this.maxOfflineMessages = maxOfflineMessages;
        }

        public long getHeartbeatIntervalMillis() {
            return heartbeatIntervalMillis;
        }

        public void setHeartbeatIntervalMillis(long heartbeatIntervalMillis) {
            this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        }

        public long getNodeTimeoutMillis() {
            return nodeTimeoutMillis;
        }

        public void setNodeTimeoutMillis(long nodeTimeoutMillis) {
            this.nodeTimeoutMillis = nodeTimeoutMillis;
        }

        public long getDeliveryAckTimeoutMillis() {
            return deliveryAckTimeoutMillis;
        }

        public void setDeliveryAckTimeoutMillis(long deliveryAckTimeoutMillis) {
            this.deliveryAckTimeoutMillis = deliveryAckTimeoutMillis;
        }
    }
}