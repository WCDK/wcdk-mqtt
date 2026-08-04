package com.wcdk.mqtt.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.wcdk.mqtt.core.config.MqttBrokerProperties;
import com.wcdk.mqtt.core.core.MqttBrokerClusterManager;
import com.wcdk.mqtt.core.core.MqttBrokerSessionRegistry;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @auther WCDK
 * @date 2026/8/3
 * @version 1.0
 **/
@RestController
@ConditionalOnProperty(prefix = "wcdk.mqtt.broker", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequestMapping("/wcdk/mqtt/cluster")
@Tag(name = "MQTT Broker 集群管理", description = "集群节点状态、配置和统计信息")
public class MqttClusterController {

    private final MqttBrokerProperties mqttBrokerProperties;
    private final MqttBrokerSessionRegistry sessionRegistry;
    private final ObjectProvider<MqttBrokerClusterManager> clusterManagerProvider;

    public MqttClusterController(MqttBrokerProperties mqttBrokerProperties,
                                 MqttBrokerSessionRegistry sessionRegistry,
                                 ObjectProvider<MqttBrokerClusterManager> clusterManagerProvider) {
        this.mqttBrokerProperties = mqttBrokerProperties;
        this.sessionRegistry = sessionRegistry;
        this.clusterManagerProvider = clusterManagerProvider;
    }

    @GetMapping("/nodes")
    @Operation(summary = "获取集群节点列表", description = "返回当前集群中所有节点的状态信息")
    public List<ClusterNodeView> getClusterNodes() {
        MqttBrokerClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        List<ClusterNodeView> nodes = new ArrayList<>();
        
        if (clusterManager == null || !clusterManager.isClusterEnabled()) {
            // 集群未启用时，返回当前节点信息
            ClusterNodeView currentNode = new ClusterNodeView();
            currentNode.setNodeId(resolveNodeId());
            currentNode.setRole(mqttBrokerProperties.getCluster() != null ? 
                    mqttBrokerProperties.getCluster().getRole().name() : "SLAVE");
            currentNode.setHost(mqttBrokerProperties.getHost());
            currentNode.setPort(mqttBrokerProperties.getPort());
            currentNode.setMqttHost(mqttBrokerProperties.getHost());
            currentNode.setMqttPort(mqttBrokerProperties.getPort());
            currentNode.setSessionCount((int) sessionRegistry.sessions().stream().filter(session -> session.isActive()).count());
            currentNode.setClusterEnabled(false);
            currentNode.setActive(true);
            currentNode.setUpdatedAt(System.currentTimeMillis());
            currentNode.setIsCurrentNode(true);
            nodes.add(currentNode);
            return nodes;
        }
        
        // 获取集群节点信息
        Map<String, MqttBrokerClusterManager.NodeState> clusterNodes = clusterManager.getNodes();
        for (Map.Entry<String, MqttBrokerClusterManager.NodeState> entry : clusterNodes.entrySet()) {
            MqttBrokerClusterManager.NodeState state = entry.getValue();
            ClusterNodeView nodeView = new ClusterNodeView();
            nodeView.setNodeId(state.getNodeId());
            nodeView.setRole(state.getRole() != null ? state.getRole().name() : "SLAVE");
            nodeView.setHost(state.getHost());
            nodeView.setPort(state.getPort());
            nodeView.setMqttHost(state.getMqttHost());
            nodeView.setMqttPort(state.getMqttPort());
            nodeView.setSessionCount(state.getSessionCount());
            nodeView.setClusterEnabled(true);
            nodeView.setActive(clusterManager.isNodeAlive(state.getNodeId()));
            nodeView.setUpdatedAt(state.getUpdatedAt());
            nodeView.setIsCurrentNode(state.getNodeId().equals(clusterManager.nodeId()));
            nodes.add(nodeView);
        }
        
        // 如果当前节点不在列表中，添加它
        boolean currentNodeExists = nodes.stream()
                .anyMatch(node -> node.getNodeId().equals(clusterManager.nodeId()));
        if (!currentNodeExists) {
            ClusterNodeView currentNode = new ClusterNodeView();
            currentNode.setNodeId(clusterManager.nodeId());
            currentNode.setRole(clusterManager.getRole().name());
            currentNode.setHost(mqttBrokerProperties.getHost());
            currentNode.setPort(mqttBrokerProperties.getPort());
            currentNode.setMqttHost(mqttBrokerProperties.getHost());
            currentNode.setMqttPort(mqttBrokerProperties.getPort());
            currentNode.setSessionCount((int) sessionRegistry.sessions().stream().filter(session -> session.isActive()).count());
            currentNode.setClusterEnabled(true);
            currentNode.setActive(true);
            currentNode.setUpdatedAt(System.currentTimeMillis());
            currentNode.setIsCurrentNode(true);
            nodes.add(currentNode);
        }
        
        return nodes;
    }

    @GetMapping("/config")
    @Operation(summary = "获取集群配置", description = "返回当前 Broker 的集群配置信息")
    public ClusterConfigView getClusterConfig() {
        MqttBrokerProperties.Cluster clusterConfig = mqttBrokerProperties.getCluster();
        ClusterConfigView configView = new ClusterConfigView();
        
        if (clusterConfig == null) {
            configView.setEnabled(false);
            configView.setNodeId(resolveNodeId());
            configView.setChannel("TCP");
            configView.setBindHost("0.0.0.0");
            configView.setBindPort(28883);
            configView.setPeers(new ArrayList<>());
            configView.setGlobalSession(true);
            configView.setDistributedRetained(true);
            configView.setOfflineQueue(true);
            configView.setMaxOfflineMessages(1000);
            configView.setHeartbeatIntervalMillis(10000);
            configView.setNodeTimeoutMillis(30000);
            configView.setDeliveryAckTimeoutMillis(3000);
            return configView;
        }
        
        configView.setEnabled(clusterConfig.isEnabled());
        configView.setNodeId(resolveNodeId());
        configView.setRole(clusterConfig.getRole().name());
        configView.setChannel(clusterConfig.getChannel().name());
        configView.setBindHost(clusterConfig.getBindHost());
        configView.setBindPort(clusterConfig.getBindPort());
        
        // 主节点配置
        if (clusterConfig.getMaster() != null) {
            MasterNodeView masterView = new MasterNodeView();
            masterView.setHost(clusterConfig.getMaster().getHost());
            masterView.setPort(clusterConfig.getMaster().getPort());
            configView.setMaster(masterView);
        }
        
        configView.setPeers(new ArrayList<>(clusterConfig.getPeers()));
        configView.setGlobalSession(clusterConfig.isGlobalSession());
        configView.setDistributedRetained(clusterConfig.isDistributedRetained());
        configView.setOfflineQueue(clusterConfig.isOfflineQueue());
        configView.setMaxOfflineMessages(clusterConfig.getMaxOfflineMessages());
        configView.setHeartbeatIntervalMillis(clusterConfig.getHeartbeatIntervalMillis());
        configView.setNodeTimeoutMillis(clusterConfig.getNodeTimeoutMillis());
        configView.setDeliveryAckTimeoutMillis(clusterConfig.getDeliveryAckTimeoutMillis());
        
        return configView;
    }

    @GetMapping("/stats")
    @Operation(summary = "获取集群统计信息", description = "返回集群的统计信息")
    public ClusterStatsView getClusterStats() {
        MqttBrokerClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        ClusterStatsView statsView = new ClusterStatsView();
        
        // 基本统计
        statsView.setTotalSessions(sessionRegistry.sessions().size());
        statsView.setActiveSessions((int) sessionRegistry.sessions().stream()
                .filter(session -> session.isActive())
                .count());
        statsView.setClusterEnabled(clusterManager != null && clusterManager.isClusterEnabled());
        
        if (clusterManager != null && clusterManager.isClusterEnabled()) {
            // 集群统计
            Map<String, MqttBrokerClusterManager.NodeState> clusterNodes = clusterManager.getNodes();
            statsView.setTotalNodes(clusterNodes.size());
            statsView.setActiveNodes((int) clusterNodes.values().stream()
                    .filter(state -> clusterManager.isNodeAlive(state.getNodeId()))
                    .count());
            statsView.setCurrentNodeId(clusterManager.nodeId());
            
            // 客户端归属统计
            Map<String, String> clientOwners = clusterManager.getClientOwners();
            statsView.setTotalClientOwners(clientOwners.size());
            
            // 会话快照统计
            Map<String, ?> sessionSnapshots = clusterManager.getSessionSnapshots();
            statsView.setTotalSessionSnapshots(sessionSnapshots.size());
            
            // 保留消息统计
            Map<String, ?> retainedMessages = clusterManager.getRetainedMessages();
            statsView.setTotalRetainedMessages(retainedMessages.size());
            
            // 离线消息统计
            Map<String, ?> offlineMessages = clusterManager.getOfflineMessages();
            statsView.setTotalOfflineMessages(offlineMessages.size());
        } else {
            statsView.setTotalNodes(1);
            statsView.setActiveNodes(1);
            statsView.setCurrentNodeId(resolveNodeId());
            statsView.setTotalClientOwners(0);
            statsView.setTotalSessionSnapshots(0);
            statsView.setTotalRetainedMessages(0);
            statsView.setTotalOfflineMessages(0);
        }
        
        return statsView;
    }

    private String resolveNodeId() {
        if (mqttBrokerProperties.getCluster() != null && mqttBrokerProperties.getCluster().getNodeId() != null
                && !mqttBrokerProperties.getCluster().getNodeId().isBlank()) {
            return mqttBrokerProperties.getCluster().getNodeId().trim();
        }
        return mqttBrokerProperties.getHost() + ":" + mqttBrokerProperties.getPort();
    }

    /**
     * 集群节点视图
     */
    public static class ClusterNodeView {
        private String nodeId;
        private String role;
        private String host;
        private int port;
        private String mqttHost;
        private int mqttPort;
        private int sessionCount;
        private boolean clusterEnabled;
        private boolean active;
        private long updatedAt;
        private boolean isCurrentNode;

        public String getNodeId() {
            return nodeId;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public String getRole() {
            return role;
        }

        public void setRole(String role) {
            this.role = role;
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

        public String getMqttHost() {
            return mqttHost;
        }

        public void setMqttHost(String mqttHost) {
            this.mqttHost = mqttHost;
        }

        public int getMqttPort() {
            return mqttPort;
        }

        public void setMqttPort(int mqttPort) {
            this.mqttPort = mqttPort;
        }

        public int getSessionCount() {
            return sessionCount;
        }

        public void setSessionCount(int sessionCount) {
            this.sessionCount = sessionCount;
        }

        public boolean isClusterEnabled() {
            return clusterEnabled;
        }

        public void setClusterEnabled(boolean clusterEnabled) {
            this.clusterEnabled = clusterEnabled;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }

        public long getUpdatedAt() {
            return updatedAt;
        }

        public void setUpdatedAt(long updatedAt) {
            this.updatedAt = updatedAt;
        }

        public boolean isIsCurrentNode() {
            return isCurrentNode;
        }

        public void setIsCurrentNode(boolean isCurrentNode) {
            this.isCurrentNode = isCurrentNode;
        }
    }

    /**
     * 集群配置视图
     */
    public static class ClusterConfigView {
        private boolean enabled;
        private String nodeId;
        private String role;
        private String channel;
        private String bindHost;
        private int bindPort;
        private MasterNodeView master;
        private List<String> peers;
        private boolean globalSession;
        private boolean distributedRetained;
        private boolean offlineQueue;
        private int maxOfflineMessages;
        private long heartbeatIntervalMillis;
        private long nodeTimeoutMillis;
        private long deliveryAckTimeoutMillis;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getNodeId() {
            return nodeId;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public String getRole() {
            return role;
        }

        public void setRole(String role) {
            this.role = role;
        }

        public String getChannel() {
            return channel;
        }

        public void setChannel(String channel) {
            this.channel = channel;
        }

        public String getBindHost() {
            return bindHost;
        }

        public void setBindHost(String bindHost) {
            this.bindHost = bindHost;
        }

        public int getBindPort() {
            return bindPort;
        }

        public void setBindPort(int bindPort) {
            this.bindPort = bindPort;
        }

        public MasterNodeView getMaster() {
            return master;
        }

        public void setMaster(MasterNodeView master) {
            this.master = master;
        }

        public List<String> getPeers() {
            return peers;
        }

        public void setPeers(List<String> peers) {
            this.peers = peers;
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

    /**
     * 主节点配置视图
     */
    public static class MasterNodeView {
        private String host;
        private int port;

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
    }

    /**
     * 集群统计视图
     */
    public static class ClusterStatsView {
        private boolean clusterEnabled;
        private String currentNodeId;
        private int totalNodes;
        private int activeNodes;
        private int totalSessions;
        private int activeSessions;
        private int totalClientOwners;
        private int totalSessionSnapshots;
        private int totalRetainedMessages;
        private int totalOfflineMessages;
        public boolean isClusterEnabled() {
            return clusterEnabled;
        }

        public void setClusterEnabled(boolean clusterEnabled) {
            this.clusterEnabled = clusterEnabled;
        }

        public String getCurrentNodeId() {
            return currentNodeId;
        }

        public void setCurrentNodeId(String currentNodeId) {
            this.currentNodeId = currentNodeId;
        }

        public int getTotalNodes() {
            return totalNodes;
        }

        public void setTotalNodes(int totalNodes) {
            this.totalNodes = totalNodes;
        }

        public int getActiveNodes() {
            return activeNodes;
        }

        public void setActiveNodes(int activeNodes) {
            this.activeNodes = activeNodes;
        }

        public int getTotalSessions() {
            return totalSessions;
        }

        public void setTotalSessions(int totalSessions) {
            this.totalSessions = totalSessions;
        }

        public int getActiveSessions() {
            return activeSessions;
        }

        public void setActiveSessions(int activeSessions) {
            this.activeSessions = activeSessions;
        }

        public int getTotalClientOwners() {
            return totalClientOwners;
        }

        public void setTotalClientOwners(int totalClientOwners) {
            this.totalClientOwners = totalClientOwners;
        }

        public int getTotalSessionSnapshots() {
            return totalSessionSnapshots;
        }

        public void setTotalSessionSnapshots(int totalSessionSnapshots) {
            this.totalSessionSnapshots = totalSessionSnapshots;
        }

        public int getTotalRetainedMessages() {
            return totalRetainedMessages;
        }

        public void setTotalRetainedMessages(int totalRetainedMessages) {
            this.totalRetainedMessages = totalRetainedMessages;
        }

        public int getTotalOfflineMessages() {
            return totalOfflineMessages;
        }

        public void setTotalOfflineMessages(int totalOfflineMessages) {
            this.totalOfflineMessages = totalOfflineMessages;
        }
    }
}