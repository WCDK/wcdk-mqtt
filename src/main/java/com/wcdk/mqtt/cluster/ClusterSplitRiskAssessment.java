package com.wcdk.mqtt.cluster;

import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.core.MqttSession;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 集群脑裂风险评估
 * <p>
 * 评估 MQTT 集群中网络分区（脑裂）的风险等级。
 * 通过分析心跳超时、连接数、节点不响应等指标计算风险分。
 * 风险等级：安全、低风险、中风险、高风险、极危
 * </p>
 *
 * @author wcdk
 */
public class ClusterSplitRiskAssessment {

    private final MqttClusterManager clusterManager;
    private final MqttProperties properties;

    // 风险阈值配置
    private static final int HEARTBEAT_TIMEOUT_THRESHOLD_MS = 30000;
    private static final int NODE_TIMEOUT_MULTIPLIER = 3;
    private static final int CONNECTED_PEER_WEIGHT = 20;
    private static final int UNRESPONSIVE_PEER_WEIGHT = 30;
    private static final int SINGLE_NODE_WEIGHT = 25;

    /**
     * 构造函数
     *
     * @param clusterManager 集群管理器
     * @param properties     MQTT 属性配置
     */
    public ClusterSplitRiskAssessment(MqttClusterManager clusterManager, MqttProperties properties) {
        this.clusterManager = clusterManager;
        this.properties = properties;
    }

    /**
     * 评估集群脑裂风险
     *
     * @return 风险评估结果
     */
    public RiskAssessmentResult assess() {
        int riskScore = calculateRiskScore();
        RiskLevel riskLevel = determineRiskLevel(riskScore);

        return new RiskAssessmentResult(
                riskScore,
                riskLevel,
                generateRiskFactors(riskScore),
                generateMitigationAdvice(riskLevel),
                Instant.now(),
                clusterManager != null && clusterManager.isRunning()
                        ? clusterManager.connectedPeerCount()
                        : 0
        );
    }

    /**
     * 计算风险分数 (0-100)，分数越高风险越大
     */
    private int calculateRiskScore() {
        int score = 0;

        // 1. 检查集群是否启用
        if (!clusterManager.isRunning()) {
            // 单实例模式，脑裂风险极低
            return 5;
        }

        // 2. 检查对等节点连接数
        int connectedPeers = clusterManager.connectedPeerCount();
        int totalConfiguredPeers = getTotalConfiguredPeers();

        if (totalConfiguredPeers > 0) {
            double connectionRatio = (double) connectedPeers / totalConfiguredPeers;
            if (connectionRatio < 0.3) {
                score += 35;
            } else if (connectionRatio < 0.6) {
                score += 20;
            } else if (connectionRatio < 0.8) {
                score += 10;
            }
        }

        // 3. 检查心跳超时节点
        // ClusterNodeState 有 @Data，提供 getLastSeenAt() getter
        java.util.List<Object> nodeStatesList = new java.util.ArrayList<>();
        // 通过反射或直接访问 nodeStates
        // 使用 clusterManager 的受保护访问或通过其公开的方法
        // 这里使用 nodes() 方法返回 ClusterNodeState 列表
        java.util.List<ClusterNodeState> nodeStates = clusterManager.nodes();
        for (ClusterNodeState state : nodeStates) {
            // 获取距离上次心跳时长
            Instant lastSeen = state.getLastSeenAt();
            if (lastSeen != null) {
                long secondsSinceLastSeen = java.time.Duration.between(lastSeen, Instant.now()).getSeconds();
                long heartbeatInterval = Math.max(1000L, properties.getCluster().getHeartbeatIntervalMillis());
                long nodeTimeoutMillis = heartbeatInterval * NODE_TIMEOUT_MULTIPLIER;
                long nodeTimeoutSeconds = nodeTimeoutMillis / 1000;

                if (secondsSinceLastSeen > nodeTimeoutSeconds) {
                    score += UNRESPONSIVE_PEER_WEIGHT;
                    if (secondsSinceLastSeen > nodeTimeoutSeconds * 2) {
                        score += 15;
                    }
                }
            }
        }

        // 4. 检查是否仅剩单节点运行（单点故障风险）
        if (connectedPeers <= 1 && totalConfiguredPeers > 1) {
            score += SINGLE_NODE_WEIGHT;
        }

        // 5. 检查配置的对等节点是否实际存在
        List<String> configuredPeers = properties.getCluster().getPeers();
        if (configuredPeers != null && !configuredPeers.isEmpty()) {
            int activePeerCount = 0;
            for (String peerAddr : configuredPeers) {
                if (peerAddr != null && !peerAddr.isBlank()) {
                    // 通过 connectedPeerCount 和 nodes 间接检查连通性
                    // 只要有至少一个对等节点连接，即视为部分活跃
                    if (clusterManager.connectedPeerCount() > 0) {
                        activePeerCount++;
                        break;
                    }
                }
            }
            // 配置了对等节点但实际活动连接少，风险增加
            if (configuredPeers.size() > 0 && activePeerCount < configuredPeers.size()) {
                score += 15;
            }
        }

        // 确保分数在 0-100 范围内
        return Math.min(Math.max(score, 0), 100);
    }

    /**
     * 获取总配置对等节点数
     */
    private int getTotalConfiguredPeers() {
        List<String> peers = properties.getCluster().getPeers();
        return peers != null ? peers.size() : 0;
    }

    /**
     * 根据分数确定风险等级
     */
    private RiskLevel determineRiskLevel(int score) {
        if (score >= 80) {
            return RiskLevel.EXTREME;
        } else if (score >= 60) {
            return RiskLevel.HIGH;
        } else if (score >= 40) {
            return RiskLevel.MEDIUM;
        } else if (score >= 20) {
            return RiskLevel.LOW;
        } else {
            return RiskLevel.SAFE;
        }
    }

    /**
     * 生成风险因素描述列表
     */
    private List<String> generateRiskFactors(int score) {
        List<String> factors = new ArrayList<>();

        if (score >= 80) {
            factors.add("超过 50% 配置的对等节点处于心跳超时状态");
            factors.add("检测到网络分区风险，可能存在脑裂情况");
            factors.add("单节点或少节点模式下仍继续处理写入，存在数据不一致风险");
            factors.add("建议立即检查网络连通性并恢复全网连接");
        } else if (score >= 60) {
            factors.add("部分对等节点心跳超时，网络可能存在抖动");
            factors.add("风险进入中高水平，建议监控网络状况");
            factors.add("建议检查网络链路和防火墙设置");
        } else if (score >= 40) {
            factors.add("部分节点响应延迟，建议关注心跳间隔配置");
            factors.add("连接不足但未进入危急状态");
            factors.add("建议适当增加心跳频率或检查网络延迟");
        } else if (score >= 20) {
            factors.add("连接率有所下降，建议检查节点可达性");
            factors.add("整体风险较低，保持当前配置即可");
        } else {
            factors.add("集群连接正常，脑裂风险极低");
            factors.add("建议保持现有的心跳和超时配置");
        }

        return factors;
    }

    /**
     * 生成风险等级对应的防御建议
     */
    private String generateMitigationAdvice(RiskLevel riskLevel) {
        switch (riskLevel) {
            case EXTREME:
                return "立即执行紧急恢复流程：\n" +
                        "1. 检查网络链路是否中断或丢包严重\n" +
                        "2. 重启失联的集群节点\n" +
                        "3. 确保所有节点恢复网络连接后再恢复业务写入\n" +
                        "4. 如有必要，人工介入清理可能的分区状态\n" +
                        "5. 考虑暂时停止少数节点的写入业务，防止数据不一致";
            case HIGH:
                return "高风险预警：\n" +
                        "1. 尽快排查导致心跳超时的网络原因\n" +
                        "2. 重启或重新连接超时的节点\n" +
                        "3. 在节点恢复前，谨慎进行新的持久化写入操作\n" +
                        "4. 建议增大心跳间隔或调整节点超时阈值以适应网络抖动";
            case MEDIUM:
                return "中等风险提醒：\n" +
                        "1. 关注网络状况变化趋势\n" +
                        "2. 适当调大心跳间隔或节点超时时间以容忍短时网络波动\n" +
                        "3. 检查是否有节点正在重启或进行网络切换";
            case LOW:
                return "低风险提示：\n" +
                        "1. 关注连接率变化，特别是少于 50% 配置对等时\n" +
                        "2. 正常监控网络状况即可，无需紧急行动";
            case SAFE:
            default:
                return "安全：集群连接正常，脑裂风险极低。" +
                        "建议保持当前配置，定期进行集群健康检查";
        }
    }

    /**
     * 获取详细的节点状态信息
     */
    public Map<String, Object> getNodeDetails() {
        Map<String, Object> details = new HashMap<>();
        // 使用 clusterManager.nodes() 获取节点状态
        java.util.List<ClusterNodeState> nodeStates = clusterManager.nodes();
        List<Map<String, Object>> nodeInfo = new ArrayList<>();

        for (ClusterNodeState state : nodeStates) {
            Map<String, Object> nodeDetail = new HashMap<>();
            nodeDetail.put("nodeId", state.getNodeId());
            nodeDetail.put("connected", state.isConnected());
            nodeDetail.put("lastSeenAt", state.getLastSeenAt());
            nodeDetail.put("address", state.getAddress());
            nodeDetail.put("dialed", state.isDialed());
            // 计算距离上次心跳时长
            Instant lastSeen = state.getLastSeenAt();
            long secondsSinceLastSeen = (lastSeen != null)
                    ? java.time.Duration.between(lastSeen, Instant.now()).getSeconds()
                    : -1;
            nodeDetail.put("secondsSinceLastSeen", secondsSinceLastSeen);
            nodeInfo.add(nodeDetail);
        }

        details.put("nodeCount", nodeStates.size());
        details.put("nodes", nodeInfo);
        return details;
    }

    /**
     * 获取对等连接详情
     */
    public Map<String, Object> getPeerDetails() {
        Map<String, Object> details = new HashMap<>();
        // MqttClusterManager 的 peers 是 private 的，通过类内部方法间接访问受限
        // 这里提供基本信息
        int connectedCount = clusterManager.connectedPeerCount();
        int totalKnownPeers = clusterManager.nodes().size();

        details.put("connectedCount", connectedCount);
        details.put("totalKnownPeers", totalKnownPeers);
        details.put("note", "详细对等信息请参考集群管理控制台");
        return details;
    }

    /**
     * 风险等级枚举
     */
    public enum RiskLevel {
        SAFE("安全"), LOW("低风险"), MEDIUM("中风险"), HIGH("高风险"), EXTREME("极危");

        private final String description;

        RiskLevel(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    /**
     * 风险评估结果数据类
     */
    public static class RiskAssessmentResult {
        private final int riskScore;
        private final RiskLevel riskLevel;
        private final List<String> riskFactors;
        private final String mitigationAdvice;
        private final Instant timestamp;
        private final int connectedPeerCount;

        public RiskAssessmentResult(int riskScore, RiskLevel riskLevel,
                                    List<String> riskFactors, String mitigationAdvice,
                                    Instant timestamp, int connectedPeerCount) {
            this.riskScore = riskScore;
            this.riskLevel = riskLevel;
            this.riskFactors = riskFactors;
            this.mitigationAdvice = mitigationAdvice;
            this.timestamp = timestamp;
            this.connectedPeerCount = connectedPeerCount;
        }

        public int getRiskScore() {
            return riskScore;
        }

        public RiskLevel getRiskLevel() {
            return riskLevel;
        }

        public List<String> getRiskFactors() {
            return riskFactors;
        }

        public String getMitigationAdvice() {
            return mitigationAdvice;
        }

        public Instant getTimestamp() {
            return timestamp;
        }

        public int getConnectedPeerCount() {
            return connectedPeerCount;
        }

        public String getRiskScoreColor() {
            if (riskScore >= 80) return "#d9534f";
            if (riskScore >= 60) return "#f0ad4e";
            if (riskScore >= 40) return "#5bc0de";
            if (riskScore >= 20) return "#f0ad4e";
            return "#5cb85c";
        }

        public String getRiskLevelColor() {
            if (riskLevel == RiskLevel.EXTREME) return "#d9534f";
            if (riskLevel == RiskLevel.HIGH) return "#f0ad4e";
            if (riskLevel == RiskLevel.MEDIUM) return "#f0ad4e";
            if (riskLevel == RiskLevel.LOW) return "#f0ad4e";
            return "#5cb85c";
        }

        @Override
        public String toString() {
            return "RiskAssessmentResult{" +
                    "riskScore=" + riskScore +
                    ", riskLevel=" + riskLevel +
                    ", connectedPeerCount=" + connectedPeerCount +
                    '}';
        }
    }
}