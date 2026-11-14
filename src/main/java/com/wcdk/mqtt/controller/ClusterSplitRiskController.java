package com.wcdk.mqtt.controller;

import com.wcdk.mqtt.cluster.ClusterSplitRiskAssessment;
import com.wcdk.mqtt.config.MqttProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;

/**
 * 集群脑裂风险评估控制器
 * <p>
 * 提供集群网络分区（脑裂）风险的实时评估和监控接口。
 * 通过分析心跳状态、连接数、节点响应情况计算风险分和等级。
 * 风险等级：安全(绿色) → 低风险(绿色) → 中风险(蓝色) → 高风险(橙色) → 极危(红色)
 * </p>
 *
 * @author wcdk
 */
@RestController
@RequestMapping("/api/cluster/risk")
public class ClusterSplitRiskController {

    private final MqttProperties properties;
    private final com.wcdk.mqtt.cluster.MqttClusterManager clusterManager;

    private final ClusterSplitRiskAssessment riskAssessment;

    @Autowired
    public ClusterSplitRiskController(MqttProperties properties,
                                      com.wcdk.mqtt.cluster.MqttClusterManager clusterManager) {
        this.properties = properties;
        this.clusterManager = clusterManager;
        this.riskAssessment = new ClusterSplitRiskAssessment(clusterManager, properties);
    }

    /**
     * 获取集群脑裂风险评估结果
     *
     * @return 风险评估结果
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getRiskAssessment() {
        if (riskAssessment == null) {
            return ResponseEntity.status(500)
                    .body(createErrorResponse("集群管理器未初始化", 500));
        }

        ClusterSplitRiskAssessment.RiskAssessmentResult result = riskAssessment.assess();

        Map<String, Object> response = new java.util.HashMap<>();
        response.put("success", true);
        response.put("data", result);
        response.put("timestamp", result.getTimestamp().toString());

        // 添加节点详情
        Map<String, Object> nodeDetails = riskAssessment.getNodeDetails();
        response.put("nodeDetails", nodeDetails);

        // 添加对等连接详情
        Map<String, Object> peerDetails = riskAssessment.getPeerDetails();
        response.put("peerDetails", peerDetails);

        return ResponseEntity.ok(response);
    }

    /**
     * 刷新/重新评估风险
     *
     * @return 刷新结果
     */
    @GetMapping("/refresh")
    public ResponseEntity<Map<String, Object>> refreshRiskAssessment() {
        if (riskAssessment == null) {
            return ResponseEntity.status(500)
                    .body(createErrorResponse("集群管理器未初始化", 500));
        }

        ClusterSplitRiskAssessment.RiskAssessmentResult result = riskAssessment.assess();

        Map<String, Object> response = new java.util.HashMap<>();
        response.put("success", true);
        response.put("data", result);
        response.put("message", "风险评估已刷新");

        return ResponseEntity.ok(response);
    }

    /**
     * 获取节点详细状态
     *
     * @return 节点状态详情
     */
    @GetMapping("/nodes")
    public ResponseEntity<Map<String, Object>> getNodeDetails() {
        if (riskAssessment == null) {
            return ResponseEntity.status(500)
                    .body(createErrorResponse("集群管理器未初始化", 500));
        }

        return ResponseEntity.ok(riskAssessment.getNodeDetails());
    }

    /**
     * 获取对等连接详情
     *
     * @return 对等连接详情
     */
    @GetMapping("/peers")
    public ResponseEntity<Map<String, Object>> getPeerDetails() {
        if (riskAssessment == null) {
            return ResponseEntity.status(500)
                    .body(createErrorResponse("集群管理器未初始化", 500));
        }

        return ResponseEntity.ok(riskAssessment.getPeerDetails());
    }

    /**
     * 获取当前集群配置信息
     *
     * @return 集群配置信息
     */
    @GetMapping("/config")
    public ResponseEntity<Map<String, Object>> getClusterConfig() {
        Map<String, Object> config = new java.util.HashMap<>();
        var cluster = properties.getCluster();

        config.put("enabled", cluster.isEnabled());
        config.put("nodeId", cluster.getNodeId());
        config.put("bindHost", cluster.getBindHost());
        config.put("bindPort", cluster.getBindPort());
        config.put("advertiseHost", cluster.getAdvertiseHost());
        config.put("peers", cluster.getPeers());
        config.put("heartbeatIntervalMillis", cluster.getHeartbeatIntervalMillis());
        config.put("nodeTimeoutMillis", cluster.getNodeTimeoutMillis());
        config.put("reconnectDelayMillis", cluster.getReconnectDelayMillis());
        config.put("username", cluster.getUsername());
        config.put("password", cluster.getPassword());

        return ResponseEntity.ok(config);
    }

    private Map<String, Object> createErrorResponse(String message, int status) {
        Map<String, Object> response = new java.util.HashMap<>();
        response.put("success", false);
        response.put("error", message);
        response.put("status", status);
        return response;
    }
}