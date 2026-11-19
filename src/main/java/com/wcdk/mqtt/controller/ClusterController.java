package com.wcdk.mqtt.controller;

import java.util.LinkedHashMap;
import java.util.Map;

import com.wcdk.mqtt.cluster.MqttClusterManager;
import com.wcdk.mqtt.config.MqttProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/***
* 集群状态查询接口。
* @author wcdk
***/
@RestController
@RequestMapping("/api/cluster")
public class ClusterController {

    private final MqttClusterManager clusterManager;
    private final MqttProperties properties;

    public ClusterController(MqttClusterManager clusterManager, MqttProperties properties) {
        this.clusterManager = clusterManager;
        this.properties = properties;
    }

    /***
    * 查询本节点与对等节点的集群状态。
    * @author wcdk
    * @return 集群视图
    ***/
    @GetMapping
    public Mono<ApiResponse<Map<String, Object>>> cluster() {
        return Mono.fromSupplier(() -> {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("enabled", properties.getCluster().isEnabled());
            data.put("nodeId", clusterManager.nodeId());
            data.put("advertiseAddress", clusterManager.advertisedAddress());
            data.put("bindHost", properties.getCluster().getBindHost());
            data.put("bindPort", properties.getCluster().getBindPort());
            data.put("connectedPeers", clusterManager.connectedPeerCount());
            data.put("configuredPeers", properties.getCluster().getPeers());
            data.put("quorumEnabled", properties.getCluster().isQuorumEnabled());
            data.put("expectedNodes", clusterManager.expectedNodeCount());
            data.put("quorumSize", clusterManager.quorumSize());
            data.put("reachableNodes", clusterManager.reachableNodeCount());
            data.put("hasQuorum", clusterManager.hasQuorum());
            data.put("readOnly", !clusterManager.hasQuorum());
            data.put("pendingPublishes", clusterManager.pendingPeerPublishCount());
            data.put("droppedPublishes", clusterManager.droppedClusterPublishCount());
            data.put("requireAuth", properties.getCluster().isRequireAuth());
            data.put("tlsEnabled", properties.getCluster().isTlsEnabled());
            data.put("nodes", clusterManager.nodes());
            return ApiResponse.ok(data);
        });
    }
}
