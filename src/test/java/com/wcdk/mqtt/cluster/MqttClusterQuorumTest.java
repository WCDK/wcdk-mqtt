package com.wcdk.mqtt.cluster;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.core.MqttAcl;
import com.wcdk.mqtt.core.MqttBrokerMessage;
import com.wcdk.mqtt.core.MqttRetainedStore;
import com.wcdk.mqtt.core.MqttSessionRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/***
* 集群多数派计算单元测试：覆盖固定成员数、peers 推导与关闭开关。
* @author wcdk
***/
class MqttClusterQuorumTest {

    /***
    * 验证固定成员数优先于 peers 推导，未达成多数派时判定为不可写。
    * @author wcdk
    ***/
    @Test
    void fixedExpectedNodesShouldDriveQuorum() {
        MqttProperties properties = new MqttProperties();
        properties.getCluster().setEnabled(true);
        properties.getCluster().setExpectedNodes(3);
        MqttClusterManager manager = manager(properties);

        assertThat(manager.expectedNodeCount()).isEqualTo(3);
        assertThat(manager.quorumSize()).isEqualTo(2);
        // 仅本节点可达，3 节点集群的多数派为 2，因此未达成
        assertThat(manager.reachableNodeCount()).isEqualTo(1);
        assertThat(manager.hasQuorum()).isFalse();
    }

    /***
    * 验证 expected-nodes 为 0 时按 peers 非空数量 + 1 计算。
    * @author wcdk
    ***/
    @Test
    void expectedNodesShouldDefaultToPeersPlusSelf() {
        MqttProperties properties = new MqttProperties();
        properties.getCluster().setEnabled(true);
        properties.getCluster().setPeers(List.of("node-2:28883", " ", "node-3:28883"));
        MqttClusterManager manager = manager(properties);

        assertThat(manager.expectedNodeCount()).isEqualTo(3);
        assertThat(manager.quorumSize()).isEqualTo(2);
        assertThat(manager.hasQuorum()).isFalse();
    }

    /***
    * 验证关闭多数派校验或关闭集群时恒为可写，保证单实例与旧配置兼容。
    * @author wcdk
    ***/
    @Test
    void quorumShouldBeBypassedWhenDisabled() {
        MqttProperties properties = new MqttProperties();
        properties.getCluster().setEnabled(true);
        properties.getCluster().setExpectedNodes(5);
        properties.getCluster().setQuorumEnabled(false);
        assertThat(manager(properties).hasQuorum()).isTrue();

        MqttProperties single = new MqttProperties();
        single.getCluster().setEnabled(false);
        single.getCluster().setExpectedNodes(5);
        assertThat(manager(single).hasQuorum()).isTrue();
    }

    /***
    * 验证节点不可达期间的发布进入有界缓冲，容量满时丢弃最旧消息并计数。
    * @author wcdk
    ***/
    @Test
    void pendingPublishesShouldBeBoundedWhenPeerUnavailable() {
        MqttProperties properties = new MqttProperties();
        properties.getCluster().setEventQueueCapacity(16);
        MqttClusterManager manager = manager(properties);

        for (int i = 0; i < 20; i++) {
            manager.bufferPendingPublish("node-2", new MqttBrokerMessage(null, "cluster/buffer/" + i,
                    "x".getBytes(StandardCharsets.UTF_8), 1, false));
        }

        assertThat(manager.pendingPeerPublishCount()).isEqualTo(16);
        assertThat(manager.droppedClusterPublishCount()).isEqualTo(4);
    }

    /***
    * 验证启用集群且强制认证时，未配置凭据会在启动阶段快速失败，
    * 避免无认证集群端口被任意节点注入事件。
    * @author wcdk
    ***/
    @Test
    void startShouldFailWhenAuthRequiredButCredentialsMissing() {
        MqttProperties properties = new MqttProperties();
        properties.getCluster().setEnabled(true);
        MqttClusterManager manager = manager(properties);

        assertThatThrownBy(manager::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("认证凭据");
    }

    private static MqttClusterManager manager(MqttProperties properties) {
        // 单测不启动网络，仅验证纯计算逻辑
        MqttSessionRegistry registry = new MqttSessionRegistry(properties, new MqttAcl(properties),
                new MqttRetainedStore());
        return new MqttClusterManager(properties, registry, new ObjectMapper());
    }
}
