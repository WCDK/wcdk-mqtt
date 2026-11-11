package com.wcdk.mqtt.cluster;

import com.wcdk.mqtt.MqttBrokerApplication;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/***
* 集群通道认证集成测试：凭据一致的节点完成握手，凭据不一致的节点始终无法建立集群连接。
* @author wcdk
***/
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "wcdk.mqtt.broker.port=0",
        "wcdk.mqtt.security.anonymous=true",
        "wcdk.mqtt.cluster.enabled=true",
        "wcdk.mqtt.cluster.node-id=node-auth-a",
        "wcdk.mqtt.cluster.bind-host=127.0.0.1",
        "wcdk.mqtt.cluster.bind-port=38921",
        "wcdk.mqtt.cluster.advertise-host=127.0.0.1",
        "wcdk.mqtt.cluster.peers[0]=127.0.0.1:38922",
        "wcdk.mqtt.cluster.peers[1]=127.0.0.1:38923",
        "wcdk.mqtt.cluster.reconnect-delay-millis=1000",
        "wcdk.mqtt.cluster.username=cluster-user",
        "wcdk.mqtt.cluster.password=cluster-secret"
})
class MqttClusterAuthIntegrationTest {

    /** 等待集群握手建立的最长毫秒数 */
    private static final long HANDSHAKE_TIMEOUT_MILLIS = 15000;

    /** 观察错误凭据节点始终无法连接的时长毫秒数 */
    private static final long REJECT_OBSERVE_MILLIS = 3000;

    @Autowired
    private MqttClusterManager clusterManagerA;

    /** 凭据正确的对等节点 node-auth-b 的 Spring 上下文 */
    private static ConfigurableApplicationContext nodeBContext;

    /** 凭据错误的对等节点 node-auth-c 的 Spring 上下文 */
    private static ConfigurableApplicationContext nodeCContext;

    /** 凭据正确的对等节点的集群管理器 */
    private static MqttClusterManager clusterManagerB;

    /** 凭据错误的对等节点的集群管理器 */
    private static MqttClusterManager clusterManagerC;

    /***
    * 启动两个对等节点：凭据一致的 node-auth-b 与密码错误的 node-auth-c。
    * @author wcdk
    ***/
    @BeforeAll
    static void startPeerNodes() {
        nodeBContext = new SpringApplicationBuilder(MqttBrokerApplication.class)
                .web(WebApplicationType.REACTIVE)
                .run(new String[]{
                        "--server.port=0",
                        "--wcdk.mqtt.broker.port=0",
                        "--wcdk.mqtt.security.anonymous=true",
                        "--wcdk.mqtt.cluster.enabled=true",
                        "--wcdk.mqtt.cluster.node-id=node-auth-b",
                        "--wcdk.mqtt.cluster.bind-host=127.0.0.1",
                        "--wcdk.mqtt.cluster.bind-port=38922",
                        "--wcdk.mqtt.cluster.advertise-host=127.0.0.1",
                        "--wcdk.mqtt.cluster.peers[0]=127.0.0.1:38921",
                        "--wcdk.mqtt.cluster.reconnect-delay-millis=1000",
                        "--wcdk.mqtt.cluster.username=cluster-user",
                        "--wcdk.mqtt.cluster.password=cluster-secret"
                });
        clusterManagerB = nodeBContext.getBean(MqttClusterManager.class);

        nodeCContext = new SpringApplicationBuilder(MqttBrokerApplication.class)
                .web(WebApplicationType.REACTIVE)
                .run(new String[]{
                        "--server.port=0",
                        "--wcdk.mqtt.broker.port=0",
                        "--wcdk.mqtt.security.anonymous=true",
                        "--wcdk.mqtt.cluster.enabled=true",
                        "--wcdk.mqtt.cluster.node-id=node-auth-c",
                        "--wcdk.mqtt.cluster.bind-host=127.0.0.1",
                        "--wcdk.mqtt.cluster.bind-port=38923",
                        "--wcdk.mqtt.cluster.advertise-host=127.0.0.1",
                        "--wcdk.mqtt.cluster.peers[0]=127.0.0.1:38921",
                        "--wcdk.mqtt.cluster.reconnect-delay-millis=1000",
                        "--wcdk.mqtt.cluster.username=cluster-user",
                        "--wcdk.mqtt.cluster.password=cluster-wrong"
                });
        clusterManagerC = nodeCContext.getBean(MqttClusterManager.class);
    }

    /***
    * 关闭两个对等节点，释放集群与接入端口。
    * @author wcdk
    ***/
    @AfterAll
    static void stopPeerNodes() {
        if (nodeBContext != null) {
            nodeBContext.close();
            nodeBContext = null;
        }
        if (nodeCContext != null) {
            nodeCContext.close();
            nodeCContext = null;
        }
    }

    /***
    * 验证凭据一致的节点完成握手，密码错误的节点在观察期内始终无法建立集群连接。
    * @author wcdk
    ***/
    @Test
    void handshakeShouldRejectMismatchedCredentials() throws Exception {
        // 凭据一致的节点应完成双向握手
        awaitPeerConnected(clusterManagerA, "node-auth-b");
        awaitPeerConnected(clusterManagerB, "node-auth-a");

        // 凭据错误的节点持续重拨，但在观察期内任何一侧都不会登记为已连接
        Thread.sleep(REJECT_OBSERVE_MILLIS);
        assertThat(isConnected(clusterManagerA, "node-auth-c")).isFalse();
        assertThat(isConnected(clusterManagerC, "node-auth-a")).isFalse();
        assertThat(clusterManagerA.connectedPeerCount()).isEqualTo(1);
        assertThat(clusterManagerC.connectedPeerCount()).isEqualTo(0);
    }

    /* ---------- 测试工具 ---------- */

    private static void awaitPeerConnected(MqttClusterManager manager, String remoteNodeId) {
        long deadline = System.currentTimeMillis() + HANDSHAKE_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            if (isConnected(manager, remoteNodeId)) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                fail("等待集群握手时被中断");
            }
        }
        fail("等待节点 " + manager.nodeId() + " 与 " + remoteNodeId + " 握手超时，当前节点状态："
                + manager.nodes());
    }

    private static boolean isConnected(MqttClusterManager manager, String nodeId) {
        ClusterNodeState state = manager.nodes().stream()
                .filter(item -> nodeId.equals(item.getNodeId()))
                .findFirst()
                .orElse(null);
        return state != null && state.isConnected();
    }
}
