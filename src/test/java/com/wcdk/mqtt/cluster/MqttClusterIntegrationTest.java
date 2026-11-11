package com.wcdk.mqtt.cluster;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.wcdk.mqtt.MqttBrokerApplication;
import com.wcdk.mqtt.core.MqttSession;
import com.wcdk.mqtt.core.MqttSessionRegistry;
import com.wcdk.mqtt.protocol.MqttBrokerServer;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
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
* 对等集群集成测试：同 JVM 内启动两个对等节点，验证握手、状态视图与跨节点消息投递。
* @author wcdk
***/
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "wcdk.mqtt.broker.port=0",
        "wcdk.mqtt.security.anonymous=true",
        "wcdk.mqtt.cluster.enabled=true",
        "wcdk.mqtt.cluster.node-id=node-a",
        "wcdk.mqtt.cluster.bind-host=127.0.0.1",
        "wcdk.mqtt.cluster.bind-port=38901",
        "wcdk.mqtt.cluster.advertise-host=127.0.0.1",
        "wcdk.mqtt.cluster.peers[0]=127.0.0.1:38902",
        "wcdk.mqtt.cluster.reconnect-delay-millis=1000"
})
class MqttClusterIntegrationTest {

    /** 等待集群握手建立的最长毫秒数 */
    private static final long HANDSHAKE_TIMEOUT_MILLIS = 15000;

    @Autowired
    private MqttClusterManager clusterManagerA;

    @Autowired
    private MqttBrokerServer brokerServerA;

    @Autowired
    private MqttSessionRegistry sessionRegistryA;

    /** 第二个对等节点 node-b 的 Spring 上下文 */
    private static ConfigurableApplicationContext nodeBContext;

    /** 第二个对等节点的集群管理器 */
    private static MqttClusterManager clusterManagerB;

    /** 第二个对等节点的 MQTT 接入服务 */
    private static MqttBrokerServer brokerServerB;

    /***
    * 启动第二个对等节点 node-b，与 node-a 通过回环地址互配对等。
    * @author wcdk
    ***/
    @BeforeAll
    static void startNodeB() {
        nodeBContext = new SpringApplicationBuilder(MqttBrokerApplication.class)
                .web(WebApplicationType.REACTIVE)
                .run(new String[]{
                        "--server.port=0",
                        "--wcdk.mqtt.broker.port=0",
                        "--wcdk.mqtt.security.anonymous=true",
                        "--wcdk.mqtt.cluster.enabled=true",
                        "--wcdk.mqtt.cluster.node-id=node-b",
                        "--wcdk.mqtt.cluster.bind-host=127.0.0.1",
                        "--wcdk.mqtt.cluster.bind-port=38902",
                        "--wcdk.mqtt.cluster.advertise-host=127.0.0.1",
                        "--wcdk.mqtt.cluster.peers[0]=127.0.0.1:38901",
                        "--wcdk.mqtt.cluster.reconnect-delay-millis=1000"
                });
        clusterManagerB = nodeBContext.getBean(MqttClusterManager.class);
        brokerServerB = nodeBContext.getBean(MqttBrokerServer.class);
    }

    /***
    * 关闭第二个对等节点，释放集群与接入端口。
    * @author wcdk
    ***/
    @AfterAll
    static void stopNodeB() {
        if (nodeBContext != null) {
            nodeBContext.close();
            nodeBContext = null;
        }
    }

    /***
    * 验证两个对等节点互相拨号完成握手，状态视图与公布地址正确。
    * @author wcdk
    ***/
    @Test
    void peerNodesShouldCompleteHandshake() {
        awaitPeerConnected(clusterManagerA, "node-b");
        awaitPeerConnected(clusterManagerB, "node-a");

        assertThat(clusterManagerA.nodeId()).isEqualTo("node-a");
        assertThat(clusterManagerA.advertisedAddress()).isEqualTo("127.0.0.1:38901");
        assertThat(clusterManagerA.connectedPeerCount()).isEqualTo(1);
        assertThat(clusterManagerB.connectedPeerCount()).isEqualTo(1);

        ClusterNodeState nodeBState = findNode(clusterManagerA, "node-b");
        assertThat(nodeBState).isNotNull();
        assertThat(nodeBState.isConnected()).isTrue();
        assertThat(nodeBState.getAddress()).isEqualTo("127.0.0.1:38902");

        ClusterNodeState nodeAState = findNode(clusterManagerB, "node-a");
        assertThat(nodeAState).isNotNull();
        assertThat(nodeAState.isConnected()).isTrue();
        assertThat(nodeAState.getAddress()).isEqualTo("127.0.0.1:38901");
    }

    /***
    * 验证节点 B 入站发布的消息经集群广播投递到节点 A 上的订阅者，且不回环重复。
    * @author wcdk
    ***/
    @Test
    void publishOnNodeBShouldDeliverToSubscriberOnNodeA() throws Exception {
        awaitPeerConnected(clusterManagerA, "node-b");

        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String topic = "cluster/cross/" + suffix;
        Collector collector = new Collector();

        MqttClient subscriber = createClient("tcp://127.0.0.1:" + brokerServerA.port(),
                "cluster-sub-" + suffix, collector);
        subscriber.connect(connectOptions());
        subscriber.subscribe(topic, 1);

        MqttClient publisher = createClient("tcp://127.0.0.1:" + brokerServerB.port(),
                "cluster-pub-" + suffix, null);
        publisher.connect(connectOptions());
        publisher.publish(topic, bytes("cross-node-payload"), 1, false);

        MqttMessage received = collector.await(10000);
        assertThat(received).isNotNull();
        assertThat(new String(received.getPayload(), StandardCharsets.UTF_8)).isEqualTo("cross-node-payload");
        // 集群接收端只做本地投递，不应出现回环重复
        assertThat(collector.await(800)).isNull();

        disconnect(publisher);
        disconnect(subscriber);
    }

    /***
    * 验证持久会话跨节点迁移：客户端从节点 A 迁到节点 B 后，节点 A 释放本地会话副本，
    * 不再为该客户端缓冲消息；客户端回迁节点 A 时订阅随快照恢复且不重复收到迁移期间的消息。
    * @author wcdk
    ***/
    @Test
    void migratedSessionShouldNotDuplicateOfflineDelivery() throws Exception {
        awaitPeerConnected(clusterManagerA, "node-b");
        MqttSessionRegistry registryB = nodeBContext.getBean(MqttSessionRegistry.class);

        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String clientId = "migrate-" + suffix;
        String topic = "cluster/migrate/" + suffix;

        // 1. 客户端在节点 A 建立持久会话并订阅后断开，会话保留在节点 A
        MqttClient first = createClient("tcp://127.0.0.1:" + brokerServerA.port(), clientId, null);
        first.connect(connectOptions(false));
        first.subscribe(topic, 1);
        first.disconnect();
        first.close();
        assertThat(sessionRegistryA.findByClientId(clientId)).isNotNull();

        // 2. 等待节点 B 同步到含订阅的会话快照，同一客户端接入节点 B
        awaitSnapshotWithSubscription(registryB, clientId);
        Collector migrationCollector = new Collector();
        MqttClient second = createClient("tcp://127.0.0.1:" + brokerServerB.port(), clientId,
                migrationCollector);
        second.connect(connectOptions(false));

        // 3. 节点 A 收到 SESSION_CLAIM 后释放本地会话副本，不再为该客户端缓冲消息
        awaitLocalSessionReleased(sessionRegistryA, clientId);

        // 4. 节点 A 上发布的消息经集群投递到节点 B 上的客户端，只投递一次
        MqttClient publisher = createClient("tcp://127.0.0.1:" + brokerServerA.port(),
                "migrate-pub-" + suffix, null);
        publisher.connect(connectOptions(true));
        publisher.publish(topic, bytes("delivered-once"), 1, false);
        MqttMessage delivered = migrationCollector.await(10000);
        assertThat(delivered).isNotNull();
        assertThat(new String(delivered.getPayload(), StandardCharsets.UTF_8)).isEqualTo("delivered-once");
        disconnect(publisher);

        // 5. 客户端断开节点 B 并回迁节点 A，不重新订阅
        second.disconnect();
        second.close();

        Collector returnCollector = new Collector();
        MqttClient third = createClient("tcp://127.0.0.1:" + brokerServerA.port(), clientId, returnCollector);
        third.connect(connectOptions(false));

        // 迁移期间的消息已在节点 B 收到过，回迁后不应重复投递
        assertThat(returnCollector.await(1500)).isNull();

        // 订阅随快照恢复，后续消息仍只投递一次
        publisher = createClient("tcp://127.0.0.1:" + brokerServerA.port(),
                "migrate-pub2-" + suffix, null);
        publisher.connect(connectOptions(true));
        publisher.publish(topic, bytes("after-migration"), 1, false);
        disconnect(publisher);

        MqttMessage received = returnCollector.await(10000);
        assertThat(received).isNotNull();
        assertThat(new String(received.getPayload(), StandardCharsets.UTF_8)).isEqualTo("after-migration");
        assertThat(returnCollector.await(800)).isNull();

        disconnect(third);
    }

    /* ---------- 测试工具 ---------- */

    private static void awaitLocalSessionReleased(MqttSessionRegistry registry, String clientId) {
        long deadline = System.currentTimeMillis() + HANDSHAKE_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            if (registry.findByClientId(clientId) == null) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                fail("等待释放被迁移会话时被中断");
            }
        }
        fail("等待节点释放被迁移的本地会话超时，客户端ID：" + clientId);
    }

    private static void awaitSnapshotWithSubscription(MqttSessionRegistry registry, String clientId) {
        long deadline = System.currentTimeMillis() + HANDSHAKE_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            MqttSession.SessionSnapshot snapshot = registry.remoteSnapshots().get(clientId);
            if (snapshot != null && snapshot.getSubscriptions() != null
                    && !snapshot.getSubscriptions().isEmpty()) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                fail("等待会话快照同步时被中断");
            }
        }
        fail("等待含订阅的会话快照同步超时，客户端ID：" + clientId);
    }

    private static void awaitPeerConnected(MqttClusterManager manager, String remoteNodeId) {
        long deadline = System.currentTimeMillis() + HANDSHAKE_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            if (findNode(manager, remoteNodeId) != null && findNode(manager, remoteNodeId).isConnected()) {
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

    private static ClusterNodeState findNode(MqttClusterManager manager, String nodeId) {
        return manager.nodes().stream()
                .filter(state -> nodeId.equals(state.getNodeId()))
                .findFirst()
                .orElse(null);
    }

    private byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private MqttConnectOptions connectOptions() {
        return connectOptions(true);
    }

    private MqttConnectOptions connectOptions(boolean cleanSession) {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(cleanSession);
        options.setConnectionTimeout(5);
        options.setKeepAliveInterval(30);
        options.setAutomaticReconnect(false);
        return options;
    }

    private MqttClient createClient(String url, String clientId, Collector collector) throws Exception {
        MqttClient client = new MqttClient(url, clientId, new MemoryPersistence());
        if (collector != null) {
            client.setCallback(collector);
        }
        return client;
    }

    private void disconnect(MqttClient client) {
        try {
            if (client.isConnected()) {
                client.disconnect(1000);
            }
            client.close();
        } catch (Exception ex) {
            // 测试收尾阶段忽略断开异常
        }
    }

    /***
    * 消息收集回调：把收到的消息放入阻塞队列供断言使用。
    * @author wcdk
    ***/
    private static final class Collector implements MqttCallback {

        private final BlockingQueue<MqttMessage> queue = new LinkedBlockingQueue<>();

        @Override
        public void connectionLost(Throwable cause) {
            // 测试中无预期断连，忽略
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
            queue.offer(message);
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
            // QoS 发布确认无需额外处理
        }

        MqttMessage await(long timeoutMillis) throws InterruptedException {
            return queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        }
    }
}
