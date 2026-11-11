package com.wcdk.mqtt.core;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.wcdk.mqtt.protocol.MqttBrokerServer;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/***
* 访问控制集成测试：默认拒绝策略下按用户名与主题过滤器放行发布订阅。
* @author wcdk
***/
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "wcdk.mqtt.broker.port=0",
        "wcdk.mqtt.cluster.enabled=false",
        "wcdk.mqtt.security.anonymous=true",
        "wcdk.mqtt.acl.enabled=true",
        "wcdk.mqtt.acl.default-policy=deny",
        "wcdk.mqtt.acl.rules[0].policy=allow",
        "wcdk.mqtt.acl.rules[0].action=all",
        "wcdk.mqtt.acl.rules[0].usernames[0]=admin",
        "wcdk.mqtt.acl.rules[0].topic-filters[0]=#",
        "wcdk.mqtt.acl.rules[1].policy=allow",
        "wcdk.mqtt.acl.rules[1].action=subscribe",
        "wcdk.mqtt.acl.rules[1].usernames[0]=guest",
        "wcdk.mqtt.acl.rules[1].topic-filters[0]=public/#",
        "wcdk.mqtt.acl.rules[2].policy=allow",
        "wcdk.mqtt.acl.rules[2].action=publish",
        "wcdk.mqtt.acl.rules[2].usernames[0]=guest",
        "wcdk.mqtt.acl.rules[2].topic-filters[0]=public/telemetry"
})
class MqttAclIntegrationTest {

    @Autowired
    private MqttBrokerServer brokerServer;

    private String brokerUrl;

    @BeforeEach
    void setUp() {
        brokerUrl = "tcp://127.0.0.1:" + brokerServer.port();
    }

    /***
    * 验证放行规则可正常发布订阅、默认拒绝规则拦截越权发布与越权订阅。
    * @author wcdk
    ***/
    @Test
    void aclShouldEnforcePublishAndSubscribeRules() throws Exception {
        String suffix = shortId();
        Collector adminCollector = new Collector();
        Collector guestCollector = new Collector();

        MqttClient admin = createClient("acl-admin-" + suffix, adminCollector, "admin");
        admin.subscribe("#", 1);

        MqttClient guest = createClient("acl-guest-" + suffix, guestCollector, "guest");
        guest.subscribe("public/#", 1);
        // 越权订阅应被拒绝（部分客户端会抛异常，这里只要求连接不中断）
        try {
            guest.subscribe("secret/#", 1);
        } catch (Exception ignored) {
            // SUBACK 返回失败码时客户端可能抛出异常，符合拒绝预期
        }

        // 放行规则：guest 可发布 public/telemetry，admin 与 guest 均应收到
        guest.publish("public/telemetry", bytes("allowed-publish"), 1, false);
        MqttMessage atAdmin = adminCollector.await(5);
        assertThat(atAdmin).isNotNull();
        assertThat(new String(atAdmin.getPayload(), StandardCharsets.UTF_8)).isEqualTo("allowed-publish");
        MqttMessage atGuest = guestCollector.await(5);
        assertThat(atGuest).isNotNull();

        adminCollector.clear();
        guestCollector.clear();

        // 默认拒绝：guest 发布 secret/data 不应被投递给任何人
        guest.publish("secret/data", bytes("denied-publish"), 1, false);
        assertThat(adminCollector.await(1500)).isNull();
        assertThat(guestCollector.await(200)).isNull();

        // 拒绝订阅：admin 发布 secret/other 只有 admin 自己能收到
        admin.publish("secret/other", bytes("secret-payload"), 1, false);
        MqttMessage ownMessage = adminCollector.await(5);
        assertThat(ownMessage).isNotNull();
        assertThat(guestCollector.await(1500)).isNull();

        disconnect(guest);
        disconnect(admin);
    }

    /* ---------- 测试工具 ---------- */

    private String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private MqttClient createClient(String clientId, Collector collector, String username) throws Exception {
        MqttClient client = new MqttClient(brokerUrl, clientId, new MemoryPersistence());
        client.setCallback(collector);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setConnectionTimeout(5);
        options.setKeepAliveInterval(30);
        options.setAutomaticReconnect(false);
        options.setUserName(username);
        options.setPassword("any".toCharArray());
        client.connect(options);
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

        void clear() {
            queue.clear();
        }
    }
}
