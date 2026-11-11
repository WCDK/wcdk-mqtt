package com.wcdk.mqtt.protocol;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/***
* MQTT 协议集成测试：以 Paho 客户端覆盖连接认证、QoS 0/1/2、保留消息、遗嘱与持久会话离线投递。
* @author wcdk
***/
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "wcdk.mqtt.broker.port=0",
        "wcdk.mqtt.broker.sweep-interval-seconds=1",
        "wcdk.mqtt.security.anonymous=false",
        "wcdk.mqtt.security.username=iot",
        "wcdk.mqtt.security.password=secret"
})
class MqttProtocolIntegrationTest {

    /** 测试统一的 MQTT 认证账号 */
    private static final String USERNAME = "iot";
    /** 测试统一的 MQTT 认证密码 */
    private static final String PASSWORD = "secret";

    @Autowired
    private MqttBrokerServer brokerServer;

    private String brokerUrl;

    @BeforeEach
    void setUp() {
        brokerUrl = "tcp://127.0.0.1:" + brokerServer.port();
    }

    /***
    * 验证 QoS 0/1/2 发布订阅全链路：订阅端按订阅 QoS 收到对应消息。
    * @author wcdk
    ***/
    @Test
    void publishShouldDeliverAcrossQosLevels() throws Exception {
        String suffix = shortId();
        String topic0 = "it/qos0/" + suffix;
        String topic1 = "it/qos1/" + suffix;
        String topic2 = "it/qos2/" + suffix;

        Collector collector = new Collector();
        MqttClient subscriber = createClient("sub-qos-" + suffix, collector);
        connect(subscriber, true);
        subscriber.subscribe(new String[]{topic0, topic1, topic2}, new int[]{0, 1, 2});

        MqttClient publisher = createClient("pub-qos-" + suffix, null);
        connect(publisher, true);
        publisher.publish(topic0, bytes("zero"), 0, false);
        publisher.publish(topic1, bytes("one"), 1, false);
        publisher.publish(topic2, bytes("two"), 2, false);

        MqttMessage received0 = collector.await(5);
        MqttMessage received1 = collector.await(5);
        MqttMessage received2 = collector.await(5);

        assertThat(received0).isNotNull();
        assertThat(received1).isNotNull();
        assertThat(received2).isNotNull();
        assertThat(text(received0)).isEqualTo("zero");
        assertThat(text(received1)).isEqualTo("one");
        assertThat(text(received2)).isEqualTo("two");
        assertThat(received1.getQos()).isEqualTo(1);
        assertThat(received2.getQos()).isEqualTo(2);

        disconnect(publisher);
        disconnect(subscriber);
    }

    /***
    * 验证保留消息：先发布保留消息，后订阅者应立即收到该消息。
    * @author wcdk
    ***/
    @Test
    void retainedMessageShouldReachLateSubscriber() throws Exception {
        String suffix = shortId();
        String topic = "it/retained/" + suffix;

        MqttClient publisher = createClient("pub-retained-" + suffix, null);
        connect(publisher, true);
        publisher.publish(topic, bytes("retained-payload"), 1, true);
        disconnect(publisher);

        Collector collector = new Collector();
        MqttClient lateSubscriber = createClient("sub-retained-" + suffix, collector);
        connect(lateSubscriber, true);
        lateSubscriber.subscribe(topic, 1);

        MqttMessage received = collector.await(5);
        assertThat(received).isNotNull();
        assertThat(text(received)).isEqualTo("retained-payload");
        disconnect(lateSubscriber);
    }

    /***
    * 验证同 ID 会话接管时按规范发布遗嘱消息。
    * @author wcdk
    ***/
    @Test
    void willMessageShouldPublishOnSessionTakeover() throws Exception {
        String suffix = shortId();
        String clientId = "will-owner-" + suffix;
        String willTopic = "it/will/" + suffix;

        Collector collector = new Collector();
        MqttClient subscriber = createClient("will-sub-" + suffix, collector);
        connect(subscriber, true);
        subscriber.subscribe(willTopic, 1);

        MqttClient owner = createClient(clientId, null);
        MqttConnectOptions options = options(true);
        options.setUserName(USERNAME);
        options.setPassword(PASSWORD.toCharArray());
        options.setWill(willTopic, bytes("gone-offline"), 1, false);
        owner.connect(options);

        // 第二个客户端以相同 ID 接入，旧连接被接管属异常断开，应触发遗嘱发布
        MqttClient taker = createClient(clientId, null);
        connect(taker, true);

        MqttMessage will = collector.await(5);
        assertThat(will).isNotNull();
        assertThat(text(will)).isEqualTo("gone-offline");

        disconnect(taker);
        disconnect(subscriber);
    }

    /***
    * 验证持久会话断开期间缓冲的 QoS 1 消息在重连后补投，且订阅关系随会话恢复。
    * @author wcdk
    ***/
    @Test
    void persistentSessionShouldDeliverOfflineMessagesAfterReconnect() throws Exception {
        String suffix = shortId();
        String clientId = "persist-" + suffix;
        String topic = "it/offline/" + suffix;

        MqttClient first = createClient(clientId, null);
        connect(first, false);
        first.subscribe(topic, 1);
        first.disconnect();
        first.close();

        MqttClient publisher = createClient("pub-offline-" + suffix, null);
        connect(publisher, true);
        publisher.publish(topic, bytes("buffered-while-away"), 1, false);
        disconnect(publisher);

        // 重连时不重新订阅，验证订阅与离线队列随持久会话恢复
        Collector collector = new Collector();
        MqttClient second = createClient(clientId, collector);
        connect(second, false);

        MqttMessage received = collector.await(5);
        assertThat(received).isNotNull();
        assertThat(text(received)).isEqualTo("buffered-while-away");

        disconnect(second);
    }

    /***
    * 验证密码错误时 CONNECT 被拒绝。
    * @author wcdk
    ***/
    @Test
    void connectWithWrongPasswordShouldBeRejected() throws Exception {
        MqttClient client = createClient("bad-auth-" + shortId(), null);
        MqttConnectOptions options = options(true);
        options.setUserName(USERNAME);
        options.setPassword("wrong-password".toCharArray());

        assertThatThrownBy(() -> client.connect(options)).isInstanceOf(MqttException.class);
        assertThat(client.isConnected()).isFalse();
        client.close();
    }

    /***
    * 验证退订后不再收到该主题消息。
    * @author wcdk
    ***/
    @Test
    void unsubscribeShouldStopDelivery() throws Exception {
        String suffix = shortId();
        String topic = "it/unsub/" + suffix;

        Collector collector = new Collector();
        MqttClient subscriber = createClient("sub-unsub-" + suffix, collector);
        connect(subscriber, true);
        subscriber.subscribe(topic, 1);
        subscriber.unsubscribe(topic);

        MqttClient publisher = createClient("pub-unsub-" + suffix, null);
        connect(publisher, true);
        publisher.publish(topic, bytes("after-unsubscribe"), 1, false);

        assertThat(collector.await(1500)).isNull();

        disconnect(publisher);
        disconnect(subscriber);
    }

    /***
    * 验证持续发布的客户端即使不发 PINGREQ 也不会被 Keep Alive 清扫误断：
    * KeepAlive 2 秒（1.5 倍超时 3 秒），以 800 毫秒间隔连续发布 5 秒，
    * 只有每条报文都刷新活跃时间连接才能存活。
    * @author wcdk
    ***/
    @Test
    void activePublishingShouldRefreshKeepAliveWithoutPingreq() throws Exception {
        String suffix = shortId();
        String topic = "it/keepalive/" + suffix;

        Collector collector = new Collector();
        MqttClient subscriber = createClient("ka-sub-" + suffix, collector);
        connectWithKeepAlive(subscriber, 2);
        subscriber.subscribe(topic, 1);

        MqttClient publisher = createClient("ka-pub-" + suffix, null);
        connectWithKeepAlive(publisher, 2);

        long deadline = System.currentTimeMillis() + 5000;
        int sent = 0;
        while (System.currentTimeMillis() < deadline && publisher.isConnected()) {
            publisher.publish(topic, bytes("tick-" + sent++), 1, false);
            Thread.sleep(800);
        }

        assertThat(publisher.isConnected()).isTrue();
        assertThat(subscriber.isConnected()).isTrue();

        // 连接存活时最终消息仍能正常投递
        collector.clear();
        publisher.publish(topic, bytes("still-alive"), 1, false);
        MqttMessage received = collector.await(3000);
        assertThat(received).isNotNull();
        assertThat(text(received)).isEqualTo("still-alive");

        disconnect(publisher);
        disconnect(subscriber);
    }

    /* ---------- 测试工具 ---------- */

    private String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private String text(MqttMessage message) {
        return new String(message.getPayload(), StandardCharsets.UTF_8);
    }

    private MqttConnectOptions options(boolean cleanSession) {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(cleanSession);
        options.setConnectionTimeout(5);
        options.setKeepAliveInterval(30);
        options.setAutomaticReconnect(false);
        return options;
    }

    private MqttClient createClient(String clientId, Collector collector) throws MqttException {
        MqttClient client = new MqttClient(brokerUrl, clientId, new MemoryPersistence());
        if (collector != null) {
            client.setCallback(collector);
        }
        return client;
    }

    private void connect(MqttClient client, boolean cleanSession) throws MqttException {
        connectWithKeepAlive(client, 30, cleanSession);
    }

    private void connectWithKeepAlive(MqttClient client, int keepAliveSeconds) throws MqttException {
        connectWithKeepAlive(client, keepAliveSeconds, true);
    }

    private void connectWithKeepAlive(MqttClient client, int keepAliveSeconds, boolean cleanSession)
            throws MqttException {
        MqttConnectOptions options = options(cleanSession);
        options.setUserName(USERNAME);
        options.setPassword(PASSWORD.toCharArray());
        options.setKeepAliveInterval(keepAliveSeconds);
        client.connect(options);
    }

    private void disconnect(MqttClient client) {
        try {
            if (client.isConnected()) {
                client.disconnect(1000);
            }
            client.close();
        } catch (MqttException ex) {
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
            // 会话接管测试中断连属预期，忽略
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
