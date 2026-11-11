package com.wcdk.mqtt.protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

import com.wcdk.mqtt.core.MqttSessionRegistry;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/***
* MQTT5 会话过期集成测试：以手工构造的 MQTT5 报文覆盖 Session Expiry Interval 全生命周期。
* @author wcdk
***/
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "wcdk.mqtt.broker.port=0",
        "wcdk.mqtt.broker.sweep-interval-seconds=1"
})
class MqttSessionExpiryIntegrationTest {

    @Autowired
    private MqttBrokerServer brokerServer;

    @Autowired
    private MqttSessionRegistry registry;

    private String brokerUrl;

    @BeforeEach
    void setUp() {
        brokerUrl = "tcp://127.0.0.1:" + brokerServer.port();
    }

    /***
    * 验证 Clean Start + 会话过期时间的会话断开后保留，离线消息在过期前重连可补收。
    * @author wcdk
    ***/
    @Test
    void offlineMessageShouldReplayWithinExpiryWindow() throws Exception {
        String suffix = shortId();
        String clientId = "expiry-replay-" + suffix;
        String topic = "it/expiry/replay/" + suffix;
        String payload = "offline-expiry-" + suffix;

        try (Socket first = open()) {
            sendConnect(first, clientId, true, 30);
            assertConnAck(first, false);
            sendSubscribe(first, topic);
            sendDisconnect(first, null);
        }
        // 会话过期时间未到，断开后仍保留在注册表
        assertThat(registry.findByClientId(clientId)).isNotNull();

        publishViaPaho(topic, payload);

        try (Socket second = open()) {
            sendConnect(second, clientId, false, 30);
            // Clean Start=0 且会话仍在过期窗口内，恢复既有会话
            assertConnAck(second, true);
            byte[] publish = readPacket(second.getInputStream());
            assertThat(publish[0] & 0xF0).isEqualTo(0x30);
            assertThat(parsePublishPayload(publish)).isEqualTo(payload);
            // 收尾：DISCONNECT 携带 0 立即结束会话，避免残留影响后续用例
            sendDisconnect(second, 0L);
        }
        awaitRemoved(clientId, 3);
    }

    /***
    * 验证会话超过 Session Expiry Interval 后由巡检从注册表移除。
    * @author wcdk
    ***/
    @Test
    void sessionShouldBeRemovedAfterExpiry() throws Exception {
        String clientId = "expiry-drop-" + shortId();

        try (Socket socket = open()) {
            sendConnect(socket, clientId, true, 1);
            assertConnAck(socket, false);
            sendDisconnect(socket, null);
        }
        // 过期时间未到前会话保留
        assertThat(registry.findByClientId(clientId)).isNotNull();
        // 1 秒过期 + 1 秒扫描间隔，留足容错余量
        awaitRemoved(clientId, 6);
    }

    /***
    * 验证 DISCONNECT 携带 Session Expiry Interval=0 时立即结束会话。
    * @author wcdk
    ***/
    @Test
    void disconnectWithZeroExpiryShouldRemoveSessionImmediately() throws Exception {
        String clientId = "expiry-zero-" + shortId();

        try (Socket socket = open()) {
            sendConnect(socket, clientId, true, 100);
            assertConnAck(socket, false);
            sendDisconnect(socket, 0L);
        }
        awaitRemoved(clientId, 3);
    }

    private Socket open() throws IOException {
        Socket socket = new Socket("127.0.0.1", brokerServer.port());
        socket.setSoTimeout(5000);
        return socket;
    }

    /***
    * 发送 MQTT5 CONNECT，携带 Session Expiry Interval 属性。
    ***/
    private void sendConnect(Socket socket, String clientId, boolean cleanStart, long expirySeconds)
            throws IOException {
        ByteArrayOutputStream variableHeader = new ByteArrayOutputStream();
        // 协议名 MQTT
        variableHeader.write(0);
        variableHeader.write(4);
        variableHeader.write('M');
        variableHeader.write('Q');
        variableHeader.write('T');
        variableHeader.write('T');
        variableHeader.write(5);
        // 连接标志：仅 Clean Start，Keep Alive 30 秒
        variableHeader.write(cleanStart ? 0x02 : 0x00);
        variableHeader.write(0);
        variableHeader.write(30);
        // 属性：Session Expiry Interval（5 字节）
        variableHeader.write(5);
        variableHeader.write(0x11);
        writeInt(variableHeader, expirySeconds);

        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        writeUtf(payload, clientId);
        writePacket(socket, 0x10, concat(variableHeader.toByteArray(), payload.toByteArray()));
    }

    /***
    * 发送 MQTT5 SUBSCRIBE，属性长度为 0，单主题 QoS 1。
    ***/
    private void sendSubscribe(Socket socket, String topic) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        // 包 ID
        body.write(0);
        body.write(1);
        // 属性长度 0
        body.write(0);
        writeUtf(body, topic);
        // 请求 QoS 1
        body.write(1);
        writePacket(socket, 0x82, body.toByteArray());
    }

    /***
    * 发送 DISCONNECT，expirySeconds 非空时携带 Session Expiry Interval 属性覆盖 CONNECT 取值。
    ***/
    private void sendDisconnect(Socket socket, Long expirySeconds) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        if (expirySeconds != null) {
            // 原因码 0x00
            body.write(0x00);
            // 属性长度 5
            body.write(5);
            body.write(0x11);
            writeInt(body, expirySeconds);
        }
        writePacket(socket, 0xE0, body.toByteArray());
    }

    private void assertConnAck(Socket socket, boolean sessionPresent) throws IOException {
        byte[] packet = readPacket(socket.getInputStream());
        assertThat(packet[0] & 0xF0).isEqualTo(0x20);
        assertThat((packet[2] & 0x01) == 1).isEqualTo(sessionPresent);
        // MQTT5 原因码 0 = 接受
        assertThat(packet[3]).isEqualTo((byte) 0x00);
    }

    private void awaitRemoved(String clientId, int timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (registry.findByClientId(clientId) != null && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        assertThat(registry.findByClientId(clientId)).isNull();
    }

    private void publishViaPaho(String topic, String payload) throws MqttException {
        MqttClient client = new MqttClient(brokerUrl, "expiry-pub-" + shortId(), new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setConnectionTimeout(5);
        try {
            client.connect(options);
            client.publish(topic, payload.getBytes(StandardCharsets.UTF_8), 1, false);
            client.disconnect(2000);
        } finally {
            client.close();
        }
    }

    private void writePacket(Socket socket, int fixedHeader, byte[] body) throws IOException {
        ByteArrayOutputStream packet = new ByteArrayOutputStream();
        packet.write(fixedHeader);
        writeVarInt(packet, body.length);
        packet.write(body);
        socket.getOutputStream().write(packet.toByteArray());
        socket.getOutputStream().flush();
    }

    private byte[] readPacket(InputStream in) throws IOException {
        int type = in.read();
        assertThat(type).isNotEqualTo(-1);
        ByteArrayOutputStream packet = new ByteArrayOutputStream();
        packet.write(type);
        int remaining = 0;
        int shift = 0;
        int digit;
        do {
            digit = in.read();
            assertThat(digit).isNotEqualTo(-1);
            packet.write(digit);
            remaining |= (digit & 0x7F) << shift;
            shift += 7;
        } while ((digit & 0x80) != 0);
        packet.write(in.readNBytes(remaining));
        return packet.toByteArray();
    }

    private String parsePublishPayload(byte[] packet) {
        int index = 1;
        // 跳过剩余长度变长整数
        while ((packet[index] & 0x80) != 0) {
            index++;
        }
        index++;
        int topicLength = ((packet[index] & 0xFF) << 8) | (packet[index + 1] & 0xFF);
        index += 2 + topicLength;
        int qos = (packet[0] >> 1) & 0x03;
        if (qos > 0) {
            index += 2;
        }
        // MQTT5 属性长度变长整数
        while ((packet[index] & 0x80) != 0) {
            index++;
        }
        index++;
        return new String(Arrays.copyOfRange(packet, index, packet.length), StandardCharsets.UTF_8);
    }

    private void writeVarInt(OutputStream out, int value) throws IOException {
        do {
            int digit = value % 128;
            value /= 128;
            if (value > 0) {
                digit |= 0x80;
            }
            out.write(digit);
        } while (value > 0);
    }

    private void writeInt(OutputStream out, long value) throws IOException {
        out.write((int) (value >> 24) & 0xFF);
        out.write((int) (value >> 16) & 0xFF);
        out.write((int) (value >> 8) & 0xFF);
        out.write((int) value & 0xFF);
    }

    private void writeUtf(ByteArrayOutputStream out, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        out.write((bytes.length >> 8) & 0xFF);
        out.write(bytes.length & 0xFF);
        out.write(bytes);
    }

    private byte[] concat(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
