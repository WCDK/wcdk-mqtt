package com.wcdk.mqtt.core;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/***
* 消息日志单元测试：文本负载原样展示，二进制负载转 Base64 并打标。
* @author wcdk
***/
class MqttMessageLogTest {

    /***
    * 验证可读文本负载按原文记录。
    * @author wcdk
    ***/
    @Test
    void textPayloadShouldBeStoredAsText() {
        MqttMessageLog log = new MqttMessageLog(10);
        String text = "你好, MQTT world\t与换行\n";
        log.append(message("it/text", text.getBytes(StandardCharsets.UTF_8)), List.of("it/#"));

        List<MqttMessageLog.LoggedMessage> result = log.query(null, null, null, null, 10);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).payload()).isEqualTo(text);
        assertThat(result.get(0).binary()).isFalse();
        assertThat(result.get(0).matchedSubscriptions()).containsExactly("it/#");
    }

    /***
    * 验证二进制负载转为 Base64 并标记 binary，避免控制字符污染管理台。
    * @author wcdk
    ***/
    @Test
    void binaryPayloadShouldBeStoredAsBase64() {
        byte[] payload = new byte[]{0x00, 0x01, (byte) 0xFF, 0x02, 0x03};
        MqttMessageLog log = new MqttMessageLog(10);
        log.append(message("it/binary", payload), List.of());

        List<MqttMessageLog.LoggedMessage> result = log.query(null, null, null, null, 10);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).binary()).isTrue();
        assertThat(result.get(0).payload())
                .isEqualTo(Base64.getEncoder().encodeToString(payload));
    }

    private static MqttBrokerMessage message(String topic, byte[] payload) {
        return new MqttBrokerMessage(null, topic, payload, 1, false);
    }
}
