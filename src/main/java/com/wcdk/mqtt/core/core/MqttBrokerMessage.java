package com.wcdk.mqtt.core.core;

import java.util.Arrays;
import java.util.UUID;

import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.Data;

/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
@Data
public final class MqttBrokerMessage {
    private int id;
    private final String topic;

    private final byte[] payload;

    private final MqttQoS qos;

    private final boolean retained;

    private final String messageId;

    public MqttBrokerMessage(String topic, byte[] payload, MqttQoS qos, boolean retained) {
        this(topic, payload, qos, retained, UUID.randomUUID().toString());
    }

    public MqttBrokerMessage(String topic, byte[] payload, MqttQoS qos, boolean retained, String messageId) {
        this.topic = topic;
        this.payload = payload == null ? new byte[0] : Arrays.copyOf(payload, payload.length);
        this.qos = qos == null ? MqttQoS.AT_MOST_ONCE : qos;
        this.retained = retained;
        this.messageId = messageId == null || messageId.isBlank() ? UUID.randomUUID().toString() : messageId;
    }


    public byte[] payload() {
        return Arrays.copyOf(payload, payload.length);
    }


    public MqttBrokerMessage asRetained() {
        return new MqttBrokerMessage(topic, payload, qos, true, messageId);
    }
}
