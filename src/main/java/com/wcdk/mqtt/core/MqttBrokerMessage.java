package com.wcdk.mqtt.core;

import java.util.Arrays;
import java.util.UUID;

/***
* MQTT 消息模型，承载主题、负载、QoS 与保留标记。
* @author wcdk
***/
public record MqttBrokerMessage(
        String messageId,
        String topic,
        byte[] payload,
        int qos,
        boolean retained) {

    /***
    * 规范化构造，补齐消息 ID 并对负载做防御性拷贝。
    * @author wcdk
    ***/
    public MqttBrokerMessage {
        messageId = (messageId == null || messageId.isBlank())
                ? UUID.randomUUID().toString()
                : messageId;
        payload = payload == null ? new byte[0] : Arrays.copyOf(payload, payload.length);
        qos = Math.max(0, Math.min(2, qos));
    }

    /***
    * 按指定主题与 QoS 复制一条新消息，保留原消息 ID 以便集群去重。
    * @author wcdk
    ***/
    public MqttBrokerMessage copyTo(String targetTopic, int targetQos) {
        return new MqttBrokerMessage(messageId, targetTopic, payload, targetQos, retained);
    }

    /***
    * 返回负载的防御性拷贝，避免外部修改内部数组。
    * @author wcdk
    ***/
    public byte[] payloadCopy() {
        return Arrays.copyOf(payload, payload.length);
    }

    /***
    * 标记为保留消息副本。
    * @author wcdk
    ***/
    public MqttBrokerMessage asRetained() {
        return new MqttBrokerMessage(messageId, topic, payload, qos, true);
    }
}
