package com.wcdk.mqtt.influx;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.wcdk.mqtt.core.core.MqttBrokerMessage;
import com.wcdk.mqtt.core.core.MqttBrokerPublishListener;
import com.wcdk.mqtt.core.config.MqttBrokerProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;


@Component
public class MqttMessageInfluxPersistenceListener implements MqttBrokerPublishListener {

    private static final Logger log = LoggerFactory.getLogger(MqttMessageInfluxPersistenceListener.class);

    private static final String MEASUREMENT_NAME = "mqtt_broker_message";
    private static final Map<String, Boolean> STORED_MESSAGE_IDS = new ConcurrentHashMap<>();

    private final ObjectProvider<InfluxDbUtil> influxDbUtilProvider;

    private final MqttBrokerProperties mqttBrokerProperties;

    public MqttMessageInfluxPersistenceListener(ObjectProvider<InfluxDbUtil> influxDbUtilProvider,
                                                  MqttBrokerProperties mqttBrokerProperties) {
        this.influxDbUtilProvider = influxDbUtilProvider;
        this.mqttBrokerProperties = mqttBrokerProperties;
    }


    @Override
    public void onPublish(MqttBrokerMessage message) {
        if (message == null || !isMasterNode()) {
            return;
        }
        int qos = message.getQos().value();
        if (qos != MqttQoS.AT_LEAST_ONCE.value() && qos != MqttQoS.EXACTLY_ONCE.value()) {
            return;
        }
        InfluxDbUtil influxDbUtil = influxDbUtilProvider.getIfAvailable();
        if (influxDbUtil == null) {
            log.debug("跳过MQTT Influx持久化，因为InfluxDbUtil不可用，topic={}, qos={}", message.getTopic(), qos);
            return;
        }

        byte[] payload = message.payload();
        String messageId = message.getMessageId();
        if (messageId == null || messageId.isBlank()) {
            log.warn("?????? ID ? MQTT Influx ???topic={}", message.getTopic());
            return;
        }
        if (STORED_MESSAGE_IDS.putIfAbsent(messageId, Boolean.TRUE) != null) {
            log.debug("????? MQTT ???messageId={}, topic={}", messageId, message.getTopic());
            return;
        }
        Map<String, String> tags = Map.of("topic", message.getTopic());
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", messageId);

        fields.put("qos", qos);
        fields.put("retained", message.isRetained());
        fields.put("payload", new String(payload, StandardCharsets.UTF_8));
        fields.put("payloadSize", payload.length);

        try {
            // Do not block the Netty MQTT event-loop on an InfluxDB HTTP request.
            // A slow/failed InfluxDB must not disconnect or stall the MQTT client.
            influxDbUtil.writePointAsync(
                    influxDbUtil.buildPoint(MEASUREMENT_NAME, tags, fields, OffsetDateTime.now()));
        } catch (RuntimeException ex) {
            STORED_MESSAGE_IDS.remove(messageId);
            log.warn("提交 MQTT 消息到 InfluxDB 失败，主题：{}，服务质量：{}", message.getTopic(), qos, ex);
            return;
        }
    }
    private boolean isMasterNode() {
        MqttBrokerProperties.Cluster cluster = mqttBrokerProperties.getCluster();
        return cluster == null || !cluster.isEnabled()
                || cluster.getRole() == MqttBrokerProperties.NodeRole.MASTER;
    }

}
