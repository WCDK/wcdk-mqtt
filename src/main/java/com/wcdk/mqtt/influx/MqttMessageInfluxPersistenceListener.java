package com.wcdk.mqtt.influx;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

import com.wcdk.mqtt.core.core.MqttBrokerMessage;
import com.wcdk.mqtt.core.core.MqttBrokerPublishListener;
import com.wcdk.mqtt.core.core.MqttQueue;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

@Component
public class MqttMessageInfluxPersistenceListener implements MqttBrokerPublishListener {

    private static final Logger log = LoggerFactory.getLogger(MqttMessageInfluxPersistenceListener.class);

    private static final String MEASUREMENT_NAME = "mqtt_broker_message";

    private final ObjectProvider<InfluxDbUtil> influxDbUtilProvider;

    public MqttMessageInfluxPersistenceListener(ObjectProvider<InfluxDbUtil> influxDbUtilProvider) {
        this.influxDbUtilProvider = influxDbUtilProvider;
    }

    @Override
    public void onPublish(MqttBrokerMessage message) {
        if (message == null) {
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

        String messageId = MqttQueue.randomId();
        Map<String, String> tags = Map.of(
                "topic", message.getTopic(),
                "id", messageId);
        Map<String, Object> fields = new LinkedHashMap<>();
        byte[] payload = message.payload();
        fields.put("qos", qos);
        fields.put("retained", message.isRetained());
        fields.put("payload", new String(payload, StandardCharsets.UTF_8));
        fields.put("payloadSize", payload.length);

        try {
            influxDbUtil.createMeasurement(MEASUREMENT_NAME, tags, fields, OffsetDateTime.now());
        } catch (RuntimeException ex) {
            log.warn("持久化MQTT消息到InfluxDB失败，topic={}, qos={}", message.getTopic(), qos, ex);
        }
    }
}
