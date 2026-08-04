package com.wcdk.mqtt.service;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.wcdk.mqtt.core.core.MqttBrokerClusterManager;
import com.wcdk.mqtt.core.core.MqttBrokerMessage;
import com.wcdk.mqtt.core.core.MqttBrokerSession;
import com.wcdk.mqtt.core.core.MqttBrokerSessionRegistry;
import com.wcdk.mqtt.core.core.MqttTopicFilter;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ResponseStatusException;

/**
 * @auther WCDK
 * @date 2026/7/29
 * @version 1.0
 **/
@Service
public class MqttMessagePushService {

    private static final Logger log = LoggerFactory.getLogger(MqttMessagePushService.class);

    private final MqttBrokerSessionRegistry sessionRegistry;

    private final ObjectProvider<MqttBrokerClusterManager> clusterManagerProvider;

    public MqttMessagePushService(MqttBrokerSessionRegistry sessionRegistry,
                                  ObjectProvider<MqttBrokerClusterManager> clusterManagerProvider) {
        this.sessionRegistry = sessionRegistry;
        this.clusterManagerProvider = clusterManagerProvider;
    }

    public String publishToClient(String clientId, String topic, String payload, Integer qos, boolean retained) {
        if (!StringUtils.hasText(clientId)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "clientId 不能为空");
        }
        String resolvedClientId = clientId.trim();
        String resolvedTopic = resolveTopic(topic, "wcdk/direct/client/" + encodeTopicLevel(resolvedClientId));
        MqttBrokerSession session = sessionRegistry.findByClientId(resolvedClientId).orElse(null);
        if (session != null) {
            return pushToSession(session, resolvedTopic, payload, qos, retained);
        }
        MqttBrokerClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        if (clusterManager != null && clusterManager.isClusterEnabled()) {
            if (clusterManager.publishToClient(resolvedClientId, buildBrokerMessage(resolvedTopic, payload, qos, retained))) {
                return resolvedTopic;
            }
            throw new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT, "跨节点 MQTT 客户端 ACK 超时");
        }
        throw new ResponseStatusException(HttpStatus.NOT_FOUND, "未找到对应的 MQTT 客户端");
    }

    public String publishToSessionUrl(String sessionUrl, String topic, String payload, Integer qos, boolean retained) {
        if (!StringUtils.hasText(sessionUrl)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "sessionUrl 不能为空");
        }
        String resolvedSessionUrl = sessionUrl.trim();
        String resolvedTopic = resolveTopic(topic, "wcdk/direct/session/" + encodeTopicLevel(resolvedSessionUrl));
        MqttBrokerSession session = sessionRegistry.findBySessionUrl(resolvedSessionUrl).orElse(null);
        if (session != null) {
            return pushToSession(session, resolvedTopic, payload, qos, retained);
        }
        MqttBrokerClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        if (clusterManager != null && clusterManager.isClusterEnabled()) {
            if (clusterManager.publishToSessionUrl(resolvedSessionUrl, buildBrokerMessage(resolvedTopic, payload, qos, retained))) {
                return resolvedTopic;
            }
            throw new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT, "跨节点 MQTT 客户端 ACK 超时");
        }
        throw new ResponseStatusException(HttpStatus.NOT_FOUND, "未找到对应的 MQTT 会话");
    }

    public void publishToBrokerUrl(String brokerUrl,
                                   String topic,
                                   String payload,
                                   Integer qos,
                                   boolean retained,
                                   String clientId,
                                   String username,
                                   String password) {
        if (!StringUtils.hasText(brokerUrl)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "brokerUrl 不能为空");
        }
        if (!MqttTopicFilter.isValidTopicName(topic)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "MQTT 主题无效");
        }
        int normalizedQos = normalizeQos(qos);
        String resolvedClientId = StringUtils.hasText(clientId)
                ? clientId.trim()
                : "wcdk-push-" + UUID.randomUUID();

        MqttClient client = null;
        try {
            client = new MqttClient(brokerUrl.trim(), resolvedClientId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(false);
            options.setCleanSession(true);
            if (StringUtils.hasText(username)) {
                options.setUserName(username.trim());
            }
            if (password != null) {
                options.setPassword(password.toCharArray());
            }
            client.connect(options);
            MqttMessage message = new MqttMessage(payload == null ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8));
            message.setQos(normalizedQos);
            message.setRetained(retained);
            client.publish(topic, message);
            client.disconnect();
        } catch (MqttException ex) {
            log.warn("发布MQTT消息到brokerUrl失败，brokerUrl={}, topic={}", brokerUrl, topic, ex);
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "连接 MQTT URL 发布失败: " + ex.getMessage(), ex);
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (MqttException ex) {
                    log.debug("关闭MQTT客户端失败，brokerUrl={}", brokerUrl, ex);
                }
            }
        }
    }

    private String pushToSession(MqttBrokerSession session, String topic, String payload, Integer qos, boolean retained) {
        MqttBrokerMessage message = buildBrokerMessage(topic, payload, qos, retained);
        MqttQoS mqttQos = message.getQos();
        session.sendPublish(message, mqttQos, retained);
        sessionRegistry.notifySessionUpdated(session);
        return message.getTopic();
    }

    private MqttBrokerMessage buildBrokerMessage(String topic, String payload, Integer qos, boolean retained) {
        String resolvedTopic = resolveTopic(topic, null);
        if (!MqttTopicFilter.isValidTopicName(resolvedTopic)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "MQTT 主题无效");
        }
        int normalizedQos = normalizeQos(qos);
        return new MqttBrokerMessage(
                resolvedTopic,
                payload == null ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8),
                MqttQoS.valueOf(normalizedQos),
                retained);
    }

    private String resolveTopic(String topic, String defaultTopic) {
        if (StringUtils.hasText(topic)) {
            return topic.trim();
        }
        return defaultTopic;
    }

    private String encodeTopicLevel(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private int normalizeQos(Integer qos) {
        if (qos == null) {
            return 0;
        }
        if (qos < 0 || qos > 2) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "MQTT QoS 只能是 0、1 或 2");
        }
        return qos;
    }
}