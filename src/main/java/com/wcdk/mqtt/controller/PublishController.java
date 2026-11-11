package com.wcdk.mqtt.controller;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import com.wcdk.mqtt.core.MqttBrokerMessage;
import com.wcdk.mqtt.core.MqttTopicMatcher;
import com.wcdk.mqtt.service.MqttBrokerService;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/***
* 控制台模拟发布接口。
* @author wcdk
***/
@RestController
@RequestMapping("/api")
public class PublishController {

    private final MqttBrokerService brokerService;

    public PublishController(MqttBrokerService brokerService) {
        this.brokerService = brokerService;
    }

    /***
    * 发布一条消息，指定 clientId 时点对点投递，否则按主题路由并集群广播。
    * @author wcdk
    * @param request 发布请求
    * @return 发布结果
    ***/
    @PostMapping("/publish")
    public Mono<ApiResponse<Map<String, Object>>> publish(@RequestBody PublishRequest request) {
        return Mono.fromSupplier(() -> {
            if (request == null || !StringUtils.hasText(request.topic())) {
                throw new IllegalArgumentException("主题不能为空");
            }
            String topic = request.topic().trim();
            if (!MqttTopicMatcher.isValidTopicName(topic)) {
                throw new IllegalArgumentException("主题名称不合法（不允许包含通配符）：" + topic);
            }
            int qos = request.qos() == null ? 0 : request.qos();
            if (qos < 0 || qos > 2) {
                throw new IllegalArgumentException("QoS 只能为 0、1 或 2");
            }
            byte[] payload = decodePayload(request);
            boolean retained = Boolean.TRUE.equals(request.retained());
            MqttBrokerMessage message = new MqttBrokerMessage(null, topic, payload, qos, retained);

            boolean directDelivered = false;
            if (StringUtils.hasText(request.clientId())) {
                directDelivered = brokerService.publishToClient(request.clientId().trim(), message);
            } else {
                brokerService.publish(message);
            }
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("messageId", message.messageId());
            data.put("topic", topic);
            data.put("qos", qos);
            data.put("retained", retained);
            data.put("targetClientId", request.clientId());
            data.put("directDelivered", directDelivered);
            return ApiResponse.ok(data);
        });
    }

    private byte[] decodePayload(PublishRequest request) {
        String payload = request.payload() == null ? "" : request.payload();
        if ("base64".equalsIgnoreCase(request.payloadEncoding())) {
            try {
                return Base64.getDecoder().decode(payload);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("负载 Base64 编码不合法");
            }
        }
        return payload.getBytes(StandardCharsets.UTF_8);
    }

    /***
    * 发布请求体。
    * @author wcdk
    * @param topic 主题名称
    * @param payload 负载文本
    * @param payloadEncoding 负载编码：text 或 base64，默认 text
    * @param qos QoS
    * @param retained 是否保留消息
    * @param clientId 可选，指定后点对点投递给该客户端
    ***/
    public record PublishRequest(
            String topic,
            String payload,
            String payloadEncoding,
            Integer qos,
            Boolean retained,
            String clientId) {
    }
}
