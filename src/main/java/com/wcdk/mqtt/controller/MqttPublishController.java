package com.wcdk.mqtt.controller;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @auther WCDK
 * @date 2026/7/29
 * @version 1.0
 **/
@RestController
@ConditionalOnProperty(prefix = "wcdk.mqtt.broker", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequestMapping("/wcdk/mqtt")
public class MqttPublishController {

    private final MqttServiceController mqttServiceController;

    public MqttPublishController(MqttServiceController mqttServiceController) {
        this.mqttServiceController = mqttServiceController;
    }

    @PostMapping("/publish")
    public MqttServiceController.PublishResponse publish(@RequestBody MqttServiceController.PublishRequest request) {
        return mqttServiceController.publish(request);
    }

    @PostMapping("/publish/client")
    public MqttServiceController.PublishResponse publishToClientWithDefaultTopic(
            @RequestBody MqttServiceController.ClientPublishRequest request) {
        return mqttServiceController.publishToClientWithDefaultTopic(request);
    }

    @PostMapping("/publish/client/legacy")
    public MqttServiceController.PublishResponse publishToClient(
            @RequestBody MqttServiceController.ClientPublishRequest request) {
        return mqttServiceController.publishToClient(request);
    }

    @PostMapping("/publish/url")
    public MqttServiceController.PublishResponse publishToBrokerUrl(
            @RequestBody MqttServiceController.UrlPublishRequest request) {
        return mqttServiceController.publishToBrokerUrl(request);
    }
}