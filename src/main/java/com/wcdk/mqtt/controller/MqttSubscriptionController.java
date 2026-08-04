package com.wcdk.mqtt.controller;

import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @auther WCDK
 * @date 2026/7/29
 * @version 1.0
 **/
@RestController
@ConditionalOnProperty(prefix = "wcdk.mqtt.broker", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequestMapping("/wcdk/mqtt")
public class MqttSubscriptionController {

    private final MqttServiceController mqttServiceController;

    public MqttSubscriptionController(MqttServiceController mqttServiceController) {
        this.mqttServiceController = mqttServiceController;
    }

    @PostMapping("/subscriptions")
    public List<String> subscribe(@RequestParam String topicFilter) {
        return mqttServiceController.subscribe(topicFilter);
    }

    @GetMapping("/subscriptions")
    public List<String> subscriptions() {
        return mqttServiceController.subscriptions();
    }

    @DeleteMapping("/subscriptions")
    public List<String> unsubscribe(@RequestParam String topicFilter) {
        return mqttServiceController.unsubscribe(topicFilter);
    }
}