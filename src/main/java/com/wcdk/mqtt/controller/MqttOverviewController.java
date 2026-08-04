package com.wcdk.mqtt.controller;

import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
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
public class MqttOverviewController {

    private final MqttServiceController mqttServiceController;

    public MqttOverviewController(MqttServiceController mqttServiceController) {
        this.mqttServiceController = mqttServiceController;
    }

    @GetMapping("/overview")
    public MqttServiceController.OverviewResponse overview() {
        return mqttServiceController.overview();
    }

    @GetMapping("/clients")
    public List<MqttServiceController.ClientSessionView> clients() {
        return mqttServiceController.clients();
    }
}