package com.wcdk.mqtt.controller;

import java.util.List;

import com.wcdk.mqtt.core.core.MqttQueue;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
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
public class MqttMessageController {

    private final MqttServiceController mqttServiceController;

    public MqttMessageController(MqttServiceController mqttServiceController) {
        this.mqttServiceController = mqttServiceController;
    }

    @GetMapping("/messages")
    public List<MqttQueue.MqttMessage> messages(@RequestParam(required = false) String topicFilter,
                                                @RequestParam(defaultValue = "100") int limit) {
        return mqttServiceController.messages(topicFilter, limit);
    }

    @GetMapping("/messages/list")
    public MqttServiceController.MessagePageResponse influxMessages(
            @RequestParam(required = false) String topicFilter,
            @RequestParam(required = false) Integer qos,
            @RequestParam(required = false) String sentFrom,
            @RequestParam(required = false) String sentTo,
            @RequestParam(defaultValue = "receivedAt") String sortBy,
            @RequestParam(defaultValue = "desc") String sortDirection,
            @RequestParam(defaultValue = "1") int pageNo,
            @RequestParam(defaultValue = "10") int pageSize) {
        return mqttServiceController.influxMessages(topicFilter, qos, sentFrom, sentTo, sortBy, sortDirection, pageNo, pageSize);
    }

    @DeleteMapping("/messages/list")
    public MqttServiceController.DeleteMessagesResponse deleteInfluxMessages(
            @RequestParam(required = false) String topicFilter,
            @RequestParam(required = false) Integer qos,
            @RequestParam(required = false) String sentFrom,
            @RequestParam(required = false) String sentTo,
            @RequestParam(defaultValue = "false") boolean all) {
        return mqttServiceController.deleteInfluxMessages(topicFilter, qos, sentFrom, sentTo, all);
    }

    @DeleteMapping("/messages/list/selected")
    public MqttServiceController.DeleteMessagesResponse deleteSelectedMessages(
            @RequestBody List<MqttServiceController.SelectedMessageRequest> selectedMessages) {
        return mqttServiceController.deleteSelectedMessages(selectedMessages);
    }

    @DeleteMapping("/messages")
    public void clearMessages() {
        mqttServiceController.clearMessages();
    }
}