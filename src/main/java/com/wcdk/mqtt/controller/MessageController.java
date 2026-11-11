package com.wcdk.mqtt.controller;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.wcdk.mqtt.core.MqttBrokerMessage;
import com.wcdk.mqtt.core.MqttMessageLog;
import com.wcdk.mqtt.core.MqttRetainedStore;
import com.wcdk.mqtt.core.MqttTopicMatcher;
import com.wcdk.mqtt.service.MqttBrokerService;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/***
* 最近消息与保留消息管理接口。
* @author wcdk
***/
@RestController
@RequestMapping("/api")
public class MessageController {

    private final MqttBrokerService brokerService;
    private final MqttMessageLog messageLog;
    private final MqttRetainedStore retainedStore;

    public MessageController(MqttBrokerService brokerService,
                             MqttMessageLog messageLog,
                             MqttRetainedStore retainedStore) {
        this.brokerService = brokerService;
        this.messageLog = messageLog;
        this.retainedStore = retainedStore;
    }

    /***
    * 按条件查询最近消息。
    * @author wcdk
    * @param topicFilter 主题过滤器
    * @param qos QoS 过滤
    * @param from 起始时间
    * @param to 结束时间
    * @param limit 条数上限
    * @return 消息列表
    ***/
    @GetMapping("/messages")
    public Mono<ApiResponse<List<MqttMessageLog.LoggedMessage>>> messages(
            @RequestParam(required = false) String topicFilter,
            @RequestParam(required = false) Integer qos,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(defaultValue = "100") Integer limit) {
        return Mono.fromSupplier(() -> ApiResponse.ok(
                brokerService.messages(topicFilter, qos, from, to, limit)));
    }

    /***
    * 按 ID 或条件批量删除最近消息。
    * @author wcdk
    * @param ids 逗号分隔的消息 ID，与条件二选一
    * @param topicFilter 主题过滤器
    * @param qos QoS 过滤
    * @param from 起始时间
    * @param to 结束时间
    * @return 删除条数
    ***/
    @DeleteMapping("/messages")
    public Mono<ApiResponse<Map<String, Object>>> clearMessages(
            @RequestParam(required = false) String ids,
            @RequestParam(required = false) String topicFilter,
            @RequestParam(required = false) Integer qos,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to) {
        return Mono.fromSupplier(() -> {
            int deleted;
            if (StringUtils.hasText(ids)) {
                List<String> idList = Arrays.stream(ids.split(","))
                        .map(String::trim)
                        .filter(StringUtils::hasText)
                        .toList();
                deleted = brokerService.clearMessagesByIds(idList);
            } else {
                deleted = brokerService.clearMessages(topicFilter, qos, from, to);
            }
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("deleted", deleted);
            return ApiResponse.ok(data);
        });
    }

    /***
    * 查询全部保留消息。
    * @author wcdk
    * @return 保留消息视图列表
    ***/
    @GetMapping("/retained")
    public Mono<ApiResponse<List<RetainedView>>> retained() {
        return Mono.fromSupplier(() -> {
            List<RetainedView> result = new ArrayList<>();
            for (MqttBrokerMessage message : retainedStore.findAll()) {
                result.add(toView(message));
            }
            result.sort((left, right) -> left.topic().compareTo(right.topic()));
            return ApiResponse.ok(result);
        });
    }

    /***
    * 删除指定主题的保留消息（发布空负载保留消息即可删除）。
    * @author wcdk
    * @param topic 主题名称
    * @return 操作结果
    ***/
    @DeleteMapping("/retained")
    public Mono<ApiResponse<Map<String, Object>>> clearRetained(@RequestParam String topic) {
        return Mono.fromSupplier(() -> {
            if (!MqttTopicMatcher.isValidTopicName(topic)) {
                throw new IllegalArgumentException("主题名称不合法：" + topic);
            }
            boolean existed = !retainedStore.findByFilter(topic).stream()
                    .noneMatch(message -> topic.equals(message.topic()));
            retainedStore.store(new MqttBrokerMessage(null, topic, new byte[0], 0, true));
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("removed", existed);
            return ApiResponse.ok(data);
        });
    }

    private RetainedView toView(MqttBrokerMessage message) {
        return new RetainedView(
                message.messageId(),
                message.topic(),
                message.qos(),
                new String(message.payload(), java.nio.charset.StandardCharsets.UTF_8),
                Base64.getEncoder().encodeToString(message.payload()));
    }

    /***
    * 保留消息视图，负载同时提供文本与 Base64 两种形式。
    * @author wcdk
    ***/
    public record RetainedView(
            String messageId,
            String topic,
            int qos,
            String payloadText,
            String payloadBase64) {
    }
}
