package com.wcdk.mqtt.web;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.wcdk.mqtt.core.MqttBrokerMessage;
import com.wcdk.mqtt.core.MqttPublishListener;
import com.wcdk.mqtt.core.MqttSession;
import com.wcdk.mqtt.core.MqttSessionListener;
import com.wcdk.mqtt.core.MqttSessionRegistry;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Sinks;

/***
* 控制台实时事件总线，汇集消息与会话变更并通过 SSE 推送给网页控制台。
* @author wcdk
***/
@Component
public class ConsoleEventBus implements MqttPublishListener, MqttSessionListener {

    /** 事件缓冲容量，超出后丢弃最旧事件 */
    private static final int BUFFER_SIZE = 256;

    private final Sinks.Many<ConsoleEvent> events =
            Sinks.many().multicast().onBackpressureBuffer(BUFFER_SIZE, false);

    public ConsoleEventBus(MqttSessionRegistry registry) {
        registry.addPublishListener(this);
        registry.addSessionListener(this);
    }

    /***
    * 返回实时事件流，供 SSE 接口订阅。
    * @author wcdk
    * @return 事件汇聚点
    ***/
    public Sinks.Many<ConsoleEvent> stream() {
        return events;
    }

    /***
    * 消息发布回调，转换为控制台事件推送给订阅者。
    * @author wcdk
    * @param message 消息
    * @param matchedSubscriptions 命中的订阅过滤器
    ***/
    @Override
    public void onPublish(MqttBrokerMessage message, List<String> matchedSubscriptions) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("id", message.messageId());
        data.put("topic", message.topic());
        data.put("payload", new String(message.payload(), StandardCharsets.UTF_8));
        data.put("qos", message.qos());
        data.put("retained", message.retained());
        data.put("matchedSubscriptions", matchedSubscriptions);
        data.put("receivedAt", Instant.now().toString());
        emit(new ConsoleEvent("message", "append", data));
    }

    @Override
    public void onSessionChanged(MqttSession session) {
        String action = session.isActive() ? "online" : "offline";
        emit(new ConsoleEvent("session", action, sessionView(session)));
    }

    @Override
    public void onSessionRemoved(MqttSession session) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("clientId", session.clientId());
        data.put("remoteAddress", session.remoteAddress() == null ? "" : session.remoteAddress());
        emit(new ConsoleEvent("session", "removed", data));
    }

    private Map<String, Object> sessionView(MqttSession session) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("clientId", session.clientId());
        data.put("username", session.username() == null ? "" : session.username());
        data.put("online", session.isActive());
        data.put("cleanSession", session.cleanSession());
        data.put("ownerNodeId", session.ownerNodeId() == null ? "" : session.ownerNodeId());
        data.put("remoteAddress", session.remoteAddress() == null ? "" : session.remoteAddress());
        data.put("subscriptions", session.subscriptions());
        data.put("offlineQueueSize", session.offlineQueueSize());
        data.put("outboundPendingCount", session.outboundPendingCount());
        data.put("lastActivityTime", Instant.ofEpochMilli(session.lastActivityTime()).toString());
        return data;
    }

    private void emit(ConsoleEvent event) {
        // 无订阅者时返回失败属正常情况，直接忽略
        events.tryEmitNext(event);
    }

    /***
    * 控制台实时事件。
    * @author wcdk
    * @param type 事件类型：message 或 session
    * @param action 动作：append/online/offline/removed
    * @param data 事件数据
    ***/
    public record ConsoleEvent(String type, String action, Object data) {
    }
}
