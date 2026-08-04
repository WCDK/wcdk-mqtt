package com.wcdk.mqtt.core.core;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;
/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
@Component
public class MqttQueue implements MqttBrokerPublishListener {

    private static final int MAX_MESSAGES = 500;

    private final Set<String> subscriptions = ConcurrentHashMap.newKeySet();

    private final ArrayDeque<MqttMessage> messages = new ArrayDeque<>();

    public void subscribe(String topicFilter) {
        if (!MqttTopicFilter.isValidSubscriptionFilter(topicFilter)) {
            throw new IllegalArgumentException("MQTT 主题过滤器无效: " + topicFilter);
        }
        subscriptions.add(topicFilter);
    }

    public boolean unsubscribe(String topicFilter) {
        return subscriptions.remove(topicFilter);
    }

    public List<String> subscriptions() {
        return subscriptions.stream().sorted().toList();
    }

    public List<MqttMessage> messages(String topicFilter, int limit) {
        int resultLimit = Math.max(1, Math.min(limit, MAX_MESSAGES));
        synchronized (messages) {
            return messages.stream()
                    .filter(message -> topicFilter == null || MqttTopicFilter.matches(topicFilter, message.topic))
                    .sorted(Comparator.comparing(MqttMessage::receivedAt).reversed())
                    .limit(resultLimit)
                    .toList();
        }
    }

    public void clearMessages() {
        synchronized (messages) {
            messages.clear();
        }
    }

    public int removeMessages(String topicFilter, Integer qos, Instant sentFrom, Instant sentTo) {
        synchronized (messages) {
            int originalSize = messages.size();
            messages.removeIf(message -> matchesMessage(message, topicFilter, qos, sentFrom, sentTo));
            return originalSize - messages.size();
        }
    }

    public int removeMessagesByIds(List<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        Set<String> idset = new HashSet<>(ids);
        synchronized (messages) {
            int originalSize = messages.size();
            messages.removeIf(message -> idset.contains(message.id()));
            return originalSize - messages.size();
        }
    }

    public int messageCount() {
        synchronized (messages) {
            return messages.size();
        }
    }

    public int capacity() {
        return MAX_MESSAGES;
    }

    @Override
    public void onPublish(MqttBrokerMessage message) {
        List<String> matchedSubscriptions = subscriptions.stream()
                .filter(topicFilter -> MqttTopicFilter.matches(topicFilter, message.getTopic()))
                .sorted()
                .toList();
        if (matchedSubscriptions.isEmpty()) {
            return;
        }

        MqttMessage testMessage = new MqttMessage(
                randomId(),
                message.getTopic(),
                new String(message.payload(), StandardCharsets.UTF_8),
                message.getQos().value(),
                message.isRetained(),
                matchedSubscriptions,
                Instant.now());
        synchronized (messages) {
            messages.addLast(testMessage);
            while (messages.size() > MAX_MESSAGES) {
                messages.removeFirst();
            }
        }
    }

    private boolean matchesMessage(MqttMessage message, String topicFilter, Integer qos, Instant sentFrom, Instant sentTo) {
        if (topicFilter != null && !topicFilter.isBlank() && !MqttTopicFilter.matches(topicFilter, message.topic())) {
            return false;
        }
        if (qos != null && message.qos() != qos) {
            return false;
        }
        if (sentFrom != null && message.receivedAt().isBefore(sentFrom)) {
            return false;
        }
        if (sentTo != null && message.receivedAt().isAfter(sentTo)) {
            return false;
        }
        return true;
    }

    public static String randomId() {
        return UUID.randomUUID().toString();
    }

    private static String buildLegacyid(String topic,
                                               String payload,
                                               int qos,
                                               boolean retained,
                                               Instant receivedAt) {
        String raw = String.join("|",
                topic == null ? "" : topic,
                payload == null ? "" : payload,
                String.valueOf(qos),
                String.valueOf(retained),
                receivedAt == null ? "" : receivedAt.toString());
        return UUID.nameUUIDFromBytes(raw.getBytes(StandardCharsets.UTF_8)).toString();
    }

    public record MqttMessage(String id,
                              String topic,
                              String payload,
                              int qos,
                              boolean retained,
                              List<String> matchedSubscriptions,
                              Instant receivedAt) {

        public MqttMessage {
            id = (id == null || id.isBlank())
                    ? buildLegacyid(topic, payload, qos, retained, receivedAt)
                    : id;
            matchedSubscriptions = List.copyOf(new ArrayList<>(matchedSubscriptions));
        }
    }
}
