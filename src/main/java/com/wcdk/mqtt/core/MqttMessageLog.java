package com.wcdk.mqtt.core;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/***
* 最近日志消息的环形缓冲，供管理接口查询与展示。
* @author wcdk
***/
public class MqttMessageLog implements MqttPublishListener {

    private static final int DEFAULT_CAPACITY = 500;

    private final int capacity;
    private final ArrayDeque<LoggedMessage> messages = new ArrayDeque<>();

    public MqttMessageLog() {
        this(DEFAULT_CAPACITY);
    }

    public MqttMessageLog(int capacity) {
        this.capacity = Math.max(1, capacity);
    }

    /***
    * 记录一条已投递的消息及其命中的订阅过滤器。
    * @author wcdk
    * @param message 消息
    * @param matchedSubscriptions 命中的订阅过滤器
    ***/
    @Override
    public void onPublish(MqttBrokerMessage message, List<String> matchedSubscriptions) {
        append(message, matchedSubscriptions);
    }

    /***
    * 记录一条已投递的消息及其命中的订阅过滤器。
    * @author wcdk
    * @param message 消息
    * @param matchedSubscriptions 命中的订阅过滤器
    ***/
    public void append(MqttBrokerMessage message, List<String> matchedSubscriptions) {
        if (message == null) {
            return;
        }
        byte[] payload = message.payload();
        boolean binary = payload != null && !isPrintableUtf8(payload);
        LoggedMessage logged = new LoggedMessage(
                message.messageId(),
                message.topic(),
                binary
                        ? java.util.Base64.getEncoder().encodeToString(payload)
                        : new String(payload == null ? new byte[0] : payload,
                                java.nio.charset.StandardCharsets.UTF_8),
                binary,
                message.qos(),
                message.retained(),
                matchedSubscriptions == null ? List.of() : List.copyOf(matchedSubscriptions),
                Instant.now());
        synchronized (messages) {
            messages.addLast(logged);
            while (messages.size() > capacity) {
                messages.removeFirst();
            }
        }
    }

    /***
    * 判断负载能否作为可读文本展示：UTF-8 严格解码成功且不含不可见控制字符。
    * 二进制负载改用 Base64 展示，避免控制字符污染管理台。
    * @author wcdk
    * @param payload 负载字节
    * @return 可作为文本展示返回 true
    ***/
    private static boolean isPrintableUtf8(byte[] payload) {
        String text;
        try {
            text = java.nio.charset.StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(payload))
                    .toString();
        } catch (java.nio.charset.CharacterCodingException ex) {
            return false;
        }
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isISOControl(c) && c != '\n' && c != '\r' && c != '\t') {
                return false;
            }
        }
        return true;
    }

    /***
    * 按条件分页查询消息。
    * @author wcdk
    * @param topicFilter 主题过滤器，可为空
    * @param qos QoS 过滤，可为空
    * @param from 起始时间，可为空
    * @param to 结束时间，可为空
    * @param limit 最大返回条数
    * @return 按时间倒序排列的消息列表
    ***/
    public List<LoggedMessage> query(String topicFilter, Integer qos, Instant from, Instant to, int limit) {
        int resultLimit = Math.max(1, Math.min(limit, capacity));
        // 仅在锁内复制，过滤与排序在锁外执行，避免控制台查询阻塞消息写入
        List<LoggedMessage> snapshot;
        synchronized (messages) {
            snapshot = new ArrayList<>(messages);
        }
        return snapshot.stream()
                .filter(message -> matches(message, topicFilter, qos, from, to))
                .sorted(Comparator.comparing(LoggedMessage::receivedAt).reversed())
                .limit(resultLimit)
                .toList();
    }

    /***
    * 按消息 ID 批量删除。
    * @author wcdk
    * @param ids 消息 ID 列表
    * @return 删除条数
    ***/
    public int removeByIds(List<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        Set<String> idSet = new LinkedHashSet<>(ids);
        synchronized (messages) {
            int original = messages.size();
            messages.removeIf(message -> idSet.contains(message.id()));
            return original - messages.size();
        }
    }

    /***
    * 按条件批量删除。
    * @author wcdk
    * @param topicFilter 主题过滤器，可为空
    * @param qos QoS 过滤，可为空
    * @param from 起始时间，可为空
    * @param to 结束时间，可为空
    * @return 删除条数
    ***/
    public int removeByCondition(String topicFilter, Integer qos, Instant from, Instant to) {
        synchronized (messages) {
            int original = messages.size();
            messages.removeIf(message -> matches(message, topicFilter, qos, from, to));
            return original - messages.size();
        }
    }

    /***
    * 清空全部消息。
    * @author wcdk
    ***/
    public void clear() {
        synchronized (messages) {
            messages.clear();
        }
    }

    public int size() {
        synchronized (messages) {
            return messages.size();
        }
    }

    public int capacity() {
        return capacity;
    }

    private boolean matches(LoggedMessage message, String topicFilter, Integer qos, Instant from, Instant to) {
        if (topicFilter != null && !topicFilter.isBlank()
                && !MqttTopicMatcher.matches(topicFilter, message.topic())) {
            return false;
        }
        if (qos != null && message.qos() != qos) {
            return false;
        }
        if (from != null && message.receivedAt().isBefore(from)) {
            return false;
        }
        return to == null || !message.receivedAt().isAfter(to);
    }

    /***
    * 管理接口返回的消息视图。
    * @author wcdk
    * @param id 消息 ID
    * @param topic 主题
    * @param payload 负载文本，二进制负载为 Base64 编码
    * @param binary 是否为 Base64 编码的二进制负载
    * @param qos QoS
    * @param retained 是否保留消息
    * @param matchedSubscriptions 命中的订阅过滤器
    * @param receivedAt 接收时间
    ***/
    public record LoggedMessage(
            String id,
            String topic,
            String payload,
            boolean binary,
            int qos,
            boolean retained,
            List<String> matchedSubscriptions,
            Instant receivedAt) {
    }
}
