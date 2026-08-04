package com.wcdk.mqtt.core.core;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.Data;

/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
@Data
public class MqttBrokerSession {

    private volatile Channel channel;
    private final String clientId;
    private final boolean cleanSession;
    private final String username;
    private volatile MqttBrokerMessage willMessage;
    private final Map<String, MqttQoS> subscriptions = new ConcurrentHashMap<>();
    private final Map<Integer, MqttBrokerMessage> inboundQos2 = new ConcurrentHashMap<>();
    private final Map<Integer, OutboundPendingMessage> outboundPendingMessages = new ConcurrentHashMap<>();
    /** 集群确认上下文映射，存储等待集群确认的消息包ID和确认信息 */
    private final Map<Integer, ClusterAckContext> clusterAckContexts = new ConcurrentHashMap<>();
    private final Queue<QueuedPublishMessage> queuedPublishMessages = new ConcurrentLinkedQueue<>();
    private final Queue<String> publicTopics = new ConcurrentLinkedQueue<>();
    private final AtomicInteger packetIdSequence = new AtomicInteger();
    private volatile boolean disconnectedGracefully;

    public MqttBrokerSession(Channel channel,
                             String clientId,
                             boolean cleanSession,
                             String username,
                             MqttBrokerMessage willMessage) {
        this.channel = channel;
        this.clientId = clientId;
        this.cleanSession = cleanSession;
        this.username = username;
        this.willMessage = willMessage;
    }

    public Channel channel() {
        return channel;
    }

    public String clientId() {
        return clientId;
    }

    public boolean cleanSession() {
        return cleanSession;
    }

    public String username() {
        return username;
    }

    public MqttBrokerMessage willMessage() {
        return willMessage;
    }

    public boolean isActive() {
        Channel currentChannel = channel;
        return currentChannel != null && currentChannel.isActive();
    }

    public String sessionUrl() {
        Channel currentChannel = channel;
        if (currentChannel == null || currentChannel.remoteAddress() == null) {
            return null;
        }
        return String.valueOf(currentChannel.remoteAddress());
    }

    public void markDisconnectedGracefully() {
        this.disconnectedGracefully = true;
    }

    public void attach(Channel channel, MqttBrokerMessage willMessage) {
        this.channel = channel;
        this.willMessage = willMessage;
        this.disconnectedGracefully = false;
    }

    public void detach() {
        this.channel = null;
    }

    public void close() {
        Channel currentChannel = channel;
        if (currentChannel != null) {
            currentChannel.close();
        }
    }

    public void subscribe(String topicFilter, MqttQoS qos) {
        subscriptions.put(topicFilter, qos);
    }

    public void unsubscribe(String topicFilter) {
        subscriptions.remove(topicFilter);
    }

    public Map<String, MqttQoS> subscriptions() {
        return Map.copyOf(subscriptions);
    }

    public List<String> publicTopics() {
        return publicTopics.stream().distinct().sorted().toList();
    }

    public void recordPublicTopic(String topic) {
        if (topic != null && !topic.isBlank()) {
            publicTopics.add(topic);
        }
    }

    public OptionalInt maxSubscriptionQos(String topic) {
        int maxQos = -1;
        for (Map.Entry<String, MqttQoS> entry : subscriptions.entrySet()) {
            if (MqttTopicFilter.matches(entry.getKey(), topic)) {
                maxQos = Math.max(maxQos, entry.getValue().value());
            }
        }
        return maxQos < 0 ? OptionalInt.empty() : OptionalInt.of(maxQos);
    }

    public void saveInboundQos2(int packetId, MqttBrokerMessage message) {
        inboundQos2.putIfAbsent(packetId, message);
    }

    public MqttBrokerMessage removeInboundQos2(int packetId) {
        return inboundQos2.remove(packetId);
    }

    public synchronized ClusterAckContext confirmOutboundQos1(int packetId) {
        OutboundPendingMessage pendingMessage = outboundPendingMessages.get(packetId);
        if (pendingMessage != null && pendingMessage.state() == OutboundPendingState.WAITING_FOR_PUBACK) {
            outboundPendingMessages.remove(packetId);
            return clusterAckContexts.remove(packetId);
        }
        return null;
    }

    public synchronized boolean receiveOutboundQos2PubRec(int packetId) {
        OutboundPendingMessage pendingMessage = outboundPendingMessages.get(packetId);
        if (pendingMessage == null || pendingMessage.state() != OutboundPendingState.WAITING_FOR_PUBREC) {
            return false;
        }
        outboundPendingMessages.put(packetId, pendingMessage.withState(OutboundPendingState.WAITING_FOR_PUBCOMP));
        return true;
    }

    public synchronized ClusterAckContext confirmOutboundQos2(int packetId) {
        OutboundPendingMessage pendingMessage = outboundPendingMessages.get(packetId);
        if (pendingMessage != null && pendingMessage.state() == OutboundPendingState.WAITING_FOR_PUBCOMP) {
            outboundPendingMessages.remove(packetId);
            return clusterAckContexts.remove(packetId);
        }
        return null;
    }

    public synchronized int sendPublish(MqttBrokerMessage message, MqttQoS qos, boolean retained) {
        if (qos == MqttQoS.AT_MOST_ONCE) {
            writePublish(message, qos, retained, 0, false);
            return 0;
        }
        if (!isActive()) {
            if (!cleanSession) {
                queuedPublishMessages.add(new QueuedPublishMessage(message, qos, retained));
            }
            return -1;
        }
        int packetId = nextPacketId();
        OutboundPendingState state = qos == MqttQoS.EXACTLY_ONCE
                ? OutboundPendingState.WAITING_FOR_PUBREC
                : OutboundPendingState.WAITING_FOR_PUBACK;
        outboundPendingMessages.put(packetId, new OutboundPendingMessage(packetId, message, qos, retained, state));
        writePublish(message, qos, retained, packetId, false);
        return packetId;
    }

    /**
     * 绑定集群确认上下文
     * 将消息包ID与集群请求ID和源节点ID关联，用于后续确认追踪
     *
     * @param packetId MQTT消息包ID
     * @param requestId 集群请求ID，用于追踪确认
     * @param sourceNodeId 源节点ID，标识请求来自哪个节点
     */
    public synchronized void bindClusterAckContext(int packetId, String requestId, String sourceNodeId) {
        if (packetId > 0 && requestId != null && !requestId.isBlank() && sourceNodeId != null && !sourceNodeId.isBlank()) {
            clusterAckContexts.put(packetId, new ClusterAckContext(requestId, sourceNodeId));
        }
    }

    /**
     * 生成QoS状态快照
     * 将当前会话的QoS状态序列化，用于集群间的状态同步
     *
     * @return QoS状态快照对象
     */
    public synchronized QosStateSnapshot qosStateSnapshot() {
        QosStateSnapshot snapshot = new QosStateSnapshot();
        // 保存当前包ID序列号
        snapshot.setPacketIdSequence(packetIdSequence.get());
        // 保存入站QoS2消息状态
        snapshot.setInboundQos2(inboundQos2.entrySet().stream()
                .map(entry -> MessageState.from(entry.getKey(), entry.getValue(), null, false))
                .toList());
        // 保存出站待确认消息状态
        snapshot.setOutboundPendingMessages(outboundPendingMessages.values().stream()
                .map(pending -> MessageState.from(pending.packetId(), pending.message(), pending.state().name(), pending.retained()))
                .toList());
        return snapshot;
    }

    /**
     * 从快照恢复QoS状态
     * 从集群同步过来的快照中恢复会话的QoS状态
     *
     * @param snapshot QoS状态快照对象
     */
    public synchronized void restoreQosState(QosStateSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        // 清空当前状态
        inboundQos2.clear();
        outboundPendingMessages.clear();
        clusterAckContexts.clear();
        // 恢复入站QoS2消息
        if (snapshot.getInboundQos2() != null) {
            for (MessageState state : snapshot.getInboundQos2()) {
                MqttBrokerMessage message = state.toBrokerMessage();
                if (state.getPacketId() > 0 && message != null) {
                    inboundQos2.put(state.getPacketId(), message);
                }
            }
        }
        // 恢复出站待确认消息
        if (snapshot.getOutboundPendingMessages() != null) {
            for (MessageState state : snapshot.getOutboundPendingMessages()) {
                MqttBrokerMessage message = state.toBrokerMessage();
                OutboundPendingState pendingState = parseOutboundPendingState(state.getState());
                if (state.getPacketId() > 0 && message != null && pendingState != null) {
                    outboundPendingMessages.put(state.getPacketId(), new OutboundPendingMessage(
                            state.getPacketId(), message, message.getQos(), state.isRetained(), pendingState));
                }
            }
        }
        // 恢复包ID序列号
        packetIdSequence.set(Math.max(0, snapshot.getPacketIdSequence()));
    }

    /**
     * 从之前的会话恢复状态
     * 当客户端重连时，将旧会话的状态迁移到新会话
     *
     * @param previousSession 之前的会话对象
     */
    public synchronized void restoreStateFrom(MqttBrokerSession previousSession) {
        // 恢复订阅主题
        subscriptions.clear();
        subscriptions.putAll(previousSession.subscriptions);
        // 恢复入站QoS2消息
        inboundQos2.clear();
        inboundQos2.putAll(previousSession.inboundQos2);
        // 恢复出站待确认消息
        outboundPendingMessages.clear();
        outboundPendingMessages.putAll(previousSession.outboundPendingMessages);
        // 恢复集群确认上下文
        clusterAckContexts.clear();
        clusterAckContexts.putAll(previousSession.clusterAckContexts);
        // 恢复排队的发布消息
        queuedPublishMessages.clear();
        queuedPublishMessages.addAll(previousSession.drainQueuedPublishMessages());
        // 恢复发布过的主题
        publicTopics.clear();
        publicTopics.addAll(previousSession.publicTopics());
        // 恢复包ID序列号
        packetIdSequence.set(previousSession.packetIdSequence.get());
    }

    public synchronized void replayPendingMessages() {
        if (!isActive()) {
            return;
        }
        List<OutboundPendingMessage> pendingMessages = new ArrayList<>(outboundPendingMessages.values());
        for (OutboundPendingMessage pendingMessage : pendingMessages) {
            if (pendingMessage.state() == OutboundPendingState.WAITING_FOR_PUBCOMP) {
                writePubRel(pendingMessage.packetId());
                continue;
            }
            writePublish(
                    pendingMessage.message(),
                    pendingMessage.message().getQos(),
                    pendingMessage.retained(),
                    pendingMessage.packetId(),
                    true);
        }
    }

    public synchronized void drainQueuedPublishes() {
        if (!isActive()) {
            return;
        }
        QueuedPublishMessage queuedMessage;
        while ((queuedMessage = queuedPublishMessages.poll()) != null) {
            sendPublish(queuedMessage.message(), queuedMessage.qos(), queuedMessage.retained());
        }
    }

    public int inboundQos2Count() {
        return inboundQos2.size();
    }

    public int outboundPendingCount() {
        return outboundPendingMessages.size();
    }

    public int queuedPublishCount() {
        return queuedPublishMessages.size();
    }

    private int nextPacketId() {
        int next = packetIdSequence.updateAndGet(current -> current >= 65_535 ? 1 : current + 1);
        return next == 0 ? 1 : next;
    }

    private List<QueuedPublishMessage> drainQueuedPublishMessages() {
        List<QueuedPublishMessage> queuedMessages = new ArrayList<>();
        QueuedPublishMessage queuedMessage;
        while ((queuedMessage = queuedPublishMessages.poll()) != null) {
            queuedMessages.add(queuedMessage);
        }
        return queuedMessages;
    }

    private void writePublish(MqttBrokerMessage message, MqttQoS qos, boolean retained, int packetId, boolean duplicate) {
        Channel currentChannel = channel;
        if (currentChannel == null || !currentChannel.isActive()) {
            return;
        }
        MqttMessageBuilders.PublishBuilder builder = MqttMessageBuilders.publish()
                .topicName(message.getTopic())
                .qos(qos)
                .retained(retained)
                .payload(Unpooled.wrappedBuffer(message.payload()));
        if (packetId > 0) {
            builder.messageId(packetId);
        }
        if (!duplicate) {
            currentChannel.writeAndFlush(builder.build());
            return;
        }
        MqttPublishMessage publishMessage = builder.build();
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                publishMessage.fixedHeader().messageType(),
                true,
                publishMessage.fixedHeader().qosLevel(),
                publishMessage.fixedHeader().isRetain(),
                publishMessage.fixedHeader().remainingLength());
        currentChannel.writeAndFlush(MqttMessageFactory.newMessage(
                fixedHeader,
                publishMessage.variableHeader(),
                publishMessage.payload().retainedDuplicate()));
    }

    private void writePubRel(int packetId) {
        Channel currentChannel = channel;
        if (currentChannel == null || !currentChannel.isActive()) {
            return;
        }
        MqttMessage message = MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                MqttMessageIdVariableHeader.from(packetId),
                null);
        currentChannel.writeAndFlush(message);
    }

    private static OutboundPendingState parseOutboundPendingState(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return OutboundPendingState.valueOf(value);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    @Data
    public static class QosStateSnapshot {

        private int packetIdSequence;

        private List<MessageState> inboundQos2 = new ArrayList<>();

        private List<MessageState> outboundPendingMessages = new ArrayList<>();
    }

    @Data
    public static class MessageState {

        private int packetId;
        private String topic;
        private String payload;
        private int qos;
        private boolean retained;
        private String state;

        private static MessageState from(int packetId, MqttBrokerMessage message, String state, boolean retained) {
            MessageState messageState = new MessageState();
            messageState.setPacketId(packetId);
            messageState.setTopic(message.getTopic());
            messageState.setPayload(Base64.getEncoder().encodeToString(message.payload()));
            messageState.setQos(message.getQos().value());
            messageState.setRetained(retained);
            messageState.setState(state);
            return messageState;
        }

        private MqttBrokerMessage toBrokerMessage() {
            if (topic == null || topic.isBlank()) {
                return null;
            }
            byte[] body = payload == null || payload.isBlank() ? new byte[0] : Base64.getDecoder().decode(payload);
            return new MqttBrokerMessage(topic, body, MqttQoS.valueOf(qos), retained);
        }
    }

    /**
     * 集群确认上下文记录
     * 用于追踪集群间消息投递的确认状态
     *
     * @param requestId 请求ID，用于匹配请求和响应
     * @param sourceNodeId 源节点ID，标识请求来自哪个节点
     */
    public record ClusterAckContext(String requestId, String sourceNodeId) {
    }

    private enum OutboundPendingState {
        WAITING_FOR_PUBACK,
        WAITING_FOR_PUBREC,
        WAITING_FOR_PUBCOMP
    }

    private record OutboundPendingMessage(
            int packetId,
            MqttBrokerMessage message,
            MqttQoS qos,
            boolean retained,
            OutboundPendingState state) {

        private OutboundPendingMessage withState(OutboundPendingState state) {
            return new OutboundPendingMessage(packetId, message, qos, retained, state);
        }
    }

    private record QueuedPublishMessage(MqttBrokerMessage message, MqttQoS qos, boolean retained) {
    }
}