package com.wcdk.mqtt.core.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.wcdk.mqtt.core.config.MqttBrokerProperties;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.handler.codec.mqtt.MqttQoS;
/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
public class MqttBrokerSessionRegistry {

    private final MqttBrokerProperties properties;

    private final MqttBrokerAcl mqttBrokerAcl;

    private final List<MqttBrokerPublishListener> publishListeners;

    private final List<MqttBrokerSessionListener> sessionListeners;

    private final ConcurrentMap<String, MqttBrokerSession> sessionsByClientId = new ConcurrentHashMap<>();

    private final ConcurrentMap<ChannelId, MqttBrokerSession> sessionsByChannelId = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, MqttBrokerMessage> retainedMessages = new ConcurrentHashMap<>();

    public MqttBrokerSessionRegistry(MqttBrokerProperties properties) {
        this(properties, new MqttBrokerAcl(properties), List.of(), List.of());
    }

    public MqttBrokerSessionRegistry(MqttBrokerProperties properties, List<MqttBrokerPublishListener> publishListeners) {
        this(properties, new MqttBrokerAcl(properties), publishListeners, List.of());
    }

    public MqttBrokerSessionRegistry(MqttBrokerProperties properties,
                                     MqttBrokerAcl mqttBrokerAcl,
                                     List<MqttBrokerPublishListener> publishListeners,
                                     List<MqttBrokerSessionListener> sessionListeners) {
        this.properties = properties;
        this.mqttBrokerAcl = mqttBrokerAcl == null ? new MqttBrokerAcl(properties) : mqttBrokerAcl;
        this.publishListeners = publishListeners == null ? List.of() : List.copyOf(publishListeners);
        this.sessionListeners = sessionListeners == null ? List.of() : List.copyOf(sessionListeners);
    }

    public boolean register(MqttBrokerSession session) {
        boolean sessionPresent = false;
        MqttBrokerSession previousSession = sessionsByClientId.get(session.clientId());
        if (previousSession != null && previousSession != session && !previousSession.cleanSession() && !session.cleanSession()) {
            session.restoreStateFrom(previousSession);
            sessionPresent = true;
        }
        MqttBrokerSession oldSession = sessionsByClientId.put(session.clientId(), session);
        if (oldSession != null && oldSession != session) {
            oldSession.markDisconnectedGracefully();
            Channel oldChannel = oldSession.channel();
            if (oldChannel != null) {
                sessionsByChannelId.remove(oldChannel.id(), oldSession);
            }
            oldSession.close();
        }
        sessionsByChannelId.put(session.channel().id(), session);
        return sessionPresent;
    }

    public MqttBrokerSession remove(Channel channel) {
        MqttBrokerSession session = sessionsByChannelId.remove(channel.id());
        if (session != null) {
            session.detach();
            if (session.cleanSession()) {
                sessionsByClientId.remove(session.clientId(), session);
            }
            notifySessionOffline(session);
        }
        return session;
    }

    public Collection<MqttBrokerSession> sessions() {
        return sessionsByClientId.values();
    }

    public Optional<MqttBrokerSession> findByClientId(String clientId) {
        return Optional.ofNullable(sessionsByClientId.get(clientId));
    }

    public Optional<MqttBrokerSession> findBySessionUrl(String sessionUrl) {
        if (sessionUrl == null || sessionUrl.isBlank()) {
            return Optional.empty();
        }
        String target = sessionUrl.trim();
        return sessions().stream()
                .filter(session -> session.sessionUrl() != null)
                .filter(session -> target.equals(session.sessionUrl()))
                .findFirst();
    }

    public void notifySessionOnline(MqttBrokerSession session, boolean sessionPresent) {
        for (MqttBrokerSessionListener sessionListener : sessionListeners) {
            sessionListener.onSessionOnline(session, sessionPresent);
        }
    }

    public void notifySessionUpdated(MqttBrokerSession session) {
        for (MqttBrokerSessionListener sessionListener : sessionListeners) {
            sessionListener.onSessionUpdated(session);
        }
    }

    public void notifySessionOffline(MqttBrokerSession session) {
        for (MqttBrokerSessionListener sessionListener : sessionListeners) {
            sessionListener.onSessionOffline(session);
        }
    }

    public void publish(MqttBrokerMessage message) {
        if (properties.isRetainedMessages() && message.isRetained()) {
            retain(message);
        }
        for (MqttBrokerPublishListener publishListener : publishListeners) {
            publishListener.onPublish(message);
        }
        for (MqttBrokerSession session : sessions()) {
            OptionalInt subscriptionQos = session.maxSubscriptionQos(message.getTopic());
            if (subscriptionQos.isPresent()
                    && mqttBrokerAcl.canSubscribe(session.username(), session.clientId(), message.getTopic())) {
                MqttQoS deliveryQos = lowerQos(message.getQos(), subscriptionQos.getAsInt());
                if (session.isActive() || (!session.cleanSession() && deliveryQos != MqttQoS.AT_MOST_ONCE)) {
                    session.sendPublish(message, deliveryQos, false);
                    notifySessionUpdated(session);
                }
            }
        }
    }

    public void sendRetained(MqttBrokerSession session, String topicFilter, MqttQoS subscriptionQos) {
        if (!properties.isRetainedMessages()) {
            return;
        }
        for (MqttBrokerMessage retainedMessage : retainedMessages(topicFilter)) {
            if (!mqttBrokerAcl.canSubscribe(session.username(), session.clientId(), retainedMessage.getTopic())) {
                continue;
            }
            MqttQoS deliveryQos = lowerQos(retainedMessage.getQos(), subscriptionQos.value());
            session.sendPublish(retainedMessage, deliveryQos, true);
            notifySessionUpdated(session);
        }
    }

    private List<MqttBrokerMessage> retainedMessages(String topicFilter) {
        List<MqttBrokerMessage> messages = new ArrayList<>();
        for (MqttBrokerMessage message : retainedMessages.values()) {
            if (MqttTopicFilter.matches(topicFilter, message.getTopic())) {
                messages.add(message);
            }
        }
        return messages;
    }

    private void retain(MqttBrokerMessage message) {
        if (message.payload().length == 0) {
            retainedMessages.remove(message.getTopic());
            return;
        }
        retainedMessages.put(message.getTopic(), message.asRetained());
    }

    private static MqttQoS lowerQos(MqttQoS publishQos, int subscriptionQos) {
        return MqttQoS.valueOf(Math.min(publishQos.value(), subscriptionQos));
    }
}
