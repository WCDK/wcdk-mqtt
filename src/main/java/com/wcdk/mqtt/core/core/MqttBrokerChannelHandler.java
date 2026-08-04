package com.wcdk.mqtt.core.core;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.wcdk.mqtt.core.config.MqttBrokerProperties;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
public class MqttBrokerChannelHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final Logger log = LoggerFactory.getLogger(MqttBrokerChannelHandler.class);

    private final MqttBrokerProperties properties;

    private final MqttBrokerSessionRegistry sessionRegistry;

    private final MqttBrokerAcl mqttBrokerAcl;

    private final MqttBrokerClusterManager clusterManager;

    private MqttBrokerSession session;

    MqttBrokerChannelHandler(MqttBrokerProperties properties,
                             MqttBrokerSessionRegistry sessionRegistry,
                             MqttBrokerClusterManager clusterManager) {
        this.properties = properties;
        this.sessionRegistry = sessionRegistry;
        this.clusterManager = clusterManager;
        this.mqttBrokerAcl = new MqttBrokerAcl(properties);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage message) {
        if (!message.decoderResult().isSuccess()) {
            log.warn("MQTT 报文解码失败，远端地址={}", ctx.channel().remoteAddress(), message.decoderResult().cause());
            ctx.close();
            return;
        }

        try {
            MqttMessageType messageType = message.fixedHeader().messageType();
            switch (messageType) {
                case CONNECT:
                    handleConnect(ctx, (MqttConnectMessage) message);
                    break;
                case PUBLISH:
                    handlePublish(ctx, (MqttPublishMessage) message);
                    break;
                case PUBACK:
                    handlePubAck(message);
                    break;
                case PUBREC:
                    handlePubRec(ctx, message);
                    break;
                case PUBREL:
                    handlePubRel(ctx, message);
                    break;
                case PUBCOMP:
                    handlePubComp(message);
                    break;
                case SUBSCRIBE:
                    handleSubscribe(ctx, (MqttSubscribeMessage) message);
                    break;
                case UNSUBSCRIBE:
                    handleUnsubscribe(ctx, (MqttUnsubscribeMessage) message);
                    break;
                case PINGREQ:
                    ctx.writeAndFlush(MqttMessage.PINGRESP);
                    break;
                case DISCONNECT:
                    handleDisconnect(ctx);
                    break;
                default:
                    log.debug("不支持的 MQTT 报文类型={}，远端地址={}", messageType, ctx.channel().remoteAddress());
                    ctx.close();
                    break;
            }
        } catch (RuntimeException ex) {
            log.warn("MQTT 报文处理失败，远端地址={}", ctx.channel().remoteAddress(), ex);
            ctx.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        MqttBrokerSession inactiveSession = sessionRegistry.remove(ctx.channel());
        if (inactiveSession != null && clusterManager != null && clusterManager.isClusterEnabled()) {
            clusterManager.releaseClientSession(inactiveSession);
        }
        if (inactiveSession != null && !inactiveSession.isDisconnectedGracefully() && inactiveSession.willMessage() != null) {
            publish(inactiveSession.willMessage());
        }
        session = null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("MQTT 通道异常，远端地址={}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }

    private void handleConnect(ChannelHandlerContext ctx, MqttConnectMessage message) {
        if (session != null) {
            ctx.close();
            return;
        }

        MqttConnectVariableHeader variableHeader = message.variableHeader();
        MqttConnectPayload payload = message.payload();
        if (!isSupportedProtocol(variableHeader)) {
            reject(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);
            return;
        }
        if (!authenticate(payload)) {
            reject(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
            return;
        }

        String clientId = payload.clientIdentifier();
        if (!StringUtils.hasText(clientId)) {
            if (!variableHeader.isCleanSession()) {
                reject(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
                return;
            }
            clientId = "wcdk-mqtt-" + ctx.channel().id().asShortText();
        }
        if (clientId.length() > Math.max(1, properties.getMaxClientIdLength())) {
            reject(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
            return;
        }

        String username = payload.userName();
        MqttBrokerMessage willMessage = willMessage(variableHeader, payload);
        if (willMessage != null && !mqttBrokerAcl.canPublish(username, clientId, willMessage.getTopic())) {
            reject(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED);
            return;
        }

        if (clusterManager != null && clusterManager.isClusterEnabled() && !clusterManager.claimClientSession(clientId)) {
            reject(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
            return;
        }

        session = new MqttBrokerSession(ctx.channel(), clientId, variableHeader.isCleanSession(), username, willMessage);
        MqttBrokerSession currentSession = session;
        boolean sessionPresent = sessionRegistry.register(currentSession);
        if (clusterManager != null && clusterManager.isClusterEnabled()) {
            clusterManager.restoreSessionState(currentSession);
        }
        ctx.writeAndFlush(MqttMessageBuilders.connAck()
                .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                .sessionPresent(sessionPresent)
                .build()).addListener(future -> {
                    if (future.isSuccess()) {
                        currentSession.replayPendingMessages();
                        currentSession.drainQueuedPublishes();
                        if (clusterManager != null && clusterManager.isClusterEnabled()) {
                            clusterManager.replayOfflineMessages(currentSession);
                        }
                        sessionRegistry.notifySessionOnline(currentSession, sessionPresent);
                    }
                });
        log.info("MQTT 客户端已连接，客户端ID={}，清理会话={}，远端地址={}",
                clientId, variableHeader.isCleanSession(), ctx.channel().remoteAddress());
    }

    private void handlePublish(ChannelHandlerContext ctx, MqttPublishMessage message) {
        if (!ensureConnected(ctx)) {
            return;
        }
        String topic = message.variableHeader().topicName();
        if (!MqttTopicFilter.isValidTopicName(topic)) {
            ctx.close();
            return;
        }
        if (!mqttBrokerAcl.canPublish(session.username(), session.clientId(), topic)) {
            log.warn("MQTT 发布被 ACL 拒绝，客户端ID={}，主题={}", session.clientId(), topic);
            ctx.close();
            return;
        }

        MqttQoS qos = message.fixedHeader().qosLevel();
        int packetId = message.variableHeader().packetId();
        byte[] payload = ByteBufUtil.getBytes(message.payload());
        MqttBrokerMessage brokerMessage = new MqttBrokerMessage(topic, payload, qos, message.fixedHeader().isRetain());

        if (qos == MqttQoS.AT_MOST_ONCE) {
            session.recordPublicTopic(topic);
            publish(brokerMessage);
            sessionRegistry.notifySessionUpdated(session);
        } else if (qos == MqttQoS.AT_LEAST_ONCE) {
            session.recordPublicTopic(topic);
            publish(brokerMessage);
            sessionRegistry.notifySessionUpdated(session);
            ctx.writeAndFlush(MqttMessageBuilders.pubAck().packetId(packetId).build());
        } else if (qos == MqttQoS.EXACTLY_ONCE) {
            session.saveInboundQos2(packetId, brokerMessage);
            sessionRegistry.notifySessionUpdated(session);
            ctx.writeAndFlush(messageWithId(MqttMessageType.PUBREC, packetId));
        } else {
            ctx.close();
        }
    }

    private void publish(MqttBrokerMessage message) {
        if (clusterManager != null && clusterManager.isClusterEnabled()) {
            clusterManager.publish(message);
            return;
        }
        sessionRegistry.publish(message);
    }

    private void handleSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage message) {
        if (!ensureConnected(ctx)) {
            return;
        }

        int packetId = message.variableHeader().messageId();
        MqttMessageBuilders.SubAckBuilder subAckBuilder = MqttMessageBuilders.subAck().packetId(packetId);
        for (MqttTopicSubscription subscription : message.payload().topicSubscriptions()) {
            String topicFilter = subscription.topicFilter();
            MqttQoS requestedQos = normalizeSubscribeQos(subscription.qualityOfService());
            if (!MqttTopicFilter.isValidSubscriptionFilter(topicFilter) || requestedQos == MqttQoS.FAILURE) {
                subAckBuilder.addGrantedQos(MqttQoS.FAILURE);
                continue;
            }
            if (!mqttBrokerAcl.canSubscribe(session.username(), session.clientId(), topicFilter)) {
                log.warn("MQTT 订阅被 ACL 拒绝，客户端ID={}，主题过滤器={}", session.clientId(), topicFilter);
                subAckBuilder.addGrantedQos(MqttQoS.FAILURE);
                continue;
            }
            session.subscribe(topicFilter, requestedQos);
            subAckBuilder.addGrantedQos(requestedQos);
        }

        ctx.writeAndFlush(subAckBuilder.build()).addListener(future -> {
            if (!future.isSuccess()) {
                return;
            }
            sessionRegistry.notifySessionUpdated(session);
            for (MqttTopicSubscription subscription : message.payload().topicSubscriptions()) {
                String topicFilter = subscription.topicFilter();
                MqttQoS requestedQos = normalizeSubscribeQos(subscription.qualityOfService());
                if (MqttTopicFilter.isValidSubscriptionFilter(topicFilter)
                        && requestedQos != MqttQoS.FAILURE
                        && mqttBrokerAcl.canSubscribe(session.username(), session.clientId(), topicFilter)) {
                    sessionRegistry.sendRetained(session, topicFilter, requestedQos);
                    if (clusterManager != null && clusterManager.isClusterEnabled()) {
                        clusterManager.sendDistributedRetained(session, topicFilter, requestedQos);
                    }
                }
            }
        });
    }

    private void handleUnsubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage message) {
        if (!ensureConnected(ctx)) {
            return;
        }
        for (String topicFilter : message.payload().topics()) {
            session.unsubscribe(topicFilter);
        }
        ctx.writeAndFlush(MqttMessageBuilders.unsubAck()
                .packetId(message.variableHeader().messageId())
                .build()).addListener(future -> {
                    if (future.isSuccess()) {
                        sessionRegistry.notifySessionUpdated(session);
                    }
                });
    }

    private void handlePubAck(MqttMessage message) {
        if (session != null) {
            int packetId = packetId(message);
            MqttBrokerSession.ClusterAckContext ackContext = session.confirmOutboundQos1(packetId);
            if (clusterManager != null && clusterManager.isClusterEnabled()) {
                clusterManager.notifyClientAck(ackContext, session.clientId(), packetId, "PUBACK");
            }
            sessionRegistry.notifySessionUpdated(session);
        }
    }

    private void handlePubRec(ChannelHandlerContext ctx, MqttMessage message) {
        if (session != null) {
            int packetId = packetId(message);
            if (session.receiveOutboundQos2PubRec(packetId)) {
                sessionRegistry.notifySessionUpdated(session);
                ctx.writeAndFlush(messageWithId(MqttMessageType.PUBREL, packetId));
            }
        }
    }

    private void handlePubRel(ChannelHandlerContext ctx, MqttMessage message) {
        if (!ensureConnected(ctx)) {
            return;
        }
        int packetId = packetId(message);
        MqttBrokerMessage pendingMessage = session.removeInboundQos2(packetId);
        sessionRegistry.notifySessionUpdated(session);
        if (pendingMessage != null) {
            if (!mqttBrokerAcl.canPublish(session.username(), session.clientId(), pendingMessage.getTopic())) {
                ctx.close();
                return;
            }
            session.recordPublicTopic(pendingMessage.getTopic());
            publish(pendingMessage);
            sessionRegistry.notifySessionUpdated(session);
        }
        ctx.writeAndFlush(messageWithId(MqttMessageType.PUBCOMP, packetId));
    }

    private void handlePubComp(MqttMessage message) {
        if (session != null) {
            int packetId = packetId(message);
            MqttBrokerSession.ClusterAckContext ackContext = session.confirmOutboundQos2(packetId);
            if (clusterManager != null && clusterManager.isClusterEnabled()) {
                clusterManager.notifyClientAck(ackContext, session.clientId(), packetId, "PUBCOMP");
            }
            sessionRegistry.notifySessionUpdated(session);
        }
    }

    private void handleDisconnect(ChannelHandlerContext ctx) {
        if (session != null) {
            session.markDisconnectedGracefully();
        }
        ctx.close();
    }

    private boolean ensureConnected(ChannelHandlerContext ctx) {
        if (session != null) {
            return true;
        }
        ctx.close();
        return false;
    }

    private void reject(ChannelHandlerContext ctx, MqttConnectReturnCode returnCode) {
        ctx.writeAndFlush(MqttMessageBuilders.connAck()
                .returnCode(returnCode)
                .sessionPresent(false)
                .build()).addListener(ChannelFutureListener.CLOSE);
    }

    private boolean isSupportedProtocol(MqttConnectVariableHeader variableHeader) {
        return ("MQTT".equals(variableHeader.name()) && (variableHeader.version() == 4 || variableHeader.version() == 5))
                || ("MQIsdp".equals(variableHeader.name()) && variableHeader.version() == 3);
    }

    private boolean authenticate(MqttConnectPayload payload) {
        boolean usernameConfigured = StringUtils.hasText(properties.getUsername());
        boolean passwordConfigured = properties.getPassword() != null;
        if (usernameConfigured || passwordConfigured) {
            return Objects.equals(properties.getUsername(), payload.userName())
                    && Objects.equals(properties.getPassword(), password(payload));
        }
        return properties.isAnonymous();
    }

    private MqttBrokerMessage willMessage(MqttConnectVariableHeader variableHeader, MqttConnectPayload payload) {
        if (!variableHeader.isWillFlag() || !MqttTopicFilter.isValidTopicName(payload.willTopic())) {
            return null;
        }
        byte[] willPayload = payload.willMessageInBytes();
        return new MqttBrokerMessage(
                payload.willTopic(),
                willPayload,
                MqttQoS.valueOf(variableHeader.willQos()),
                variableHeader.isWillRetain());
    }

    private static String password(MqttConnectPayload payload) {
        byte[] password = payload.passwordInBytes();
        return password == null ? null : new String(password, StandardCharsets.UTF_8);
    }

    private static MqttQoS normalizeSubscribeQos(MqttQoS qos) {
        if (qos == MqttQoS.AT_MOST_ONCE || qos == MqttQoS.AT_LEAST_ONCE || qos == MqttQoS.EXACTLY_ONCE) {
            return qos;
        }
        return MqttQoS.FAILURE;
    }

    private static int packetId(MqttMessage message) {
        return ((MqttMessageIdVariableHeader) message.variableHeader()).messageId();
    }

    private static MqttMessage messageWithId(MqttMessageType messageType, int packetId) {
        MqttQoS qos = messageType == MqttMessageType.PUBREL ? MqttQoS.AT_LEAST_ONCE : MqttQoS.AT_MOST_ONCE;
        return MqttMessageFactory.newMessage(
                new MqttFixedHeader(messageType, false, qos, false, 2),
                MqttMessageIdVariableHeader.from(packetId),
                null);
    }
}
