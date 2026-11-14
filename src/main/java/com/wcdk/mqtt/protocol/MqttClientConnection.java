package com.wcdk.mqtt.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.core.MqttBrokerMessage;
import com.wcdk.mqtt.core.MqttConnectionHandle;
import com.wcdk.mqtt.core.MqttSession;
import com.wcdk.mqtt.core.MqttSessionRegistry;
import com.wcdk.mqtt.core.MqttTopicMatcher;
import com.wcdk.mqtt.service.MqttBrokerService;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.util.concurrent.Queues;

import static io.netty.handler.codec.mqtt.MqttProperties.NO_PROPERTIES;

/***
* 单个 MQTT 客户端连接的协议处理器，负责报文解析、状态机推进与会话绑定。
* @author wcdk
***/
public class MqttClientConnection implements MqttConnectionHandle {

    private static final Logger log = LoggerFactory.getLogger(MqttClientConnection.class);

    /** CONNECT 拒绝原因：协议版本不支持 */
    private static final MqttConnectReturnCode REFUSED_PROTOCOL =
            MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;
    /** CONNECT 拒绝原因：客户端 ID 不合法 */
    private static final MqttConnectReturnCode REFUSED_IDENTIFIER =
            MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
    /** CONNECT 拒绝原因：用户名或密码错误 */
    private static final MqttConnectReturnCode REFUSED_BAD_CREDENTIALS =
            MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD;
    /** CONNECT 拒绝原因：未授权 */
    private static final MqttConnectReturnCode REFUSED_NOT_AUTHORIZED =
            MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED;

    private final Connection connection;
    private final MqttSessionRegistry registry;
    private final MqttBrokerService brokerService;
    private final MqttProperties properties;
    private final String nodeId;
    private final String remoteAddress;
    /** 出站汇聚点：有界缓冲，防止慢消费者拖垮堆内存 */
    private final Sinks.Many<MqttMessage> outboundSink;

    /** 当前绑定的会话，CONNECT 成功后非空 */
    private volatile MqttSession session;
    /** 是否已完成 CONNECT 握手 */
    private volatile boolean connected;
    /** 是否收到过规范的 DISCONNECT 报文 */
    private volatile boolean gracefulDisconnect;
    /** 是否已主动关闭（完成出站汇聚或直接断开） */
    private volatile boolean closed;
    /** 客户端协议级别：3 / 4 / 5 */
    private volatile int protocolLevel = 4;

    public MqttClientConnection(Connection connection,
                                MqttSessionRegistry registry,
                                MqttBrokerService brokerService,
                                MqttProperties properties,
                                String nodeId) {
        this.connection = connection;
        this.registry = registry;
        this.brokerService = brokerService;
        this.properties = properties;
        this.nodeId = nodeId;
        // 有界队列兜底：写满即判定慢消费者，防止出站缓冲无限膨胀拖垮堆内存
        this.outboundSink = Sinks.many().unicast().onBackpressureBuffer(
                Queues.<MqttMessage>get(Math.max(64, properties.getBroker().getMaxOutboundBuffer())).get());
        this.remoteAddress = connection.channel().remoteAddress() == null
                ? "unknown"
                : connection.channel().remoteAddress().toString();
    }

    /***
    * 启动本连接的读写链路，读写全部结束后清理会话并关闭连接。
    * @author wcdk
    * @param inbound 入站通道
    * @param outbound 出站通道
    * @return 读写组合 Mono
    ***/
    public Mono<Void> run(NettyInbound inbound, NettyOutbound outbound) {
        // 所有下行报文经汇聚点串行写出，保证报文顺序
        Mono<Void> writeMono = outbound.sendObject(outboundSink.asFlux())
                .then()
                .doFinally(signalType -> {
                    // 出站通道结束意味着报文已刷出或通道已死，此时关闭连接
                    disposeQuietly();
                });
        Mono<Void> readMono = inbound.receiveObject()
                .ofType(MqttMessage.class)
                .doOnNext(this::handleMessage)
                .doFinally(signalType -> {
                    // 入站结束后完成出站汇聚点，让待刷报文写完后再关连接
                    outboundSink.tryEmitComplete();
                    cleanup();
                })
                .then();
        return Mono.when(writeMono, readMono);
    }

    /***
    * 分发处理单条 MQTT 报文，所有处理均为同步状态机，不阻塞事件循环。
    * @author wcdk
    * @param message 解码后的报文
    ***/
    private void handleMessage(MqttMessage message) {
        if (message.decoderResult() != null && !message.decoderResult().isSuccess()) {
            log.warn("收到解码失败的报文，远端地址：{}，异常信息：{}",
                    remoteAddress, message.decoderResult().cause().getMessage());
            close();
            return;
        }
        // 收到任何合规报文都刷新活跃时间：持续发布但不发 PINGREQ 的客户端同样视为存活
        MqttSession active = session;
        if (active != null) {
            active.touch();
        }
        switch (message.fixedHeader().messageType()) {
            case CONNECT -> handleConnect((MqttConnectMessage) message);
            case PUBLISH -> handlePublish((MqttPublishMessage) message);
            case PUBACK -> handlePubAck(message);
            case PUBREC -> handlePubRec(message);
            case PUBREL -> handlePubRel(message);
            case PUBCOMP -> handlePubComp(message);
            case SUBSCRIBE -> handleSubscribe((MqttSubscribeMessage) message);
            case UNSUBSCRIBE -> handleUnsubscribe((MqttUnsubscribeMessage) message);
            case PINGREQ -> handlePingReq();
            case DISCONNECT -> handleDisconnect(message);
            default -> {
                log.warn("收到未预期的报文类型：{}，远端地址：{}",
                        message.fixedHeader().messageType(), remoteAddress);
                close();
            }
        }
    }

    /***
    * 处理 CONNECT 报文：协议校验、认证、遗嘱、会话注册与 CONNACK 应答。
    * @author wcdk
    * @param message CONNECT 报文
    ***/
    private void handleConnect(MqttConnectMessage message) {
        if (connected) {
            log.warn("连接重复发送 CONNECT 报文，远端地址：{}，已断开", remoteAddress);
            close();
            return;
        }
        MqttConnectVariableHeader variableHeader = message.variableHeader();
        MqttConnectPayload payload = message.payload();

        // 校验协议名与协议级别，仅支持 MQTT 3.1 / 3.1.1 / 5.0
        if (!isSupportedProtocol(variableHeader)) {
            log.warn("拒绝不支持的协议版本，远端地址：{}，协议名：{}，协议级别：{}",
                    remoteAddress, variableHeader.name(), variableHeader.version());
            sendConnAck(REFUSED_PROTOCOL, false);
            completeOutboundAndClose();
            return;
        }
        this.protocolLevel = variableHeader.version();
        // MQTT5 会话过期秒数：缺省 0（断开即结束会话），0xFFFFFFFF 归一化为永不过期；
        // 旧协议无此概念，持久会话沿用不过期语义
        long sessionExpirySeconds = protocolLevel >= 5
                ? sessionExpiryOrDefault(variableHeader.properties())
                : Long.MAX_VALUE;

        // 校验客户端 ID，空 ID 由服务端分配
        boolean cleanSession = variableHeader.isCleanSession();
        String clientId = payload.clientIdentifier() == null ? "" : payload.clientIdentifier().trim();
        if (clientId.isEmpty()) {
            if (!cleanSession && protocolLevel < 5) {
                log.warn("旧版协议要求持久会话必须携带客户端 ID，远端地址：{}", remoteAddress);
                sendConnAck(REFUSED_IDENTIFIER, false);
                completeOutboundAndClose();
                return;
            }
            clientId = "auto-" + UUID.randomUUID();
        }
        if (clientId.length() > properties.getBroker().getMaxClientIdLength()) {
            log.warn("客户端 ID 超长被拒绝，客户端ID：{}，远端地址：{}", clientId, remoteAddress);
            sendConnAck(REFUSED_IDENTIFIER, false);
            completeOutboundAndClose();
            return;
        }

        // 认证：配置了用户名则必须匹配，未配置时按是否允许匿名决定
        String username = variableHeader.hasUserName() ? payload.userName() : null;
        String password = variableHeader.hasPassword() ? payload.password() : null;
        if (!authenticate(username, password)) {
            log.warn("客户端认证失败被拒绝，客户端ID：{}，用户名：{}，远端地址：{}",
                    clientId, username, remoteAddress);
            sendConnAck(StringUtils.hasText(username) ? REFUSED_BAD_CREDENTIALS : REFUSED_NOT_AUTHORIZED, false);
            completeOutboundAndClose();
            return;
        }

        // 构造遗嘱消息，主题非法或无发布权限时拒绝连接
        MqttBrokerMessage willMessage = null;
        if (variableHeader.isWillFlag()) {
            String willTopic = payload.willTopic();
            byte[] willPayload = payload.willMessageInBytes() == null ? new byte[0] : payload.willMessageInBytes();
            if (!MqttTopicMatcher.isValidTopicName(willTopic)) {
                log.warn("遗嘱主题非法被拒绝，客户端ID：{}，遗嘱主题：{}", clientId, willTopic);
                sendConnAck(REFUSED_PROTOCOL, false);
                completeOutboundAndClose();
                return;
            }
            if (!registry.acl().canPublish(username, clientId, willTopic)) {
                log.warn("遗嘱发布权限不足被拒绝，客户端ID：{}，遗嘱主题：{}", clientId, willTopic);
                sendConnAck(REFUSED_NOT_AUTHORIZED, false);
                completeOutboundAndClose();
                return;
            }
            willMessage = new MqttBrokerMessage(null, willTopic, willPayload,
                    variableHeader.willQos(), variableHeader.isWillRetain());
        }

        // 注册会话，同 ID 重连时由注册表处理旧连接接管与状态恢复
        MqttSession newSession = new MqttSession(clientId, cleanSession, username, nodeId);
        newSession.setProtocolLevel(protocolLevel);
        newSession.setSessionExpirySeconds(sessionExpirySeconds);
        newSession.setKeepAliveSeconds(variableHeader.keepAliveTimeSeconds());
        newSession.setWillMessage(willMessage);
        newSession.setMaxOfflineCapacity(properties.getBroker().getMaxOfflineMessages());
        // 先绑定连接再注册：注册完成时的会话变更通知需要感知在线状态，
        // 集群据此立即向对等节点发送 SESSION_CLAIM 声明归属
        this.session = newSession;
        newSession.attach(this);
        boolean sessionPresent = registry.register(newSession);
        this.connected = true;

        // 归属复核：极端并发下若本会话已被更新连接接管，立即终止当前连接，避免同名双在线
        if (registry.findByClientId(clientId) != newSession) {
            log.warn("会话在握手期间已被其他连接接管，终止当前连接，客户端ID：{}，远端地址：{}",
                    clientId, remoteAddress);
            close();
            return;
        }

        sendConnAck(MqttConnectReturnCode.CONNECTION_ACCEPTED, sessionPresent && !cleanSession);
        // 按规范在 CONNACK 之后重放未确认的 QoS 1/2 消息与离线队列
        newSession.replayPendingMessages();
        newSession.drainOfflineQueue();
        log.info("客户端连接成功，客户端ID：{}，协议级别：{}，清理会话：{}，会话恢复：{}，KeepAlive：{}秒，远端地址：{}",
                clientId, protocolLevel, cleanSession, sessionPresent,
                variableHeader.keepAliveTimeSeconds(), remoteAddress);
    }

    /***
    * 校验 CONNECT 携带的协议名与协议级别是否受支持。
    * @author wcdk
    * @param variableHeader 可变头
    * @return 受支持返回 true
    ***/
    private boolean isSupportedProtocol(MqttConnectVariableHeader variableHeader) {
        String name = variableHeader.name();
        int version = variableHeader.version();
        return ("MQTT".equals(name) && (version == 4 || version == 5))
                || ("MQIsdp".equals(name) && version == 3);
    }

    /***
    * 读取 MQTT5 CONNECT 的 Session Expiry Interval 属性，未携带按规范取 0。
    * @author wcdk
    * @param nettyProperties Netty 解码出的属性集合
    * @return 会话过期秒数
    ***/
    private long sessionExpiryOrDefault(io.netty.handler.codec.mqtt.MqttProperties nettyProperties) {
        Long value = readSessionExpiry(nettyProperties);
        return value == null ? 0 : value;
    }

    /***
    * 读取 Session Expiry Interval 属性，0xFFFFFFFF 按规范归一化为永不过期。
    * @author wcdk
    * @param nettyProperties Netty 解码出的属性集合
    * @return 会话过期秒数，属性未携带返回 null
    ***/
    private Long readSessionExpiry(io.netty.handler.codec.mqtt.MqttProperties nettyProperties) {
        if (nettyProperties == null) {
            return null;
        }
        io.netty.handler.codec.mqtt.MqttProperties.MqttProperty<?> property = nettyProperties.getProperty(
                io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value());
        if (!(property instanceof io.netty.handler.codec.mqtt.MqttProperties.IntegerProperty intProperty)) {
            return null;
        }
        long value = intProperty.value();
        // 0xFFFFFFFF 表示会话永不过期
        return value == 0xFFFF_FFFFL ? Long.MAX_VALUE : Math.max(0, value);
    }

    /***
    * 执行用户名密码认证，未开启认证时放行。
    * @author wcdk
    * @param username 用户名
    * @param password 密码
    * @return 认证通过返回 true
    ***/
    private boolean authenticate(String username, String password) {
        MqttProperties.Security security = properties.getSecurity();
        if (!StringUtils.hasText(security.getUsername())) {
            // 未配置凭据：仅在允许匿名时放行
            return security.isAnonymous();
        }
        if (!security.getUsername().equals(username)) {
            return false;
        }
        String configuredPassword = security.getPassword();
        return !StringUtils.hasText(configuredPassword) || configuredPassword.equals(password);
    }

    /***
    * 处理 PUBLISH 报文，按 QoS 分别进入直发、PUBACK 或两阶段确认流程。
    * @author wcdk
    * @param message PUBLISH 报文
    ***/
    private void handlePublish(MqttPublishMessage message) {
        MqttSession current = requireSession();
        if (current == null) {
            return;
        }
        String topic = message.variableHeader().topicName();
        int qos = message.fixedHeader().qosLevel().value();
        boolean duplicate = message.fixedHeader().isDup();
        boolean retained = message.fixedHeader().isRetain();
        int packetId = qos > 0 ? message.variableHeader().packetId() : 0;
        byte[] body = readPayload(message);

        if (!MqttTopicMatcher.isValidTopicName(topic)) {
            log.warn("收到非法主题的发布报文，客户端ID：{}，主题：{}，已断开",
                    current.clientId(), topic);
            close();
            return;
        }
        if (qos > 0 && packetId <= 0) {
            log.warn("QoS 大于 0 的发布报文缺少包 ID，客户端ID：{}，主题：{}，已断开",
                    current.clientId(), topic);
            close();
            return;
        }

        // 访问控制：拒绝时仍按 QoS 回执，避免客户端无限重传
        if (!registry.acl().canPublish(current.username(), current.clientId(), topic)) {
            log.warn("客户端发布被访问控制拒绝，客户端ID：{}，主题：{}", current.clientId(), topic);
            if (qos == 1) {
                ackPublish(1, packetId, (byte) 0x87);
            } else if (qos == 2) {
                // 不暂存被拒绝的消息，PUBREL 到达时 removeInboundQos2 返回空，仅回 PUBCOMP
                emit(new MqttMessage(new MqttFixedHeader(
                        io.netty.handler.codec.mqtt.MqttMessageType.PUBREC, false,
                        MqttQoS.AT_LEAST_ONCE, false, 0),
                        new MqttPubReplyMessageVariableHeader(packetId, (byte) 0x87, NO_PROPERTIES)));
            }
            return;
        }

        MqttBrokerMessage brokerMessage = new MqttBrokerMessage(null, topic, body, qos, retained);
        current.recordPublishedTopic(topic);
        switch (qos) {
            case 0 -> brokerService.publish(brokerMessage);
            case 1 -> {
                if (!current.isInboundQos1Duplicate(packetId, duplicate)) {
                    brokerService.publish(brokerMessage);
                } else {
                    log.debug("QoS 1 重复发布报文被去重，客户端ID：{}，包ID：{}", current.clientId(), packetId);
                }
                ackPublish(1, packetId, (byte) 0);
            }
            case 2 -> {
                // 保存消息等待 PUBREL，重复的 PUBREC 仅重发回执
                current.saveInboundQos2(packetId, brokerMessage);
                emit(new MqttMessage(new MqttFixedHeader(
                        io.netty.handler.codec.mqtt.MqttMessageType.PUBREC, false,
                        MqttQoS.AT_LEAST_ONCE, false, 0),
                        new MqttPubReplyMessageVariableHeader(packetId, (byte) 0, NO_PROPERTIES)));
            }
            default -> log.warn("不支持的 QoS：{}，客户端ID：{}", qos, current.clientId());
        }
    }

    /***
    * 读取 PUBLISH 报文负载字节。
    * @author wcdk
    * @param message PUBLISH 报文
    * @return 负载字节数组
    ***/
    private byte[] readPayload(MqttPublishMessage message) {
        int readable = message.payload().readableBytes();
        if (readable <= 0) {
            return new byte[0];
        }
        byte[] body = new byte[readable];
        message.payload().getBytes(message.payload().readerIndex(), body);
        return body;
    }

    /***
    * 按 QoS 与协议版本发送 PUBLISH 的确认回执。
    * @author wcdk
    * @param qos QoS
    * @param packetId 包 ID
    * @param reasonCode v5 原因码，v3.1.1 忽略
    ***/
    private void ackPublish(int qos, int packetId, byte reasonCode) {
        if (qos == 0) {
            return;
        }
        if (qos == 1) {
            emit(new MqttMessage(new MqttFixedHeader(
                    io.netty.handler.codec.mqtt.MqttMessageType.PUBACK, false,
                    MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttPubReplyMessageVariableHeader(packetId, reasonCode, NO_PROPERTIES)));
            return;
        }
        emit(new MqttMessage(new MqttFixedHeader(
                io.netty.handler.codec.mqtt.MqttMessageType.PUBCOMP, false,
                MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttPubReplyMessageVariableHeader(packetId, reasonCode, NO_PROPERTIES)));
    }

    /***
    * 处理 PUBACK，确认 QoS 1 出站消息。
    * @author wcdk
    * @param message PUBACK 报文
    ***/
    private void handlePubAck(MqttMessage message) {
        MqttSession current = requireSession();
        if (current == null || !(message.variableHeader() instanceof MqttMessageIdVariableHeader header)) {
            return;
        }
        MqttBrokerMessage confirmed = current.confirmOutboundQos1(header.messageId());
        if (confirmed != null) {
            log.debug("QoS 1 消息已确认，客户端ID：{}，包ID：{}，主题：{}",
                    current.clientId(), header.messageId(), confirmed.topic());
        }
    }

    /***
    * 处理 PUBREC，收到后回发 PUBREL 推进 QoS 2 第二阶段。
    * @author wcdk
    * @param message PUBREC 报文
    ***/
    private void handlePubRec(MqttMessage message) {
        MqttSession current = requireSession();
        if (current == null || !(message.variableHeader() instanceof MqttMessageIdVariableHeader header)) {
            return;
        }
        if (current.onOutboundQos2PubRec(header.messageId())) {
            sendPubRel(header.messageId());
        }
    }

    /***
    * 处理 PUBREL，取出入站 QoS 2 消息正式发布并回发 PUBCOMP。
    * @author wcdk
    * @param message PUBREL 报文
    ***/
    private void handlePubRel(MqttMessage message) {
        MqttSession current = requireSession();
        if (current == null || !(message.variableHeader() instanceof MqttMessageIdVariableHeader header)) {
            return;
        }
        MqttBrokerMessage stored = current.removeInboundQos2(header.messageId());
        if (stored != null) {
            brokerService.publish(stored);
        }
        emit(new MqttMessage(new MqttFixedHeader(
                io.netty.handler.codec.mqtt.MqttMessageType.PUBCOMP, false,
                MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttPubReplyMessageVariableHeader(header.messageId(), (byte) 0, NO_PROPERTIES)));
    }

    /***
    * 处理 PUBCOMP，确认 QoS 2 出站消息。
    * @author wcdk
    * @param message PUBCOMP 报文
    ***/
    private void handlePubComp(MqttMessage message) {
        MqttSession current = requireSession();
        if (current == null || !(message.variableHeader() instanceof MqttMessageIdVariableHeader header)) {
            return;
        }
        MqttBrokerMessage confirmed = current.confirmOutboundQos2(header.messageId());
        if (confirmed != null) {
            log.debug("QoS 2 消息已确认，客户端ID：{}，包ID：{}，主题：{}",
                    current.clientId(), header.messageId(), confirmed.topic());
        }
    }

    /***
    * 处理 SUBSCRIBE，逐条做主题与 ACL 校验后回发 SUBACK 并补发保留消息。
    * @author wcdk
    * @param message SUBSCRIBE 报文
    ***/
    private void handleSubscribe(MqttSubscribeMessage message) {
        MqttSession current = requireSession();
        if (current == null) {
            return;
        }
        List<MqttTopicSubscription> subscriptions = message.payload().topicSubscriptions();
        if (subscriptions == null || subscriptions.isEmpty()) {
            log.warn("收到空订阅列表，客户端ID：{}，已断开", current.clientId());
            close();
            return;
        }
        int packetId = messageIdOf(message);
        if (packetId <= 0) {
            log.warn("订阅报文缺少包 ID，客户端ID：{}，已断开", current.clientId());
            close();
            return;
        }
        var subAckBuilder = MqttMessageBuilders.subAck()
                .packetId(packetId)
                .properties(NO_PROPERTIES);
        List<String> grantedFilters = new ArrayList<>();
        for (MqttTopicSubscription subscription : subscriptions) {
            String topicFilter = subscription.topicName();
            int requestedQos = subscription.qualityOfService().value();
            if (!MqttTopicMatcher.isValidTopicFilter(topicFilter)
                    || requestedQos < 0 || requestedQos > 2
                    || !registry.acl().canSubscribe(current.username(), current.clientId(), topicFilter)) {
                log.warn("订阅被拒绝，客户端ID：{}，主题过滤器：{}", current.clientId(), topicFilter);
                subAckBuilder.addGrantedQos(MqttQoS.FAILURE);
                continue;
            }
            current.subscribe(topicFilter, requestedQos);
            grantedFilters.add(topicFilter);
            subAckBuilder.addGrantedQos(MqttQoS.valueOf(requestedQos));
            // 按规范向新订阅补发匹配的保留消息
            registry.sendRetained(current, topicFilter, requestedQos);
        }
        emit(subAckBuilder.build());
        if (!grantedFilters.isEmpty()) {
            registry.notifySessionChanged(current);
            log.info("客户端完成订阅，客户端ID：{}，订阅主题：{}", current.clientId(), grantedFilters);
        }
    }

    /***
    * 提取带包 ID 报文的包 ID。
    * @author wcdk
    * @param message 报文
    * @return 包 ID
    ***/
    private int messageIdOf(MqttMessage message) {
        return message.variableHeader() instanceof MqttMessageIdVariableHeader header
                ? header.messageId()
                : 0;
    }

    /***
    * 处理 UNSUBSCRIBE，移除订阅并回发 UNSUBACK。
    * @author wcdk
    * @param message UNSUBSCRIBE 报文
    ***/
    private void handleUnsubscribe(MqttUnsubscribeMessage message) {
        MqttSession current = requireSession();
        if (current == null) {
            return;
        }
        int packetId = messageIdOf(message);
        if (packetId <= 0) {
            close();
            return;
        }
        List<String> topics = message.payload().topics();
        if (topics != null) {
            for (String topicFilter : topics) {
                current.unsubscribe(topicFilter);
            }
        }
        MqttUnsubAckMessage unsubAck;
        if (protocolLevel >= 5) {
            var builder = MqttMessageBuilders.unsubAck()
                    .packetId(packetId)
                    .properties(NO_PROPERTIES);
            int count = topics == null ? 0 : topics.size();
            for (int i = 0; i < count; i++) {
                builder.addReasonCode((short) 0);
            }
            unsubAck = builder.build();
        } else {
            // v3.1.1 的 UNSUBACK 仅含包 ID，必须使用普通可变头避免多写原因码
            unsubAck = new MqttUnsubAckMessage(
                    new MqttFixedHeader(io.netty.handler.codec.mqtt.MqttMessageType.UNSUBACK, false,
                            MqttQoS.AT_MOST_ONCE, false, 0),
                    MqttMessageIdVariableHeader.from(packetId));
        }
        emit(unsubAck);
        registry.notifySessionChanged(current);
    }

    /***
    * 处理 PINGREQ，回发 PINGRESP。
    * @author wcdk
    ***/
    private void handlePingReq() {
        MqttSession current = session;
        if (current != null) {
            current.touch();
        }
        emit(MqttMessage.PINGRESP);
    }

    /***
    * 处理 DISCONNECT，标记优雅断开后关闭连接，不发布遗嘱。
    * @author wcdk
    * @param message DISCONNECT 报文
    ***/
    private void handleDisconnect(MqttMessage message) {
        MqttSession current = session;
        if (current != null) {
            current.touch();
            // MQTT5 允许在 DISCONNECT 中改写会话过期时间，未携带则沿用 CONNECT 的取值
            if (protocolLevel >= 5 && message.variableHeader()
                    instanceof io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader header) {
                Long expiry = readSessionExpiry(header.properties());
                if (expiry != null) {
                    current.setSessionExpirySeconds(expiry);
                }
            }
        }
        this.gracefulDisconnect = true;
        if (current != null) {
            current.markGracefulDisconnect();
        }
        log.info("客户端正常断开连接，客户端ID：{}，远端地址：{}",
                current == null ? null : current.clientId(), remoteAddress);
        // 完成出站汇聚点，刷完剩余报文后由写链路关闭连接
        outboundSink.tryEmitComplete();
    }

    /***
    * 取出已连接的会话，未完成握手时记录告警并拒绝后续报文。
    * @author wcdk
    * @return 会话，未连接返回 null
    ***/
    private MqttSession requireSession() {
        MqttSession current = session;
        if (current == null || !connected) {
            log.warn("未完成 CONNECT 握手即收到业务报文，远端地址：{}，已断开", remoteAddress);
            close();
            return null;
        }
        return current;
    }

    /***
    * 发送 CONNACK 报文。
    * @author wcdk
    * @param returnCode 返回码
    * @param sessionPresent 会话是否恢复
    ***/
    private void sendConnAck(MqttConnectReturnCode returnCode, boolean sessionPresent) {
        emit(MqttMessageBuilders.connAck()
                .returnCode(returnCode)
                .sessionPresent(sessionPresent)
                .properties(NO_PROPERTIES)
                .build());
    }

    /***
    * 连接终止清理：解绑会话、按需发布遗嘱。
    * @author wcdk
    ***/
    private void cleanup() {
        MqttSession current = this.session;
        if (current == null) {
            return;
        }
        this.session = null;
        // 无论会话是否已被接管，都要解绑本连接持有的旧会话句柄
        current.detach();
        registry.remove(current);
        // 非优雅断开时发布遗嘱消息（含被新连接接管的场景）
        if (!gracefulDisconnect) {
            MqttBrokerMessage will = current.willMessage();
            if (will != null) {
                current.setWillMessage(null);
                log.info("连接异常断开，发布遗嘱消息，客户端ID：{}，遗嘱主题：{}",
                        current.clientId(), will.topic());
                brokerService.publish(will);
            }
        }
        log.info("客户端连接已释放，客户端ID：{}，远端地址：{}", current.clientId(), remoteAddress);
    }

    @Override
    public boolean send(MqttBrokerMessage message, int qos, boolean retained, int packetId, boolean duplicate) {
        if (closed || !isActive()) {
            return false;
        }
        MqttQoS qosLevel = switch (qos) {
            case 1 -> MqttQoS.AT_LEAST_ONCE;
            case 2 -> MqttQoS.EXACTLY_ONCE;
            default -> MqttQoS.AT_MOST_ONCE;
        };
        MqttFixedHeader header = new MqttFixedHeader(
                io.netty.handler.codec.mqtt.MqttMessageType.PUBLISH, duplicate, qosLevel, retained, 0);
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(
                message.topic(), packetId, NO_PROPERTIES);
        MqttPublishMessage publish = new MqttPublishMessage(
                header, variableHeader, Unpooled.wrappedBuffer(message.payload()));
        boolean emitted = emit(publish);
        if (!emitted) {
            // 未能入队的报文需要手动释放，避免堆外内存泄漏
            publish.release();
        }
        return emitted;
    }

    @Override
    public void sendPubRel(int packetId) {
        emit(new MqttMessage(new MqttFixedHeader(
                io.netty.handler.codec.mqtt.MqttMessageType.PUBREL, false,
                MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttPubReplyMessageVariableHeader(packetId, (byte) 0, NO_PROPERTIES)));
    }

    @Override
    public boolean isActive() {
        return !closed && connection.channel().isActive();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        // 先断入站触发清理，再断通道
        disposeQuietly();
    }

    @Override
    public String remoteAddress() {
        return remoteAddress;
    }

    /***
    * 将报文投入出站汇聚点，连接不可用时返回 false；
    * 缓冲写满说明客户端消费过慢，记录告警并断开连接防止内存持续膨胀。
    * @author wcdk
    * @param message 报文
    * @return 入队成功返回 true
    ***/
    private boolean emit(MqttMessage message) {
        if (closed) {
            return false;
        }
        Sinks.EmitResult result = outboundSink.tryEmitNext(message);
        // 序列化包装被其他线程短暂持锁时重试，避免报文因并发竞争被丢弃
        for (int attempt = 0; attempt < 3 && result == Sinks.EmitResult.FAIL_NON_SERIALIZED; attempt++) {
            Thread.yield();
            result = outboundSink.tryEmitNext(message);
        }
        if (result == Sinks.EmitResult.FAIL_OVERFLOW) {
            log.warn("客户端出站缓冲写满，判定为慢消费者并断开，远端地址：{}，缓冲上限：{}条",
                    remoteAddress, properties.getBroker().getMaxOutboundBuffer());
            // 在事件循环上异步关闭，避免在会话锁或写路径内重入清理逻辑
            try {
                connection.channel().eventLoop().execute(this::close);
            } catch (RuntimeException ex) {
                // 事件循环已关闭时连接必然失效，忽略
            }
            return false;
        }
        if (result.isFailure()) {
            log.debug("报文写出失败，报文类型：{}，远端地址：{}，原因：{}",
                    message.fixedHeader().messageType(), remoteAddress, result);
            return false;
        }
        return true;
    }

    /***
    * 发送 CONNACK 拒绝后完成出站汇聚，刷出报文后关闭连接。
    * @author wcdk
    ***/
    private void completeOutboundAndClose() {
        outboundSink.tryEmitComplete();
    }

    /***
    * 幂等关闭底层连接。
    * @author wcdk
    ***/
    private void disposeQuietly() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            connection.dispose();
        } catch (RuntimeException ex) {
            log.debug("关闭连接异常，远端地址：{}，异常信息：{}", remoteAddress, ex.getMessage());
        }
    }
}
