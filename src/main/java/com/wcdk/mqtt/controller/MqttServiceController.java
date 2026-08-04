package com.wcdk.mqtt.controller;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.wcdk.mqtt.bean.ClientSession;
import com.wcdk.mqtt.core.config.MqttBrokerProperties;
import com.wcdk.mqtt.core.core.MqttBrokerClusterManager;
import com.wcdk.mqtt.core.core.MqttBrokerSession;
import com.wcdk.mqtt.core.core.MqttBrokerSessionRegistry;
import com.wcdk.mqtt.core.core.MqttQueue;
import com.wcdk.mqtt.core.core.MqttTopicFilter;
import com.wcdk.mqtt.core.core.ReactorMqttBroker;
import com.wcdk.mqtt.influx.InfluxDbUtil;
import com.wcdk.mqtt.influx.InfluxProperties;
import com.wcdk.mqtt.service.MqttMessagePushService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;

@Component
@ConditionalOnProperty(prefix = "wcdk.mqtt.broker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MqttServiceController {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter FLUX_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final String MQTT_MESSAGE_MEASUREMENT = "mqtt_broker_message";

    private final ReactorMqttBroker mqttBroker;
    private final MqttBrokerProperties mqttBrokerProperties;
    private final MqttBrokerSessionRegistry sessionRegistry;
    private final MqttQueue mqttQueue;
    private final MqttMessagePushService mqttMessagePushService;
    private final ObjectProvider<InfluxDbUtil> influxDbUtilProvider;
    private final ObjectProvider<InfluxProperties> influxPropertiesProvider;
    private final ObjectProvider<MqttBrokerClusterManager> clusterManagerProvider;

    public MqttServiceController(ReactorMqttBroker mqttBroker,
                                 MqttBrokerProperties mqttBrokerProperties,
                                 MqttBrokerSessionRegistry sessionRegistry,
                                 MqttQueue mqttQueue,
                                 MqttMessagePushService mqttMessagePushService,
                                 ObjectProvider<InfluxDbUtil> influxDbUtilProvider,
                                 ObjectProvider<InfluxProperties> influxPropertiesProvider,
                                 ObjectProvider<MqttBrokerClusterManager> clusterManagerProvider) {
        this.mqttBroker = mqttBroker;
        this.mqttBrokerProperties = mqttBrokerProperties;
        this.sessionRegistry = sessionRegistry;
        this.mqttQueue = mqttQueue;
        this.mqttMessagePushService = mqttMessagePushService;
        this.influxDbUtilProvider = influxDbUtilProvider;
        this.influxPropertiesProvider = influxPropertiesProvider;
        this.clusterManagerProvider = clusterManagerProvider;
    }

    @PostMapping("/publish")
    @Operation(
            summary = "发布 MQTT 消息",
            description = "向当前服务内置的 MQTT Broker 发布一条测试消息",
            responses = {
                    @ApiResponse(responseCode = "200", description = "发布成功"),
                    @ApiResponse(responseCode = "400", description = "请求参数无效", content = @Content)
            })
    public PublishResponse publish(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(required = true, description = "MQTT 发布请求参数")
            @RequestBody PublishRequest request) {
        if (request == null || !MqttTopicFilter.isValidTopicName(request.topic())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "MQTT 主题无效");
        }
        int qos = normalizeQos(request.qos());
        byte[] payload = request.payload() == null ? new byte[0] : request.payload().getBytes(StandardCharsets.UTF_8);
        mqttBroker.publish(request.topic(), payload, qos, request.retained());
        return new PublishResponse(request.topic(), request.payload(), qos, request.retained(), "发布成功");
    }

    @PostMapping("/publish/client")
    public PublishResponse publishToClientWithDefaultTopic(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(required = true, description = "按客户端推送 MQTT 消息")
            @RequestBody ClientPublishRequest request) {
        if (request == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "请求参数不能为空");
        }
        if (request.clientId() != null && !request.clientId().isBlank()) {
            String topic = mqttMessagePushService.publishToClient(
                    request.clientId(), request.topic(), request.payload(), request.qos(), request.retained());
            return new PublishResponse(topic, request.payload(), normalizeQos(request.qos()), request.retained(), "按 clientId 推送成功");
        }
        if (request.sessionUrl() != null && !request.sessionUrl().isBlank()) {
            String topic = mqttMessagePushService.publishToSessionUrl(
                    request.sessionUrl(), request.topic(), request.payload(), request.qos(), request.retained());
            return new PublishResponse(topic, request.payload(), normalizeQos(request.qos()), request.retained(), "按 sessionUrl 推送成功");
        }
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "clientId 和 sessionUrl 不能同时为空");
    }

    @PostMapping("/publish/client/legacy")
    public PublishResponse publishToClient(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(required = true, description = "按客户端推送 MQTT 消息")
            @RequestBody ClientPublishRequest request) {
        if (request == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "请求参数不能为空");
        }
        if (request.clientId() != null && !request.clientId().isBlank()) {
            mqttMessagePushService.publishToClient(request.clientId(), request.topic(), request.payload(), request.qos(), request.retained());
            return new PublishResponse(request.topic(), request.payload(), normalizeQos(request.qos()), request.retained(), "按 clientId 推送成功");
        }
        if (request.sessionUrl() != null && !request.sessionUrl().isBlank()) {
            mqttMessagePushService.publishToSessionUrl(request.sessionUrl(), request.topic(), request.payload(), request.qos(), request.retained());
            return new PublishResponse(request.topic(), request.payload(), normalizeQos(request.qos()), request.retained(), "按 sessionUrl 推送成功");
        }
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "clientId 和 sessionUrl 不能同时为空");
    }

    @PostMapping("/publish/url")
    public PublishResponse publishToBrokerUrl(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(required = true, description = "按 MQTT URL 发布消息")
            @RequestBody UrlPublishRequest request) {
        if (request == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "请求参数不能为空");
        }
        mqttMessagePushService.publishToBrokerUrl(
                request.brokerUrl(),
                request.topic(),
                request.payload(),
                request.qos(),
                request.retained(),
                request.clientId(),
                request.username(),
                request.password());
        return new PublishResponse(request.topic(), request.payload(), normalizeQos(request.qos()), request.retained(), "按 brokerUrl 发布成功");
    }

    @PostMapping("/subscriptions")
    @Operation(
            summary = "新增测试订阅过滤器",
            description = "向测试队列注册一个 MQTT 主题过滤器，后续匹配消息会被缓存到控制台消息列表中",
            responses = {
                    @ApiResponse(responseCode = "200", description = "订阅成功",
                            content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
                    @ApiResponse(responseCode = "400", description = "主题过滤器无效", content = @Content)
            })
    public List<String> subscribe(
            @Parameter(description = "MQTT 主题过滤器，例如 wcdk/iot/#", required = true)
            @RequestParam String topicFilter) {
        mqttQueue.subscribe(topicFilter);
        return mqttQueue.subscriptions();
    }

    @GetMapping("/subscriptions")
    @Operation(
            summary = "查询测试订阅过滤器",
            description = "查看当前测试队列中已注册的 MQTT 主题过滤器",
            responses = @ApiResponse(responseCode = "200", description = "查询成功",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))))
    public List<String> subscriptions() {
        return mqttQueue.subscriptions();
    }

    @DeleteMapping("/subscriptions")
    @Operation(
            summary = "删除测试订阅过滤器",
            description = "从测试队列中移除一个 MQTT 主题过滤器",
            responses = @ApiResponse(responseCode = "200", description = "删除成功",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))))
    public List<String> unsubscribe(
            @Parameter(description = "需要删除的 MQTT 主题过滤器", required = true)
            @RequestParam String topicFilter) {
        mqttQueue.unsubscribe(topicFilter);
        return mqttQueue.subscriptions();
    }

    @GetMapping("/messages")
    @Operation(
            summary = "查询测试消息",
            description = "查询测试队列中最近收到的 MQTT 消息，可按主题过滤器筛选",
            responses = {
                    @ApiResponse(responseCode = "200", description = "查询成功",
                            content = @Content(array = @ArraySchema(schema = @Schema(implementation = MqttQueue.MqttMessage.class)))),
                    @ApiResponse(responseCode = "400", description = "主题过滤器无效", content = @Content)
            })
    public List<MqttQueue.MqttMessage> messages(
            @Parameter(description = "可选的 MQTT 主题过滤器，例如 wcdk/iot/#")
            @RequestParam(required = false) String topicFilter,
            @Parameter(description = "返回消息条数上限，默认 100")
            @RequestParam(defaultValue = "100") int limit) {
        if (topicFilter != null && !MqttTopicFilter.isValidSubscriptionFilter(topicFilter)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "MQTT 主题过滤器无效");
        }
        return mqttQueue.messages(topicFilter, limit);
    }

    @GetMapping("/messages/list")
    public MessagePageResponse influxMessages(
            @RequestParam(required = false) String topicFilter,
            @RequestParam(required = false) Integer qos,
            @RequestParam(required = false) String sentFrom,
            @RequestParam(required = false) String sentTo,
            @RequestParam(defaultValue = "receivedAt") String sortBy,
            @RequestParam(defaultValue = "desc") String sortDirection,
            @RequestParam(defaultValue = "1") int pageNo,
            @RequestParam(defaultValue = "10") int pageSize) {
        if (topicFilter != null && !MqttTopicFilter.isValidSubscriptionFilter(topicFilter)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "MQTT 主题过滤器无效");
        }
        if (qos != null && (qos < 0 || qos > 2)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "MQTT QoS 只能是 0、1 或 2");
        }
        Instant sentFromInstant = parseInstantParameter(sentFrom, "sentFrom");
        Instant sentToInstant = parseInstantParameter(sentTo, "sentTo");
        if (sentFromInstant != null && sentToInstant != null && sentFromInstant.isAfter(sentToInstant)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "sentFrom 不能大于 sentTo");
        }
        InfluxDbUtil influxDbUtil = influxDbUtilProvider.getIfAvailable();
        InfluxProperties influxProperties = influxPropertiesProvider.getIfAvailable();
        if (influxDbUtil == null || influxProperties == null || influxProperties.getBucket() == null || influxProperties.getBucket().isBlank()) {
            return paginateMessages(
                    mqttQueue.messages(topicFilter, 500),
                    topicFilter,
                    qos,
                    sentFromInstant,
                    sentToInstant,
                    sortBy,
                    sortDirection,
                    pageNo,
                    pageSize);
        }
        return queryMessagesFromInflux(
                influxDbUtil,
                influxProperties,
                topicFilter,
                qos,
                sentFromInstant,
                sentToInstant,
                sortBy,
                sortDirection,
                pageNo,
                pageSize);
    }

    @DeleteMapping("/messages/list")
    @Operation(summary = "删除消息数据", description = "按条件删除 InfluxDB 或测试队列中的 MQTT 消息数据；all=true 时删除全部消息数据")
    public DeleteMessagesResponse deleteInfluxMessages(
            @RequestParam(required = false) String topicFilter,
            @RequestParam(required = false) Integer qos,
            @RequestParam(required = false) String sentFrom,
            @RequestParam(required = false) String sentTo,
            @RequestParam(defaultValue = "false") boolean all) {
        validateDeleteFilterParameters(topicFilter, qos);
        Instant sentFromInstant = parseInstantParameter(sentFrom, "sentFrom");
        Instant sentToInstant = parseInstantParameter(sentTo, "sentTo");
        validateDeleteSentTimeRange(sentFromInstant, sentToInstant);
        if (all) {
            topicFilter = null;
            qos = null;
            sentFromInstant = null;
            sentToInstant = null;
        }
        InfluxDbUtil influxDbUtil = influxDbUtilProvider.getIfAvailable();
        InfluxProperties influxProperties = influxPropertiesProvider.getIfAvailable();
        if (influxDbUtil == null || influxProperties == null || influxProperties.getBucket() == null || influxProperties.getBucket().isBlank()) {
            int deleted = mqttQueue.removeMessages(topicFilter, qos, sentFromInstant, sentToInstant);
            return new DeleteMessagesResponse(deleted, "queue");
        }
        long deleted = queryInfluxMessageCountForDelete(influxDbUtil, influxProperties, topicFilter, qos, sentFromInstant, sentToInstant, all);
        if (deleted > 0) {
            deleteInfluxMessages(influxDbUtil, influxProperties, topicFilter, qos, sentFromInstant, sentToInstant, all);
        }
        return new DeleteMessagesResponse(deleted, "influx");
    }

    @DeleteMapping("/messages/list/selected")
    @Operation(summary = "删除选中的消息数据", description = "按选中记录精确删除 InfluxDB 或测试队列中的 MQTT 消息数据")
    public DeleteMessagesResponse deleteSelectedMessages(@RequestBody List<SelectedMessageRequest> selectedMessages) {
        List<String> ids = normalizeSelectedMessageKeys(selectedMessages);
        if (ids.isEmpty()) {
            return new DeleteMessagesResponse(0, "none");
        }
        InfluxDbUtil influxDbUtil = influxDbUtilProvider.getIfAvailable();
        InfluxProperties influxProperties = influxPropertiesProvider.getIfAvailable();
        if (influxDbUtil == null || influxProperties == null || influxProperties.getBucket() == null || influxProperties.getBucket().isBlank()) {
            int deleted = mqttQueue.removeMessagesByIds(ids);
            return new DeleteMessagesResponse(deleted, "queue");
        }
        ids.forEach(id -> deleteInfluxMessage(influxDbUtil, id));
        return new DeleteMessagesResponse(ids.size(), "influx");
    }

    @DeleteMapping("/messages")
    @Operation(summary = "清空测试消息", description = "清空测试队列中已缓存的 MQTT 消息")
    public void clearMessages() {
        mqttQueue.clearMessages();
    }

    @GetMapping("/overview")
    @Operation(
            summary = "查询控制台概览",
            description = "返回 Broker 运行状态、队列概览和客户端连接统计信息",
            responses = @ApiResponse(responseCode = "200", description = "查询成功"))
    public OverviewResponse overview() {
        List<MqttBrokerSession> sessions = List.copyOf(sessionRegistry.sessions());
        long activeSessionCount = sessions.stream().filter(MqttBrokerSession::isActive).count();
        long persistentSessionCount = sessions.stream().filter(session -> !session.cleanSession()).count();
        int totalSubscriptionCount = sessions.stream().mapToInt(session -> session.subscriptions().size()).sum();
        int pendingAckCount = sessions.stream().mapToInt(MqttBrokerSession::outboundPendingCount).sum();
        int queuedMessageCount = sessions.stream().mapToInt(MqttBrokerSession::queuedPublishCount).sum();
        int inboundQos2Count = sessions.stream().mapToInt(MqttBrokerSession::inboundQos2Count).sum();
        MqttBrokerClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        String clusterNodeId = clusterManager == null ? resolveConfiguredNodeId() : clusterManager.nodeId();

        return new OverviewResponse(
                mqttBroker.isRunning(),
                mqttBrokerProperties.getHost(),
                mqttBrokerProperties.getPort(),
                mqttBrokerProperties.isAnonymous(),
                mqttBrokerProperties.isRetainedMessages(),
                mqttBrokerProperties.getCluster() != null && mqttBrokerProperties.getCluster().isEnabled(),
                clusterNodeId,
                sessions.size(),
                (int) activeSessionCount,
                (int) persistentSessionCount,
                totalSubscriptionCount,
                pendingAckCount,
                queuedMessageCount,
                inboundQos2Count,
                mqttQueue.subscriptions().size(),
                mqttQueue.messageCount(),
                mqttQueue.capacity());
    }

    @GetMapping("/clients")
    @Operation(
            summary = "查询客户端会话",
            description = "返回当前 Broker 维护的 MQTT 客户端会话、订阅和 ACK 队列信息",
            responses = @ApiResponse(responseCode = "200", description = "查询成功",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = ClientSessionView.class)))))
    public List<ClientSessionView> clients() {
        Map<String, ClientSessionView> clientViews = new LinkedHashMap<>();
        sessionRegistry.sessions().stream()
                .map(this::toClientSessionView)
                .forEach(view -> clientViews.put(view.clientId(), view));
        MqttBrokerClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        if (clusterManager != null && clusterManager.isClusterEnabled()) {
            clusterManager.getSessionSnapshots().values().stream()
                    .filter(Objects::nonNull)
                    .map(this::toClusterClientSessionView)
                    .forEach(view -> clientViews.putIfAbsent(view.clientId(), view));
        }
        return clientViews.values().stream()
                .sorted(Comparator.comparing(ClientSessionView::active).reversed()
                        .thenComparing(ClientSessionView::clientId))
                .toList();
    }

    private ClientSessionView toClientSessionView(MqttBrokerSession session) {
        List<SubscriptionView> subscriptions = session.subscriptions().entrySet().stream()
                .map(entry -> new SubscriptionView(entry.getKey(), entry.getValue().value()))
                .sorted(Comparator.comparing(SubscriptionView::topicFilter))
                .toList();
        return new ClientSessionView(
                session.clientId(),
                resolveClientNodeId(session.clientId(), resolveConfiguredNodeId()),
                session.sessionUrl(),
                session.isActive(),
                session.cleanSession(),
                session.isDisconnectedGracefully(),
                subscriptions,
                session.inboundQos2Count(),
                session.outboundPendingCount(),
                session.queuedPublishCount());
    }

    private ClientSessionView toClusterClientSessionView(ClientSession session) {
        List<SubscriptionView> subscriptions = session.getSubscribeTopics() == null ? List.of() : session.getSubscribeTopics().stream()
                .filter(topicFilter -> topicFilter != null && !topicFilter.isBlank())
                .map(topicFilter -> new SubscriptionView(topicFilter, session.getQos()))
                .sorted(Comparator.comparing(SubscriptionView::topicFilter))
                .toList();
        MqttBrokerSession.QosStateSnapshot qosStateSnapshot = session.getQosStateSnapshot();
        return new ClientSessionView(
                session.getClientId(),
                resolveClientNodeId(session.getClientId(), session.getNodeId()),
                session.getSessionUrl(),
                session.isKeepAlive(),
                session.isCleanSession(),
                false,
                subscriptions,
                qosStateSnapshot == null || qosStateSnapshot.getInboundQos2() == null ? 0 : qosStateSnapshot.getInboundQos2().size(),
                qosStateSnapshot == null || qosStateSnapshot.getOutboundPendingMessages() == null ? 0 : qosStateSnapshot.getOutboundPendingMessages().size(),
                0);
    }

    private String resolveClientNodeId(String clientId, String fallbackNodeId) {
        MqttBrokerClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        if (clusterManager != null && clusterManager.isClusterEnabled() && clientId != null && !clientId.isBlank()) {
            String ownerNodeId = clusterManager.getClientOwners().get(clientId.trim());
            if (ownerNodeId != null && !ownerNodeId.isBlank()) {
                return ownerNodeId;
            }
        }
        return fallbackNodeId == null || fallbackNodeId.isBlank() ? resolveConfiguredNodeId() : fallbackNodeId;
    }
    private String resolveConfiguredNodeId() {
        if (mqttBrokerProperties.getCluster() != null && mqttBrokerProperties.getCluster().getNodeId() != null
                && !mqttBrokerProperties.getCluster().getNodeId().isBlank()) {
            return mqttBrokerProperties.getCluster().getNodeId().trim();
        }
        return mqttBrokerProperties.getHost() + ":" + mqttBrokerProperties.getPort();
    }

    private MessagePageResponse queryMessagesFromInflux(InfluxDbUtil influxDbUtil,
                                                        InfluxProperties influxProperties,
                                                        String topicFilter,
                                                        Integer qos,
                                                        Instant sentFrom,
                                                        Instant sentTo,
                                                        String sortBy,
                                                        String sortDirection,
                                                        int pageNo,
                                                        int pageSize) {
        int safePageNo = Math.max(1, pageNo);
        int safePageSize = Math.max(1, Math.min(pageSize, 100));
        long total = queryInfluxMessageCount(influxDbUtil, influxProperties, topicFilter, qos, sentFrom, sentTo);
        if (total <= 0) {
            return new MessagePageResponse(List.of(), 0, safePageNo, safePageSize);
        }

        StringBuilder flux = buildInfluxMessageFlux(influxProperties.getBucket(), topicFilter, qos, sentFrom, sentTo);
        flux.append("  |> group()\n")
                .append("  |> sort(columns: [\"").append(resolveFluxSortColumn(sortBy)).append("\"], desc: ")
                .append(isDesc(sortDirection)).append(")\n")
                .append("  |> limit(n: ").append(safePageSize).append(", offset: ").append((safePageNo - 1) * safePageSize).append(")");

        List<MqttQueue.MqttMessage> messages = new ArrayList<>();
        for (FluxTable table : influxDbUtil.query(flux.toString())) {
            for (FluxRecord record : table.getRecords()) {
                Map<String, Object> values = new LinkedHashMap<>(record.getValues());
                String topic = asString(values.get("topic"));
                if (topic.isBlank()) {
                    continue;
                }
                Instant receivedAt = record.getTime() == null ? Instant.now() : record.getTime();
                List<String> matchedSubscriptions = collectMatchedSubscriptions(topic);
                messages.add(new MqttQueue.MqttMessage(
                        asString(values.get("id")),
                        topic,
                        asString(values.get("payload")),
                        asInt(values.get("qos")),
                        asBoolean(values.get("retained")),
                        matchedSubscriptions,
                        receivedAt));
            }
        }
        return new MessagePageResponse(messages, total, safePageNo, safePageSize);
    }

    private List<String> collectMatchedSubscriptions(String topic) {
        LinkedHashSet<String> matchedSubscriptions = new LinkedHashSet<>();
        mqttQueue.subscriptions().stream()
                .filter(subscription -> MqttTopicFilter.matches(subscription, topic))
                .sorted()
                .forEach(matchedSubscriptions::add);
        sessionRegistry.sessions().stream()
                .flatMap(session -> session.subscriptions().keySet().stream())
                .filter(subscription -> MqttTopicFilter.matches(subscription, topic))
                .sorted()
                .forEach(matchedSubscriptions::add);
        return List.copyOf(matchedSubscriptions);
    }

    private MessagePageResponse paginateMessages(List<MqttQueue.MqttMessage> source,
                                                 String topicFilter,
                                                 Integer qos,
                                                 Instant sentFrom,
                                                 Instant sentTo,
                                                 String sortBy,
                                                 String sortDirection,
                                                 int pageNo,
                                                 int pageSize) {
        int safePageNo = Math.max(1, pageNo);
        int safePageSize = Math.max(1, Math.min(pageSize, 100));
        Comparator<MqttQueue.MqttMessage> comparator = comparatorFor(sortBy);
        if (isDesc(sortDirection)) {
            comparator = comparator.reversed();
        }
        List<MqttQueue.MqttMessage> filtered = source.stream()
                .filter(message -> topicFilter == null || topicFilter.isBlank() || MqttTopicFilter.matches(topicFilter, message.topic()))
                .filter(message -> qos == null || message.qos() == qos)
                .filter(message -> sentFrom == null || !message.receivedAt().isBefore(sentFrom))
                .filter(message -> sentTo == null || !message.receivedAt().isAfter(sentTo))
                .sorted(comparator)
                .toList();
        int total = filtered.size();
        int fromIndex = Math.min((safePageNo - 1) * safePageSize, total);
        int toIndex = Math.min(fromIndex + safePageSize, total);
        return new MessagePageResponse(filtered.subList(fromIndex, toIndex), total, safePageNo, safePageSize);
    }

    private void validateDeleteFilterParameters(String topicFilter, Integer qos) {
        if (topicFilter != null && !MqttTopicFilter.isValidSubscriptionFilter(topicFilter)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "MQTT 主题过滤器无效");
        }
        if (qos != null && (qos < 0 || qos > 2)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "MQTT QoS 只能是 0、1 或 2");
        }
    }

    private void validateDeleteSentTimeRange(Instant sentFromInstant, Instant sentToInstant) {
        if (sentFromInstant != null && sentToInstant != null && sentFromInstant.isAfter(sentToInstant)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "sentFrom 不能大于 sentTo");
        }
    }

    private long queryInfluxMessageCount(InfluxDbUtil influxDbUtil,
                                         InfluxProperties influxProperties,
                                         String topicFilter,
                                         Integer qos,
                                         Instant sentFrom,
                                         Instant sentTo) {
        StringBuilder flux = buildInfluxMessageFlux(influxProperties.getBucket(), topicFilter, qos, sentFrom, sentTo);
        flux.append("  |> keep(columns: [\"payload\"])\n")
                .append("  |> group()\n")
                .append("  |> count(column: \"payload\")");
        return influxDbUtil.query(flux.toString()).stream()
                .flatMap(table -> table.getRecords().stream())
                .map(record -> record.getValueByKey("payload"))
                .filter(Objects::nonNull)
                .mapToLong(MqttServiceController::asLong)
                .findFirst()
                .orElse(0L);
    }

    private long queryInfluxMessageCountForDelete(InfluxDbUtil influxDbUtil,
                                                  InfluxProperties influxProperties,
                                                  String topicFilter,
                                                  Integer qos,
                                                  Instant sentFrom,
                                                  Instant sentTo,
                                                  boolean all) {
        StringBuilder flux = buildInfluxMessageFluxForDelete(influxProperties.getBucket(), topicFilter, qos, sentFrom, sentTo, all);
        flux.append("  |> keep(columns: [\"payload\"])\n")
                .append("  |> group()\n")
                .append("  |> count(column: \"payload\")");
        return influxDbUtil.query(flux.toString()).stream()
                .flatMap(table -> table.getRecords().stream())
                .map(record -> record.getValueByKey("payload"))
                .filter(Objects::nonNull)
                .mapToLong(MqttServiceController::asLong)
                .findFirst()
                .orElse(0L);
    }

    private void deleteInfluxMessages(InfluxDbUtil influxDbUtil,
                                      InfluxProperties influxProperties,
                                      String topicFilter,
                                      Integer qos,
                                      Instant sentFrom,
                                      Instant sentTo,
                                      boolean all) {
        if (topicFilter == null || topicFilter.isBlank()) {
            influxDbUtil.deletePoints(
                    toDeleteStart(sentFrom, all),
                    toDeleteStop(sentTo),
                    buildInfluxDeletePredicate(null, qos));
            return;
        }
        queryInfluxTopicsForDelete(influxDbUtil, influxProperties, topicFilter, qos, sentFrom, sentTo, all)
                .forEach(topic -> influxDbUtil.deletePoints(
                        toDeleteStart(sentFrom, all),
                        toDeleteStop(sentTo),
                        buildInfluxDeletePredicate(topic, qos)));
    }

    private void deleteInfluxMessage(InfluxDbUtil influxDbUtil, String id) {
        influxDbUtil.deletePoints(
                OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC),
                OffsetDateTime.ofInstant(Instant.now().plusSeconds(1), ZoneOffset.UTC),
                buildInfluxDeletePredicate(id));
    }

    private List<String> queryInfluxTopicsForDelete(InfluxDbUtil influxDbUtil,
                                                    InfluxProperties influxProperties,
                                                    String topicFilter,
                                                    Integer qos,
                                                    Instant sentFrom,
                                                    Instant sentTo,
                                                    boolean all) {
        StringBuilder flux = buildInfluxMessageFluxForDelete(influxProperties.getBucket(), topicFilter, qos, sentFrom, sentTo, all);
        flux.append("  |> keep(columns: [\"topic\"])\n")
                .append("  |> group()\n")
                .append("  |> distinct(column: \"topic\")");
        return influxDbUtil.query(flux.toString()).stream()
                .flatMap(table -> table.getRecords().stream())
                .map(record -> asString(record.getValueByKey("topic")))
                .filter(topic -> !topic.isBlank())
                .distinct()
                .toList();
    }

    private StringBuilder buildInfluxMessageFlux(String bucket,
                                                 String topicFilter,
                                                 Integer qos,
                                                 Instant sentFrom,
                                                 Instant sentTo) {
        StringBuilder flux = new StringBuilder()
                .append("from(bucket: \"").append(escapeFlux(bucket)).append("\")\n")
                .append("  |> range(start: ").append(toFluxTimeLiteral(sentFrom)).append(", stop: ").append(toFluxStopLiteral(sentTo)).append(")\n")
                .append("  |> filter(fn: (r) => r._measurement == \"mqtt_broker_message\")\n");
        if (topicFilter != null && !topicFilter.isBlank()) {
            flux.append("  |> filter(fn: (r) => r.topic =~ /").append(toFluxRegex(topicFilter.trim())).append("/)\n");
        }
        flux.append("  |> pivot(rowKey: [\"_time\", \"topic\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n");
        if (qos != null) {
            flux.append("  |> filter(fn: (r) => r.qos == ").append(qos).append(")\n");
        }
        return flux;
    }

    private StringBuilder buildInfluxMessageFluxForDelete(String bucket,
                                                          String topicFilter,
                                                          Integer qos,
                                                          Instant sentFrom,
                                                          Instant sentTo,
                                                          boolean all) {
        StringBuilder flux = new StringBuilder()
                .append("from(bucket: \"").append(escapeFlux(bucket)).append("\")\n")
                .append("  |> range(start: ").append(toDeleteFluxStartLiteral(sentFrom, all)).append(", stop: ").append(toDeleteFluxStopLiteral(sentTo)).append(")\n")
                .append("  |> filter(fn: (r) => r._measurement == \"").append(MQTT_MESSAGE_MEASUREMENT).append("\")\n");
        if (topicFilter != null && !topicFilter.isBlank()) {
            flux.append("  |> filter(fn: (r) => r.topic =~ /").append(toFluxRegex(topicFilter.trim())).append("/)\n");
        }
        flux.append("  |> pivot(rowKey: [\"_time\", \"topic\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n");
        if (qos != null) {
            flux.append("  |> filter(fn: (r) => r.qos == ").append(qos).append(")\n");
        }
        return flux;
    }

    private String buildInfluxDeletePredicate(String topic, Integer qos) {
        StringBuilder predicate = new StringBuilder("_measurement=\"").append(MQTT_MESSAGE_MEASUREMENT).append("\"");
        if (topic != null && !topic.isBlank()) {
            predicate.append(" AND topic=\"").append(escapeFlux(topic)).append("\"");
        }
        if (qos != null) {
            predicate.append(" AND qos=").append(qos);
        }
        return predicate.toString();
    }

    private String buildInfluxDeletePredicate(String id) {
        return new StringBuilder("_measurement=\"").append(MQTT_MESSAGE_MEASUREMENT).append("\"")
                .append(" AND id=\"").append(escapeFlux(id)).append("\"")
                .toString();
    }

    private List<String> normalizeSelectedMessageKeys(List<SelectedMessageRequest> selectedMessages) {
        if (selectedMessages == null || selectedMessages.isEmpty()) {
            return List.of();
        }
        return selectedMessages.stream()
                .map(this::toSelectedMessageKey)
                .distinct()
                .toList();
    }

    private String toSelectedMessageKey(SelectedMessageRequest request) {
        if (request == null || request.id() == null || request.id().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "选中消息不能为空");
        }
        return request.id().trim();
    }

    private Comparator<MqttQueue.MqttMessage> comparatorFor(String sortBy) {
        if ("topic".equalsIgnoreCase(sortBy)) {
            return Comparator.comparing(MqttQueue.MqttMessage::topic, String.CASE_INSENSITIVE_ORDER)
                    .thenComparing(MqttQueue.MqttMessage::receivedAt);
        }
        if ("qos".equalsIgnoreCase(sortBy)) {
            return Comparator.comparingInt(MqttQueue.MqttMessage::qos)
                    .thenComparing(MqttQueue.MqttMessage::receivedAt);
        }
        return Comparator.comparing(MqttQueue.MqttMessage::receivedAt);
    }

    private boolean isDesc(String sortDirection) {
        return "desc".equalsIgnoreCase(sortDirection);
    }

    private String resolveFluxSortColumn(String sortBy) {
        if ("topic".equalsIgnoreCase(sortBy)) {
            return "topic";
        }
        if ("qos".equalsIgnoreCase(sortBy)) {
            return "qos";
        }
        return "_time";
    }

    private Instant parseInstantParameter(String value, String parameterName) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            if (value.chars().allMatch(Character::isDigit)) {
                return Instant.ofEpochMilli(Long.parseLong(value));
            }
            return Instant.parse(value);
        } catch (RuntimeException ex) {
            try {
                return LocalDateTime.parse(value, DATE_TIME_FORMATTER).toInstant(ZoneOffset.ofHours(8));
            } catch (RuntimeException ignored) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, parameterName + " 时间格式无效");
            }
        }
    }

    private String toFluxTimeLiteral(Instant instant) {
        return instant == null ? "-30d" : "time(v: \"" + FLUX_TIME_FORMATTER.format(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)) + "\")";
    }

    private String toFluxStopLiteral(Instant instant) {
        if (instant == null) {
            return "now()";
        }
        Instant inclusiveStop = instant.plusSeconds(1);
        return "time(v: \"" + FLUX_TIME_FORMATTER.format(OffsetDateTime.ofInstant(inclusiveStop, ZoneOffset.UTC)) + "\")";
    }

    private String toDeleteFluxStartLiteral(Instant instant, boolean all) {
        if (all) {
            return "time(v: \"1970-01-01T00:00:00Z\")";
        }
        return toFluxTimeLiteral(instant);
    }

    private String toDeleteFluxStopLiteral(Instant instant) {
        return instant == null ? "now()" : toFluxStopLiteral(instant);
    }

    private OffsetDateTime toDeleteStart(Instant instant, boolean all) {
        Instant effectiveInstant = all || instant == null ? Instant.EPOCH : instant;
        return OffsetDateTime.ofInstant(effectiveInstant, ZoneOffset.UTC);
    }

    private OffsetDateTime toDeleteStop(Instant instant) {
        Instant effectiveInstant = instant == null ? Instant.now().plusSeconds(1) : instant.plusSeconds(1);
        return OffsetDateTime.ofInstant(effectiveInstant, ZoneOffset.UTC);
    }

    private static String toFluxRegex(String topicFilter) {
        StringBuilder builder = new StringBuilder("^");
        for (int i = 0; i < topicFilter.length(); i++) {
            char current = topicFilter.charAt(i);
            if (current == '+') {
                builder.append("[^/]+");
            } else if (current == '#') {
                builder.append(".*");
            } else {
                if ("\\\\/.[]{}()*+-?^$|".indexOf(current) >= 0) {
                    builder.append('\\');
                }
                builder.append(current);
            }
        }
        builder.append('$');
        return builder.toString();
    }

    private static String escapeFlux(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String asString(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static int asInt(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String text && !text.isBlank()) {
            return Integer.parseInt(text);
        }
        return 0;
    }

    private static long asLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String text && !text.isBlank()) {
            return Long.parseLong(text);
        }
        return 0L;
    }

    private static boolean asBoolean(Object value) {
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        if (value instanceof String text && !text.isBlank()) {
            return Boolean.parseBoolean(text);
        }
        return false;
    }

    private static int normalizeQos(Integer qos) {
        if (qos == null) {
            return 0;
        }
        if (qos < 0 || qos > 2) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "MQTT QoS 只能是 0、1 或 2");
        }
        return qos;
    }

    @Schema(name = "PublishRequest", description = "MQTT 消息发布请求")
    public record PublishRequest(
            @Schema(description = "MQTT 主题", example = "wcdk/iot/demo") String topic,
            @Schema(description = "消息内容", example = "hello mqtt") String payload,
            @Schema(description = "消息 QoS，支持 0、1、2", example = "1") Integer qos,
            @Schema(description = "是否保留消息", example = "false") boolean retained) {
    }

    @Schema(name = "PublishResponse", description = "MQTT 消息发布响应")
    public record PublishResponse(
            @Schema(description = "MQTT 主题", example = "wcdk/iot/demo") String topic,
            @Schema(description = "消息内容", example = "hello mqtt") String payload,
            @Schema(description = "消息 QoS", example = "1") int qos,
            @Schema(description = "是否保留消息", example = "false") boolean retained,
            @Schema(description = "处理结果", example = "发布成功") String status) {
    }

    @Schema(name = "ClientPublishRequest", description = "按客户端定向推送 MQTT 消息")
    public record ClientPublishRequest(
            @Schema(description = "客户端 ID", example = "demo-client-1") String clientId,
            @Schema(description = "会话 URL，可替代 clientId", example = "/127.0.0.1:59012") String sessionUrl,
            @Schema(description = "MQTT 主题", example = "wcdk/iot/direct") String topic,
            @Schema(description = "消息内容", example = "hello client") String payload,
            @Schema(description = "消息 QoS，支持 0、1、2", example = "1") Integer qos,
            @Schema(description = "是否保留消息", example = "false") boolean retained) {
    }

    @Schema(name = "UrlPublishRequest", description = "按 MQTT URL 发布消息")
    public record UrlPublishRequest(
            @Schema(description = "MQTT Broker URL", example = "tcp://127.0.0.1:1883") String brokerUrl,
            @Schema(description = "发布使用的客户端 ID，为空时自动生成", example = "push-client") String clientId,
            @Schema(description = "用户名", example = "admin") String username,
            @Schema(description = "密码", example = "123456") String password,
            @Schema(description = "MQTT 主题", example = "wcdk/iot/demo") String topic,
            @Schema(description = "消息内容", example = "hello remote") String payload,
            @Schema(description = "消息 QoS，支持 0、1、2", example = "1") Integer qos,
            @Schema(description = "是否保留消息", example = "false") boolean retained) {
    }

    @Schema(name = "OverviewResponse", description = "控制台首页概览")
    public record OverviewResponse(
            @Schema(description = "Broker 是否运行中", example = "true") boolean brokerRunning,
            @Schema(description = "Broker 监听主机", example = "0.0.0.0") String host,
            @Schema(description = "Broker 监听端口", example = "1883") int port,
            @Schema(description = "是否允许匿名连接", example = "true") boolean anonymous,
            @Schema(description = "是否启用保留消息", example = "true") boolean retainedMessages,
            @Schema(description = "是否启用 Broker 集群", example = "true") boolean clusterEnabled,
            @Schema(description = "当前 Broker 节点 ID", example = "mqtt-node-1") String clusterNodeId,
            @Schema(description = "当前会话总数", example = "4") int totalSessions,
            @Schema(description = "当前活动连接数", example = "2") int activeSessions,
            @Schema(description = "持久会话数", example = "1") int persistentSessions,
            @Schema(description = "客户端订阅总数", example = "6") int totalClientSubscriptions,
            @Schema(description = "待 ACK 出站消息数", example = "3") int pendingAckMessages,
            @Schema(description = "离线待补发消息数", example = "2") int queuedMessages,
            @Schema(description = "入站 QoS2 暂存数", example = "0") int inboundQos2Messages,
            @Schema(description = "测试队列订阅数", example = "2") int queueSubscriptions,
            @Schema(description = "测试队列消息数", example = "15") int queueMessages,
            @Schema(description = "测试队列最大容量", example = "500") int queueCapacity) {
    }

    @Schema(name = "ClientSessionView", description = "客户端会话视图")
    public record ClientSessionView(
            @Schema(description = "客户端 ID", example = "demo-client-1") String clientId,
            @Schema(description = "Broker 节点 ID", example = "mqtt-node-1") String nodeId,
            @Schema(description = "会话 URL", example = "/127.0.0.1:59012") String sessionUrl,
            @Schema(description = "当前是否在线", example = "true") boolean active,
            @Schema(description = "是否清理会话", example = "false") boolean cleanSession,
            @Schema(description = "是否优雅断开", example = "false") boolean disconnectedGracefully,
            @ArraySchema(schema = @Schema(implementation = SubscriptionView.class)) List<SubscriptionView> subscriptions,
            @Schema(description = "入站 QoS2 暂存数", example = "0") int inboundQos2Messages,
            @Schema(description = "待 ACK 出站消息数", example = "1") int pendingAckMessages,
            @Schema(description = "离线待补发消息数", example = "2") int queuedMessages) {
    }

    @Schema(name = "SubscriptionView", description = "客户端订阅视图")
    public record SubscriptionView(
            @Schema(description = "主题过滤器", example = "wcdk/iot/#") String topicFilter,
            @Schema(description = "订阅 QoS", example = "1") int qos) {
    }
    @Schema(name = "MessagePageResponse", description = "MQTT message page response")
    public record MessagePageResponse(
            @ArraySchema(schema = @Schema(implementation = MqttQueue.MqttMessage.class)) List<MqttQueue.MqttMessage> records,
            @Schema(description = "Total records", example = "128") long total,
            @Schema(description = "Current page number", example = "1") int pageNo,
            @Schema(description = "Current page size", example = "20") int pageSize) {
    }

    @Schema(name = "DeleteMessagesResponse", description = "MQTT 消息数据删除结果")
    public record DeleteMessagesResponse(
            @Schema(description = "删除条数", example = "15") long deleted,
            @Schema(description = "删除数据源", example = "influx") String source) {
    }
    @Schema(name = "SelectedMessageRequest", description = "选中消息删除请求")
    public record SelectedMessageRequest(
            @Schema(description = "消息唯一键", example = "6de5b60e-3223-4ddf-a5c9-1472dcb4f2f3") String id) {
    }
}
