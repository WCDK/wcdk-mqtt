package com.wcdk.mqtt.controller;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.wcdk.mqtt.cluster.MqttClusterManager;
import com.wcdk.mqtt.core.MqttSession;
import com.wcdk.mqtt.core.MqttSessionRegistry;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/***
* 客户端会话与订阅管理接口。
* @author wcdk
***/
@RestController
@RequestMapping("/api")
public class ClientController {

    private final MqttSessionRegistry registry;
    private final MqttClusterManager clusterManager;

    public ClientController(MqttSessionRegistry registry, MqttClusterManager clusterManager) {
        this.registry = registry;
        this.clusterManager = clusterManager;
    }

    /***
    * 查询客户端列表，合并本节点会话与集群同步来的远端会话。
    * @author wcdk
    * @param keyword 关键字（匹配客户端 ID 或用户名）
    * @param online 是否只看在线
    * @return 客户端视图列表
    ***/
    @GetMapping("/clients")
    public Mono<ApiResponse<List<ClientView>>> clients(@RequestParam(required = false) String keyword,
                                                      @RequestParam(required = false) Boolean online) {
        return Mono.fromSupplier(() -> {
            List<ClientView> result = new ArrayList<>();
            for (MqttSession session : registry.sessions()) {
                ClientView view = localView(session);
                if (matches(view, keyword, online)) {
                    result.add(view);
                }
            }
            for (MqttSession.SessionSnapshot snapshot : registry.remoteSnapshots().values()) {
                if (registry.findByClientId(snapshot.getClientId()) != null) {
                    continue;
                }
                ClientView view = remoteView(snapshot);
                if (matches(view, keyword, online)) {
                    result.add(view);
                }
            }
            result.sort((left, right) -> left.clientId().compareTo(right.clientId()));
            return ApiResponse.ok(result);
        });
    }

    /***
    * 查询单个客户端详情。
    * @author wcdk
    * @param clientId 客户端 ID
    * @return 客户端视图
    ***/
    @GetMapping("/clients/{clientId}")
    public Mono<ApiResponse<ClientView>> client(@PathVariable String clientId) {
        return Mono.fromSupplier(() -> {
            MqttSession session = registry.findByClientId(clientId);
            if (session != null) {
                return ApiResponse.ok(localView(session));
            }
            MqttSession.SessionSnapshot snapshot = registry.remoteSnapshots().get(clientId);
            if (snapshot != null) {
                return ApiResponse.ok(remoteView(snapshot));
            }
            throw new java.util.NoSuchElementException("客户端不存在：" + clientId);
        });
    }

    /***
    * 查询指定客户端的订阅列表。
    * @author wcdk
    * @param clientId 客户端 ID
    * @return 订阅视图列表
    ***/
    @GetMapping("/clients/{clientId}/subscriptions")
    public Mono<ApiResponse<List<SubscriptionView>>> clientSubscriptions(@PathVariable String clientId) {
        return Mono.fromSupplier(() -> ApiResponse.ok(subscriptionsOfClient(clientId)));
    }

    /***
    * 强制下线并可选清除客户端会话。
    * @author wcdk
    * @param clientId 客户端 ID
    * @param clear 是否同时移除会话状态
    * @return 操作结果
    ***/
    @DeleteMapping("/clients/{clientId}")
    public Mono<ApiResponse<Map<String, Object>>> kick(@PathVariable String clientId,
                                                       @RequestParam(defaultValue = "false") Boolean clear) {
        return Mono.fromSupplier(() -> {
            boolean kicked = clusterManager.kickClient(clientId);
            boolean cleared = false;
            if (Boolean.TRUE.equals(clear)) {
                cleared = registry.forceRemove(clientId);
            }
            if (!kicked && !cleared) {
                throw new java.util.NoSuchElementException("客户端不存在：" + clientId);
            }
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("kicked", kicked);
            data.put("cleared", cleared);
            return ApiResponse.ok(data);
        });
    }

    /***
    * 聚合查询订阅列表，可按客户端与主题过滤。
    * @author wcdk
    * @param clientId 客户端 ID 过滤
    * @param topic 主题过滤器关键字
    * @return 订阅视图列表
    ***/
    @GetMapping("/subscriptions")
    public Mono<ApiResponse<List<SubscriptionView>>> subscriptions(@RequestParam(required = false) String clientId,
                                                                  @RequestParam(required = false) String topic) {
        return Mono.fromSupplier(() -> {
            List<SubscriptionView> result = new ArrayList<>();
            for (MqttSession session : registry.sessions()) {
                collectSubscriptions(result, session.clientId(), true, session.ownerNodeId(),
                        session.subscriptions(), session.isActive());
            }
            for (MqttSession.SessionSnapshot snapshot : registry.remoteSnapshots().values()) {
                if (registry.findByClientId(snapshot.getClientId()) != null) {
                    continue;
                }
                collectSubscriptions(result, snapshot.getClientId(), false, snapshot.getOwnerNodeId(),
                        snapshot.getSubscriptions(), snapshot.isOnline());
            }
            List<SubscriptionView> filtered = result.stream()
                    .filter(view -> !StringUtils.hasText(clientId) || view.clientId().equals(clientId))
                    .filter(view -> !StringUtils.hasText(topic) || view.topicFilter().contains(topic))
                    .sorted((left, right) -> left.clientId().compareTo(right.clientId()))
                    .toList();
            return ApiResponse.ok(filtered);
        });
    }

    private List<SubscriptionView> subscriptionsOfClient(String clientId) {
        List<SubscriptionView> result = new ArrayList<>();
        MqttSession session = registry.findByClientId(clientId);
        if (session != null) {
            collectSubscriptions(result, session.clientId(), true, session.ownerNodeId(),
                    session.subscriptions(), session.isActive());
            return result;
        }
        MqttSession.SessionSnapshot snapshot = registry.remoteSnapshots().get(clientId);
        if (snapshot != null) {
            collectSubscriptions(result, snapshot.getClientId(), false, snapshot.getOwnerNodeId(),
                    snapshot.getSubscriptions(), snapshot.isOnline());
        }
        return result;
    }

    private void collectSubscriptions(List<SubscriptionView> result,
                                      String clientId,
                                      boolean local,
                                      String ownerNodeId,
                                      Map<String, Integer> subscriptions,
                                      boolean online) {
        if (subscriptions == null) {
            return;
        }
        subscriptions.forEach((filter, qos) -> result.add(
                new SubscriptionView(clientId, filter, qos, online, local ? "local" : "remote",
                        ownerNodeId == null ? "" : ownerNodeId)));
    }

    private ClientView localView(MqttSession session) {
        return new ClientView(
                session.clientId(),
                session.username(),
                "local",
                session.isActive(),
                session.cleanSession(),
                session.ownerNodeId(),
                session.remoteAddress(),
                session.keepAliveSeconds(),
                session.subscriptions(),
                session.offlineQueueSize(),
                session.outboundPendingCount(),
                session.inboundQos2Count(),
                Instant.ofEpochMilli(session.lastActivityTime()));
    }

    private ClientView remoteView(MqttSession.SessionSnapshot snapshot) {
        String ownerNodeId = snapshot.getOwnerNodeId() == null ? "" : snapshot.getOwnerNodeId();
        return new ClientView(
                snapshot.getClientId(),
                snapshot.getUsername(),
                "remote",
                snapshot.isOnline(),
                snapshot.isCleanSession(),
                ownerNodeId,
                null,
                0,
                snapshot.getSubscriptions() == null ? Map.of() : snapshot.getSubscriptions(),
                0,
                snapshot.getOutboundPending() == null ? 0 : snapshot.getOutboundPending().size(),
                snapshot.getInboundQos2() == null ? 0 : snapshot.getInboundQos2().size(),
                null);
    }

    private boolean matches(ClientView view, String keyword, Boolean online) {
        if (online != null && view.online() != online) {
            return false;
        }
        if (!StringUtils.hasText(keyword)) {
            return true;
        }
        String lowerKeyword = keyword.toLowerCase();
        return (view.clientId() != null && view.clientId().toLowerCase().contains(lowerKeyword))
                || (view.username() != null && view.username().toLowerCase().contains(lowerKeyword));
    }

    /***
    * 客户端会话视图。
    * @author wcdk
    ***/
    public record ClientView(
            String clientId,
            String username,
            String source,
            boolean online,
            boolean cleanSession,
            String ownerNodeId,
            String remoteAddress,
            int keepAliveSeconds,
            Map<String, Integer> subscriptions,
            int offlineQueueSize,
            int outboundPendingCount,
            int inboundQos2Count,
            Instant lastActivityTime) {
    }

    /***
    * 订阅视图。
    * @author wcdk
    ***/
    public record SubscriptionView(
            String clientId,
            String topicFilter,
            int qos,
            boolean online,
            String source,
            String ownerNodeId) {
    }
}
