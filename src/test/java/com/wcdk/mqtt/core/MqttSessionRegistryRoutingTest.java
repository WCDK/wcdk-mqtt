package com.wcdk.mqtt.core;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.wcdk.mqtt.config.MqttProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/***
* 路由索引单元测试：覆盖按索引投递、订阅增删后的索引重建、会话移除清理与 $ 系统主题隔离。
* @author wcdk
***/
class MqttSessionRegistryRoutingTest {

    private MqttSessionRegistry registry;

    @BeforeEach
    void setUp() {
        MqttProperties properties = new MqttProperties();
        // 单测不依赖保留消息与远端离线缓冲，缩小投递路径干扰面
        properties.getBroker().setRetainedMessages(false);
        properties.getBroker().setOfflineQueue(false);
        registry = new MqttSessionRegistry(properties, new MqttAcl(properties), new MqttRetainedStore());
    }

    /***
    * 验证投递只触达过滤器命中的会话，命中过滤器在返回结果中去重聚合。
    * @author wcdk
    ***/
    @Test
    void deliverShouldOnlyReachMatchingSubscribers() {
        RecordingHandle sensor = register("sensor", "it/+/temp");
        RecordingHandle aggregate = register("aggregate", "it/#");
        RecordingHandle other = register("other", "other/#");

        List<String> matched = registry.deliver(message("it/a/temp", 1));

        assertThat(matched).containsExactlyInAnyOrder("it/+/temp", "it/#");
        assertThat(sensor.sent).hasSize(1);
        assertThat(aggregate.sent).hasSize(1);
        assertThat(other.sent).isEmpty();
    }

    /***
    * 验证同一会话多条过滤器命中时只投递一次，投递 QoS 取全部命中订阅的最高 QoS。
    * @author wcdk
    ***/
    @Test
    void multipleMatchingFiltersShouldDeliverOnceWithMaxQos() {
        RecordingHandle handle = register("multi", "it/+");
        MqttSession session = registry.findByClientId("multi");
        session.subscribe("it/#", 2);
        registry.notifySessionChanged(session);

        List<String> matched = registry.deliver(message("it/a", 1));

        // 两条过滤器均命中，但会话只收到一次投递，且按最高订阅 QoS（2）与发布 QoS（1）取低
        assertThat(matched).containsExactlyInAnyOrder("it/+", "it/#");
        assertThat(handle.sent).hasSize(1);
        assertThat(handle.sent.get(0).qos).isEqualTo(1);
    }

    /***
    * 验证退订后索引同步移除、重新订阅后索引恢复。
    * @author wcdk
    ***/
    @Test
    void unsubscribeAndResubscribeShouldUpdateRoutingIndex() {
        RecordingHandle handle = register("flip", "sensor/+/state");

        MqttSession session = registry.findByClientId("flip");
        session.unsubscribe("sensor/+/state");
        registry.notifySessionChanged(session);

        assertThat(registry.deliver(message("sensor/1/state", 1))).isEmpty();
        assertThat(handle.sent).isEmpty();

        session.subscribe("sensor/+/state", 1);
        registry.notifySessionChanged(session);

        assertThat(registry.deliver(message("sensor/1/state", 1))).containsExactly("sensor/+/state");
        assertThat(handle.sent).hasSize(1);
    }

    /***
    * 验证会话移除清理其索引条目，不影响共享同一过滤器的其他订阅者。
    * @author wcdk
    ***/
    @Test
    void removedSessionShouldClearIndexWithoutBreakingSharedFilter() {
        RecordingHandle first = register("first", "shared/topic");
        RecordingHandle second = register("second", "shared/topic");

        assertThat(registry.forceRemove("first")).isTrue();

        List<String> matched = registry.deliver(message("shared/topic", 1));
        assertThat(matched).containsExactly("shared/topic");
        assertThat(first.sent).isEmpty();
        assertThat(second.sent).hasSize(1);

        // 全部订阅者移除后过滤器条目彻底出清
        assertThat(registry.forceRemove("second")).isTrue();
        assertThat(registry.deliver(message("shared/topic", 1))).isEmpty();
    }

    /***
    * 验证同 ID 会话被替换后旧会话的订阅不再命中，新会话订阅正常索引。
    * @author wcdk
    ***/
    @Test
    void replacedSessionShouldReindexSubscriptions() {
        register("stable", "legacy/topic");

        // 清理会话重连：不迁移旧订阅，索引须按新会话状态重建
        MqttSession replacement = new MqttSession("stable", true, null, "node-1");
        replacement.attach(new RecordingHandle());
        registry.register(replacement);

        assertThat(registry.deliver(message("legacy/topic", 1))).isEmpty();
        assertThat(registry.findByClientId("stable").subscriptionFilters()).isEmpty();
    }

    /***
    * 验证 $ 开头的系统主题不被 # 与首层通配过滤器命中，普通通配索引不误伤。
    * @author wcdk
    ***/
    @Test
    void systemTopicShouldNotMatchWildcardFilters() {
        RecordingHandle hash = register("hash", "#");
        RecordingHandle plus = register("plus", "+/temp");
        RecordingHandle sys = register("sys", "$SYS/#");

        List<String> matched = registry.deliver(message("$SYS/broker/uptime", 0));

        assertThat(matched).containsExactly("$SYS/#");
        assertThat(hash.sent).isEmpty();
        assertThat(plus.sent).isEmpty();
        assertThat(sys.sent).hasSize(1);
    }

    /***
    * 验证 maybeMatches 首层预判对合法过滤器不产生漏判（false 时完整匹配必不命中）。
    * @author wcdk
    ***/
    @Test
    void maybeMatchesShouldNeverRejectMatchingFilter() {
        assertThat(MqttTopicMatcher.maybeMatches("it/+/temp", "it/a/temp")).isTrue();
        assertThat(MqttTopicMatcher.maybeMatches("it/#", "it")).isTrue();
        assertThat(MqttTopicMatcher.maybeMatches("#", "any/topic")).isTrue();
        assertThat(MqttTopicMatcher.maybeMatches("+/temp", "it/temp")).isTrue();
        assertThat(MqttTopicMatcher.maybeMatches("$SYS/#", "$SYS/uptime")).isTrue();

        assertThat(MqttTopicMatcher.maybeMatches("it/+", "sensor/1")).isFalse();
        assertThat(MqttTopicMatcher.maybeMatches("#", "$SYS/uptime")).isFalse();
        assertThat(MqttTopicMatcher.maybeMatches("+/temp", "$SYS/temp")).isFalse();
        // 预判为 false 时完整匹配必不命中
        assertThat(MqttTopicMatcher.matches("it/+", "sensor/1")).isFalse();
        assertThat(MqttTopicMatcher.matches("#", "$SYS/uptime")).isFalse();
    }

    private RecordingHandle register(String clientId, String topicFilter) {
        RecordingHandle handle = new RecordingHandle();
        MqttSession session = new MqttSession(clientId, true, null, "node-1");
        session.attach(handle);
        registry.register(session);
        session.subscribe(topicFilter, 1);
        registry.notifySessionChanged(session);
        return handle;
    }

    private MqttBrokerMessage message(String topic, int qos) {
        return new MqttBrokerMessage(null, topic, "payload".getBytes(StandardCharsets.UTF_8), qos, false);
    }

    /***
    * 记录投递的连接句柄桩，用于断言投递次数与投递 QoS。
    * @author wcdk
    ***/
    private static final class RecordingHandle implements MqttConnectionHandle {

        private final List<Captured> sent = new ArrayList<>();

        @Override
        public boolean send(MqttBrokerMessage message, int qos, boolean retained,
                            int packetId, boolean duplicate) {
            sent.add(new Captured(qos));
            return true;
        }

        @Override
        public void sendPubRel(int packetId) {
            // 单元测试无需真实写出
        }

        @Override
        public boolean isActive() {
            return true;
        }

        @Override
        public void close() {
            // 单元测试无需真实关闭
        }

        @Override
        public String remoteAddress() {
            return "stub";
        }
    }

    private record Captured(int qos) {
    }
}
