package com.wcdk.mqtt.core;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/***
* 保留消息版本合并单元测试：覆盖按版本覆盖、墓碑传播与迟到旧值丢弃。
* @author wcdk
***/
class MqttRetainedStoreTest {

    /***
    * 验证本地写入分配递增版本，远端更高版本覆盖，较低版本被丢弃。
    * @author wcdk
    ***/
    @Test
    void mergeShouldKeepNewestVersion() {
        MqttRetainedStore store = new MqttRetainedStore();
        store.store(message("retain/version", "first"));
        long firstVersion = store.versionOf("retain/version");
        assertThat(firstVersion).isPositive();

        store.store(message("retain/version", "second"), firstVersion + 10);
        assertThat(payloads(store.findByFilter("retain/version"))).containsExactly("second");

        // 迟到的旧版本不应覆盖新值
        store.store(message("retain/version", "stale"), firstVersion + 5);
        assertThat(payloads(store.findByFilter("retain/version"))).containsExactly("second");
    }

    /***
    * 验证空负载墓碑按版本传播，且删除后迟到的旧值不会复活消息。
    * @author wcdk
    ***/
    @Test
    void tombstoneShouldDeleteAndBlockStaleValue() {
        MqttRetainedStore store = new MqttRetainedStore();
        store.store(message("retain/tombstone", "alive"));
        long aliveVersion = store.versionOf("retain/tombstone");

        store.store(message("retain/tombstone", ""), aliveVersion + 10);
        assertThat(store.findByFilter("retain/tombstone")).isEmpty();
        assertThat(store.size()).isZero();

        // 删除前的旧值迟到抵达，不得让消息复活
        store.store(message("retain/tombstone", "resurrect"), aliveVersion + 1);
        assertThat(store.findByFilter("retain/tombstone")).isEmpty();

        // 墓碑仍参与全量对账
        assertThat(store.entries()).hasSize(1);
        assertThat(store.entries().get(0).version()).isEqualTo(aliveVersion + 10);
    }

    /***
    * 验证本地在远端高版本之后写入时仍能获得更高版本。
    * @author wcdk
    ***/
    @Test
    void localWriteShouldExceedRemoteVersion() {
        MqttRetainedStore store = new MqttRetainedStore();
        long futureVersion = System.currentTimeMillis() + 60_000;
        store.store(message("retain/local", "remote"), futureVersion);

        store.store(message("retain/local", "local"));
        assertThat(store.versionOf("retain/local")).isGreaterThan(futureVersion);
        assertThat(payloads(store.findByFilter("retain/local"))).containsExactly("local");
    }

    /***
    * 验证本地版本时钟会采纳其他主题收到的远端高版本，
    * 避免节点时钟偏慢时后续本地写入被对端判为过期。
    * @author wcdk
    ***/
    @Test
    void localVersionClockShouldAdoptRemoteVersionAcrossTopics() {
        MqttRetainedStore store = new MqttRetainedStore();
        long futureVersion = System.currentTimeMillis() + 60_000;
        store.store(message("retain/remote-topic", "remote"), futureVersion);

        store.store(message("retain/local-topic", "local"));

        assertThat(store.versionOf("retain/local-topic")).isGreaterThan(futureVersion);
    }

    private static MqttBrokerMessage message(String topic, String payload) {
        return new MqttBrokerMessage(null, topic, payload.getBytes(StandardCharsets.UTF_8), 1, true);
    }

    private static List<String> payloads(List<MqttBrokerMessage> messages) {
        return messages.stream()
                .map(message -> new String(message.payload(), StandardCharsets.UTF_8))
                .toList();
    }
}
