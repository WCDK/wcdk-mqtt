package com.wcdk.mqtt.core;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/***
* 会话包 ID 分配单元测试：覆盖回绕时跳过在途包 ID 的边界场景。
* @author wcdk
***/
class MqttSessionTest {

    /***
    * 验证包 ID 回绕到仍在等待确认的包 ID 时跳过，避免覆盖未确认消息。
    * @author wcdk
    ***/
    @Test
    void nextPacketIdShouldSkipPendingIdAfterWrap() {
        MqttSession session = new MqttSession("wrap-skip", true, null, "node-1");
        session.attach(new StubHandle());

        MqttSession.SessionSnapshot snapshot = new MqttSession.SessionSnapshot();
        snapshot.setPacketIdSequence(65535);
        MqttSession.SnapshotMessage pending = new MqttSession.SnapshotMessage();
        pending.setPacketId(1);
        pending.setTopic("it/wrap");
        pending.setPayload(Base64.getEncoder().encodeToString("x".getBytes(StandardCharsets.UTF_8)));
        pending.setQos(1);
        pending.setRetained(false);
        pending.setState(MqttSession.STATE_WAITING_PUBACK);
        snapshot.setOutboundPending(List.of(pending));
        session.restoreSnapshot(snapshot);

        int packetId = session.sendPublish(new MqttBrokerMessage(null, "it/wrap",
                "y".getBytes(StandardCharsets.UTF_8), 1, false), 1, false);

        // 序列 65535 回绕到 1，但 1 仍在等待 PUBACK，应顺延到 2
        assertThat(packetId).isEqualTo(2);
        assertThat(session.outboundPendingCount()).isEqualTo(2);
        assertThat(session.outboundPendingSnapshot())
                .extracting(MqttSession.PendingOutbound::packetId)
                .containsExactlyInAnyOrder(1, 2);
    }

    /***
    * 验证回绕后没有冲突时正常从 1 开始分配。
    * @author wcdk
    ***/
    @Test
    void nextPacketIdShouldWrapToFirstWhenNoConflict() {
        MqttSession session = new MqttSession("wrap-clean", true, null, "node-1");
        session.attach(new StubHandle());

        MqttSession.SessionSnapshot snapshot = new MqttSession.SessionSnapshot();
        snapshot.setPacketIdSequence(65535);
        snapshot.setOutboundPending(List.of());
        session.restoreSnapshot(snapshot);

        int packetId = session.sendPublish(new MqttBrokerMessage(null, "it/wrap",
                "y".getBytes(StandardCharsets.UTF_8), 1, false), 1, false);

        assertThat(packetId).isEqualTo(1);
        assertThat(session.outboundPendingCount()).isEqualTo(1);
    }

    /***
    * 验证 MQTT5 会话按过期秒数决定断开后是否保留与过期时机，旧协议沿用清理会话语义。
    * @author wcdk
    ***/
    @Test
    void sessionExpiryShouldDecidePersistenceForMqtt5() {
        // MQTT5：过期秒数大于 0 时断开后仍保留
        MqttSession expiring = new MqttSession("expiring", true, null, "node-1");
        expiring.setProtocolLevel(5);
        expiring.setSessionExpirySeconds(2);
        assertThat(expiring.persistsAfterDisconnect()).isTrue();

        expiring.detach();
        long disconnectedAt = System.currentTimeMillis();
        assertThat(expiring.isSessionExpired(disconnectedAt + 1000)).isFalse();
        assertThat(expiring.isSessionExpired(disconnectedAt + 2500)).isTrue();

        // 过期秒数为 0 时断开即结束会话，即使 Clean Start 为 false
        MqttSession noExpiry = new MqttSession("no-expiry", false, null, "node-1");
        noExpiry.setProtocolLevel(5);
        noExpiry.setSessionExpirySeconds(0);
        assertThat(noExpiry.persistsAfterDisconnect()).isFalse();

        // 旧协议：清理会话断开即删，持久会话永不过期
        MqttSession legacyClean = new MqttSession("legacy-clean", true, null, "node-1");
        assertThat(legacyClean.persistsAfterDisconnect()).isFalse();
        assertThat(legacyClean.isSessionExpired(Long.MAX_VALUE)).isFalse();

        MqttSession legacyPersist = new MqttSession("legacy-persist", false, null, "node-1");
        assertThat(legacyPersist.persistsAfterDisconnect()).isTrue();
        assertThat(legacyPersist.isSessionExpired(Long.MAX_VALUE)).isFalse();
    }

    /***
    * 恒为活跃的连接句柄桩，满足会话写出前置条件。
    * @author wcdk
    ***/
    /***
    * 验证接管旧会话时完整迁移订阅、离线队列与状态版本，且版本保持单调。
    * @author wcdk
    ***/
    @Test
    void restoreStateFromShouldCopyStateAndKeepVersionMonotonic() {
        MqttSession previous = new MqttSession("takeover", false, "user", "node-1");
        previous.subscribe("takeover/#", 1);
        previous.enqueueOffline(new MqttBrokerMessage(null, "takeover/a",
                "1".getBytes(StandardCharsets.UTF_8), 1, false), 10);
        previous.bumpStateVersion();
        previous.bumpStateVersion();

        MqttSession next = new MqttSession("takeover", false, "user", "node-2");
        next.restoreStateFrom(previous);

        assertThat(next.subscriptions()).containsEntry("takeover/#", 1);
        assertThat(next.offlineQueueSize()).isEqualTo(1);
        assertThat(next.stateVersion()).isEqualTo(previous.stateVersion());
        // 接管后继续递增，版本不应回退
        assertThat(next.bumpStateVersion()).isGreaterThan(previous.stateVersion());
    }

    /***
    * 验证接管自身或空会话时不做任何变更。
    * @author wcdk
    ***/
    @Test
    void restoreStateFromShouldIgnoreNullAndSelf() {
        MqttSession session = new MqttSession("self-takeover", false, null, "node-1");
        session.subscribe("self/#", 0);

        session.restoreStateFrom(null);
        session.restoreStateFrom(session);

        assertThat(session.subscriptions()).containsExactly(Map.entry("self/#", 0));
    }

    /***
    * 验证离线队列计数随入队、裁剪、抽取与恢复保持一致。
    * @author wcdk
    ***/
    @Test
    void offlineQueueDepthShouldTrackCapacityAndDrain() {
        MqttSession session = new MqttSession("offline-count", false, null, "node-1");
        for (int i = 0; i < 5; i++) {
            session.enqueueOffline(new MqttBrokerMessage(null, "offline-count/" + i,
                    "x".getBytes(StandardCharsets.UTF_8), 1, false), 3);
        }
        assertThat(session.offlineQueueSize()).isEqualTo(3);

        List<MqttBrokerMessage> drained = session.drainOfflineSnapshot();
        assertThat(drained).extracting(MqttBrokerMessage::topic)
                .containsExactly("offline-count/2", "offline-count/3", "offline-count/4");
        assertThat(session.offlineQueueSize()).isZero();

        session.restoreOffline(drained);
        assertThat(session.offlineQueueSize()).isEqualTo(3);
    }

    /***
    * 验证入站 QoS1 去重缓存按上限淘汰最旧记录，而不是整体清空。
    * @author wcdk
    ***/
    @Test
    void inboundQos1CacheShouldEvictOldestInsteadOfClearingAll() {
        MqttSession session = new MqttSession("qos1-cache", true, null, "node-1");
        for (int packetId = 1; packetId <= 4097; packetId++) {
            assertThat(session.isInboundQos1Duplicate(packetId, true)).isFalse();
        }
        // 最近记录仍判重，最旧记录已被淘汰并按新报文处理
        assertThat(session.isInboundQos1Duplicate(4097, true)).isTrue();
        assertThat(session.isInboundQos1Duplicate(1, true)).isFalse();
    }

    /***
    * 验证会话版本以时间种子初始化，节点重启后新会话版本高于重启前的历史版本，
    * 避免对端把新版本快照判为过期而长期拒绝更新。
    * @author wcdk
    ***/
    @Test
    void newSessionVersionShouldExceedPreRestartVersion() {
        MqttSession beforeRestart = new MqttSession("restart-version", false, null, "node-1");
        beforeRestart.bumpStateVersion();
        // 模拟重启前推进到约一分钟前的版本号
        long legacyVersion = System.currentTimeMillis() - 60_000L;

        MqttSession afterRestart = new MqttSession("restart-version", false, null, "node-1");

        assertThat(afterRestart.stateVersion()).isGreaterThan(legacyVersion);
        // 进程内新会话种子严格递增
        MqttSession next = new MqttSession("restart-version-2", false, null, "node-1");
        assertThat(next.stateVersion()).isGreaterThan(afterRestart.stateVersion());
    }

    private static final class StubHandle implements MqttConnectionHandle {

        @Override
        public boolean send(MqttBrokerMessage message, int qos, boolean retained,
                            int packetId, boolean duplicate) {
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
}
