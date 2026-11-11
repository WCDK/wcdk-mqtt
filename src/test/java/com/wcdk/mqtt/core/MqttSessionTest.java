package com.wcdk.mqtt.core;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

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
