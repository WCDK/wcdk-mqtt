package com.wcdk.mqtt.core;

import com.wcdk.mqtt.config.MqttProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/***
* 远端会话快照版本对账单元测试：覆盖旧快照丢弃、删除墓碑与版本相同裁决。
* @author wcdk
***/
class MqttSessionSnapshotVersionTest {

    private MqttSessionRegistry registry;

    @BeforeEach
    void setUp() {
        MqttProperties properties = new MqttProperties();
        properties.getBroker().setRetainedMessages(false);
        properties.getBroker().setOfflineQueue(false);
        registry = new MqttSessionRegistry(properties, new MqttAcl(properties), new MqttRetainedStore());
    }

    /***
    * 验证低版本快照不会覆盖高版本快照，高版本快照可以正常更新。
    * @author wcdk
    ***/
    @Test
    void staleSnapshotShouldBeIgnored() {
        registry.putRemoteSnapshot(snapshot("client-version", "node-1", 5, true));
        assertThat(remoteSnapshot("client-version").getStateVersion()).isEqualTo(5);

        registry.putRemoteSnapshot(snapshot("client-version", "node-2", 3, false));
        assertThat(remoteSnapshot("client-version").getStateVersion()).isEqualTo(5);
        assertThat(remoteSnapshot("client-version").isOnline()).isTrue();

        registry.putRemoteSnapshot(snapshot("client-version", "node-2", 7, false));
        assertThat(remoteSnapshot("client-version").getStateVersion()).isEqualTo(7);
        assertThat(remoteSnapshot("client-version").isOnline()).isFalse();
    }

    /***
    * 验证删除事件按版本生效，迟到快照不会重新写回已删除会话。
    * @author wcdk
    ***/
    @Test
    void removedSnapshotShouldBlockLateSnapshot() {
        registry.putRemoteSnapshot(snapshot("client-remove", "node-1", 5, true));

        // 旧删除指令版本不足，忽略
        registry.removeRemoteSnapshot("client-remove", 4, "node-1");
        assertThat(remoteSnapshot("client-remove")).isNotNull();

        registry.removeRemoteSnapshot("client-remove", 6, "node-1");
        assertThat(remoteSnapshot("client-remove")).isNull();

        // 删除前的迟到快照不得复活会话
        registry.putRemoteSnapshot(snapshot("client-remove", "node-1", 5, true));
        assertThat(remoteSnapshot("client-remove")).isNull();

        // 版本高于删除标记的新快照允许重新登记
        registry.putRemoteSnapshot(snapshot("client-remove", "node-1", 8, true));
        assertThat(remoteSnapshot("client-remove")).isNotNull();
    }

    private MqttSession.SessionSnapshot remoteSnapshot(String clientId) {
        return registry.remoteSnapshots().get(clientId);
    }

    private static MqttSession.SessionSnapshot snapshot(String clientId, String ownerNodeId,
                                                        long version, boolean online) {
        MqttSession.SessionSnapshot snapshot = new MqttSession.SessionSnapshot();
        snapshot.setClientId(clientId);
        snapshot.setOwnerNodeId(ownerNodeId);
        snapshot.setStateVersion(version);
        snapshot.setOnline(online);
        return snapshot;
    }
}
