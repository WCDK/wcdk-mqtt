package com.wcdk.mqtt.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/***
* 保留消息存储，按主题保存最近一条 retained 消息。
* 每个条目携带单调递增版本号，集群合并时按版本而不是到达顺序裁决，
* 空负载表示删除墓碑，墓碑保留一段时间以便删除动作传播到全部节点。
* @author wcdk
***/
public class MqttRetainedStore {

    /** 默认墓碑保留时长：删除动作保留 7 天，避免恢复同步时把已删除消息重新写回 */
    private static final long DEFAULT_TOMBSTONE_TTL_MILLIS = 7 * 24 * 60 * 60 * 1000L;

    private final Map<String, RetainedEntry> retained = new ConcurrentHashMap<>();

    /** 墓碑保留时长，可由集群配置覆盖 */
    private final long tombstoneTtlMillis;

    /** 本地版本时钟，保证同一节点生成的版本严格递增且不早于当前时间 */
    private long versionClock;

    public MqttRetainedStore() {
        this(DEFAULT_TOMBSTONE_TTL_MILLIS);
    }

    /***
    * 指定墓碑保留时长的构造函数。
    * @author wcdk
    * @param tombstoneTtlMillis 墓碑保留毫秒数
    ***/
    public MqttRetainedStore(long tombstoneTtlMillis) {
        this.tombstoneTtlMillis = Math.max(60_000L, tombstoneTtlMillis);
    }

    /***
    * 保存或删除保留消息，空负载表示删除该主题的保留消息。
    * @author wcdk
    * @param message 消息
    ***/
    public void store(MqttBrokerMessage message) {
        if (message == null || !message.retained()) {
            return;
        }
        store(message, -1L);
    }

    /***
    * 按指定版本保存保留消息；版本小于等于 0 表示本地写入，由本节点分配版本。
    * @author wcdk
    * @param message 消息
    * @param version 外部版本，非正数表示本地生成
    ***/
    public void store(MqttBrokerMessage message, long version) {
        if (message == null || !message.retained()) {
            return;
        }
        String topic = message.topic();
        if (topic == null || topic.isBlank()) {
            return;
        }
        // 空负载保留 retained 标记，形成删除墓碑，供集群对账使用
        MqttBrokerMessage normalized = message.asRetained();
        if (version > 0) {
            // 采纳外部版本，保证本节点后续写入的版本高于已见过的远端版本（缓解节点时钟偏差）
            observeVersion(version);
        }
        retained.compute(topic, (key, existing) -> {
            if (version > 0) {
                if (existing != null && compare(version, message.messageId(),
                        existing.version(), existing.message().messageId()) <= 0) {
                    return existing;
                }
                return new RetainedEntry(normalized, version);
            }
            long existingVersion = existing == null ? 0L : existing.version();
            long nextVersion = nextVersion(existingVersion);
            return new RetainedEntry(normalized, nextVersion);
        });
    }

    /***
    * 分配本地版本：取当前时间、版本时钟与既有版本三者最大值加一，保证严格单调。
    * @author wcdk
    * @param existingVersion 既有条目版本
    * @return 新版本号
    ***/
    private synchronized long nextVersion(long existingVersion) {
        long next = Math.max(System.currentTimeMillis(), Math.max(versionClock + 1, existingVersion + 1));
        versionClock = next;
        return next;
    }

    /***
    * 采纳已见到的远端版本，保证后续本地写入版本高于远端版本。
    * @author wcdk
    * @param version 远端版本
    ***/
    private synchronized void observeVersion(long version) {
        if (version > versionClock) {
            versionClock = version;
        }
    }

    /***
    * 按主题过滤器查询命中的保留消息，墓碑不参与投递。
    * @author wcdk
    * @param topicFilter 主题过滤器
    * @return 命中的保留消息列表
    ***/
    public List<MqttBrokerMessage> findByFilter(String topicFilter) {
        List<MqttBrokerMessage> result = new ArrayList<>();
        for (RetainedEntry entry : retained.values()) {
            MqttBrokerMessage message = entry.message();
            if (isTombstone(message)) {
                continue;
            }
            if (MqttTopicMatcher.matches(topicFilter, message.topic())) {
                result.add(message);
            }
        }
        return result;
    }

    /***
    * 返回全部有效保留消息，用于管理接口展示。
    * @author wcdk
    * @return 保留消息条目列表
    ***/
    public List<RetainedEntry> findAll() {
        List<RetainedEntry> result = new ArrayList<>();
        for (RetainedEntry entry : retained.values()) {
            if (!isTombstone(entry.message())) {
                result.add(entry);
            }
        }
        return result;
    }

    /***
    * 返回全部条目（含删除墓碑），用于集群全量对账。
    * @author wcdk
    * @return 全部条目列表
    ***/
    public List<RetainedEntry> entries() {
        return List.copyOf(retained.values());
    }

    /***
    * 查询指定主题当前条目的版本，不存在时返回 0。
    * @author wcdk
    * @param topic 主题名称
    * @return 版本号
    ***/
    public long versionOf(String topic) {
        RetainedEntry entry = topic == null ? null : retained.get(topic);
        return entry == null ? 0L : entry.version();
    }

    /***
    * 合并其他节点同步来的保留消息条目，按版本比较后保留较新值。
    * @author wcdk
    * @param entries 保留消息条目列表
    ***/
    public void merge(List<RetainedEntry> entries) {
        if (entries == null) {
            return;
        }
        for (RetainedEntry entry : entries) {
            if (entry == null || entry.message() == null) {
                continue;
            }
            store(entry.message(), entry.version());
        }
    }

    /***
    * 清理超过保留期的删除墓碑。
    * @author wcdk
    * @param now 当前时间毫秒
    ***/
    public void purgeExpiredTombstones(long now) {
        retained.entrySet().removeIf(entry -> {
            RetainedEntry value = entry.getValue();
            return isTombstone(value.message()) && now - value.version() > tombstoneTtlMillis;
        });
    }

    /***
    * 比较两个条目新旧：先比版本，版本相同再按消息 ID 字典序确定唯一胜者。
    * @author wcdk
    * @param leftVersion 左版本
    * @param leftMessageId 左消息 ID
    * @param rightVersion 右版本
    * @param rightMessageId 右消息 ID
    * @return 大于 0 表示左侧更新
    ***/
    private static int compare(long leftVersion, String leftMessageId,
                               long rightVersion, String rightMessageId) {
        int byVersion = Long.compare(leftVersion, rightVersion);
        if (byVersion != 0) {
            return byVersion;
        }
        return nullSafe(leftMessageId).compareTo(nullSafe(rightMessageId));
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    private static boolean isTombstone(MqttBrokerMessage message) {
        return message == null || message.payload().length == 0;
    }

    public int size() {
        int count = 0;
        for (RetainedEntry entry : retained.values()) {
            if (!isTombstone(entry.message())) {
                count++;
            }
        }
        return count;
    }

    public void clear() {
        retained.clear();
    }

    /***
    * 保留消息条目，版本用于集群对账。
    * @author wcdk
    ***/
    public record RetainedEntry(MqttBrokerMessage message, long version) {
    }
}
