package com.wcdk.mqtt.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/***
* 保留消息存储，按主题保存最近一条 retained 消息。
* @author wcdk
***/
public class MqttRetainedStore {

    private final Map<String, MqttBrokerMessage> retained = new ConcurrentHashMap<>();

    /***
    * 保存或删除保留消息，空负载表示删除该主题的保留消息。
    * @author wcdk
    * @param message 消息
    ***/
    public void store(MqttBrokerMessage message) {
        if (message == null || !message.retained()) {
            return;
        }
        if (message.payload().length == 0) {
            retained.remove(message.topic());
            return;
        }
        retained.put(message.topic(), message.asRetained());
    }

    /***
    * 按主题过滤器查询命中的保留消息。
    * @author wcdk
    * @param topicFilter 主题过滤器
    * @return 命中的保留消息列表
    ***/
    public List<MqttBrokerMessage> findByFilter(String topicFilter) {
        List<MqttBrokerMessage> result = new ArrayList<>();
        for (MqttBrokerMessage message : retained.values()) {
            if (MqttTopicMatcher.matches(topicFilter, message.topic())) {
                result.add(message);
            }
        }
        return result;
    }

    /***
    * 返回全部保留消息，用于集群同步。
    * @author wcdk
    ***/
    public List<MqttBrokerMessage> findAll() {
        return List.copyOf(retained.values());
    }

    /***
    * 批量合并其他节点同步来的保留消息。
    * @author wcdk
    * @param messages 保留消息列表
    ***/
    public void merge(List<MqttBrokerMessage> messages) {
        if (messages == null) {
            return;
        }
        for (MqttBrokerMessage message : messages) {
            store(message);
        }
    }

    public int size() {
        return retained.size();
    }

    public void clear() {
        retained.clear();
    }
}
