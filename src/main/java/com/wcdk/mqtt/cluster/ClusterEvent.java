package com.wcdk.mqtt.cluster;

import java.util.Map;

/***
* 集群对等事件，序列化为一行 JSON 在节点间传输。
* @author wcdk
* @param type 事件类型
* @param from 发送方节点 ID
* @param payload 事件负载
***/
public record ClusterEvent(ClusterEventType type, String from, Map<String, Object> payload) {

    /***
    * 规范化构造，负载为空时补空 Map。
    * @author wcdk
    ***/
    public ClusterEvent {
        payload = payload == null ? Map.of() : payload;
    }
}
