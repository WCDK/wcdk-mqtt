package com.wcdk.mqtt.bean;

import java.util.List;

import com.wcdk.mqtt.core.core.MqttBrokerSession;
import lombok.Data;

/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
@Data
public class ClientSession {

    /*** 客户端id ***/
    private String clientId;
    /*** Broker 节点 ID ***/
    private String nodeId;
    /*** 清理会话标识 离线后会话是否清理 **/
    private boolean cleanSession;
    /*** 是否存活 ***/
    private boolean keepAlive;
    /*** 最后活动时间 ***/
    private long lastActivityTime;
    /*** 会话URL ***/
    private String sessionUrl;
    /*** QoS级别 ***/
    private int qos;
    /*** 发布主题列表 ***/
    private List<String> publicTopics;
    /*** 订阅主题列表 ***/
    private List<String> subscribeTopics;
    /*** QoS 状态快照 ***/
    private MqttBrokerSession.QosStateSnapshot qosStateSnapshot;
}