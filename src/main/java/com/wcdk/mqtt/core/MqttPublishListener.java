package com.wcdk.mqtt.core;

/***
* 消息发布监听器，注册表完成本地投递后回调，用于集群广播与消息记录。
* @author wcdk
***/
public interface MqttPublishListener {

    /***
    * 消息发布回调。
    * @author wcdk
    * @param message 已发布的消息
    * @param matchedSubscriptions 本次投递命中的订阅过滤器
    ***/
    void onPublish(MqttBrokerMessage message, java.util.List<String> matchedSubscriptions);
}
