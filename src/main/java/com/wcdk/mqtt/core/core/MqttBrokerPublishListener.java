package com.wcdk.mqtt.core.core;
/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
public interface MqttBrokerPublishListener {

    void onPublish(MqttBrokerMessage message);
}
