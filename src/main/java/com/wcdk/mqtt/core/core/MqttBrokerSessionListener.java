package com.wcdk.mqtt.core.core;

/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
public interface MqttBrokerSessionListener {

    void onSessionOnline(MqttBrokerSession session, boolean sessionPresent);

    void onSessionUpdated(MqttBrokerSession session);

    default void onSessionOffline(MqttBrokerSession session) {
    }
}
