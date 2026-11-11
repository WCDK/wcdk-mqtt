package com.wcdk.mqtt.core;

/***
* 会话生命周期监听器，用于集群同步会话状态。
* @author wcdk
***/
public interface MqttSessionListener {

    /***
    * 会话上线或状态变更回调。
    * @author wcdk
    * @param session 会话
    ***/
    void onSessionChanged(MqttSession session);

    /***
    * 会话销毁回调，清理会话时触发。
    * @author wcdk
    * @param session 会话
    ***/
    void onSessionRemoved(MqttSession session);
}
