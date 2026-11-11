package com.wcdk.mqtt.core;

/***
* MQTT 客户端连接句柄，会话通过它向客户端写出报文并感知连接状态。
* @author wcdk
***/
public interface MqttConnectionHandle {

    /***
    * 尝试向客户端写出一条 MQTT PUBLISH 报文。
    * @author wcdk
    * @param message 待写出的 MQTT 消息
    * @param qos 实际投递 QoS
    * @param retained 是否为保留消息
    * @param packetId 包 ID，QoS 0 时为 0
    * @param duplicate 是否为重投报文，重投时需置 DUP 标记
    * @return 写出成功返回 true，连接不可用返回 false
    ***/
    boolean send(MqttBrokerMessage message, int qos, boolean retained, int packetId, boolean duplicate);

    /***
    * 写出 PUBREL 协议报文，用于 QoS 2 第二阶段确认。
    * @author wcdk
    * @param packetId 包 ID
    ***/
    void sendPubRel(int packetId);

    /***
    * 判断当前连接是否仍然可用。
    * @author wcdk
    ***/
    boolean isActive();

    /***
    * 关闭当前连接。
    * @author wcdk
    ***/
    void close();

    /***
    * 返回客户端远端地址描述，用于日志与管理接口展示。
    * @author wcdk
    ***/
    String remoteAddress();
}
