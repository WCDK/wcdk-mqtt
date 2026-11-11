package com.wcdk.mqtt.cluster;

/***
* 集群事件类型枚举，对等节点间以换行 JSON 报文交换。
* @author wcdk
***/
public enum ClusterEventType {

    /** 连接建立握手，携带节点身份与对外地址 */
    HELLO,
    /** 握手应答，携带接收方节点身份 */
    HELLO_ACK,
    /** 心跳保活，携带时间戳 */
    HEARTBEAT,
    /** 消息发布同步，可携带目标客户端实现点对点转发 */
    PUBLISH,
    /** 会话状态同步（上线、离线、订阅变更） */
    SESSION_UPSERT,
    /** 会话归属声明，通知其他节点同一客户端已接入本节点 */
    SESSION_CLAIM,
    /** 清理会话通知（清理会话销毁时触发） */
    SESSION_REMOVE,
    /** 管理端强制下线指令 */
    KICK,
    /** 保留消息全量同步 */
    RETAINED_SYNC,
    /** 离线缓冲清理通知，会话在其他节点上线时触发 */
    OFFLINE_CLEAR
}
