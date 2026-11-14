package com.wcdk.mqtt.cluster;

import java.time.Instant;

import lombok.Data;

/***
* 集群对等节点状态视图，供管理控制台展示。
* @author wcdk
***/
@Data
public class ClusterNodeState {

    /** 节点唯一 ID */
    private volatile String nodeId;

    /** 节点对外公布的地址 host:port */
    private volatile String address;

    /** 当前是否连接在线 */
    private volatile boolean connected;

    /** 本节点是主动拨号方返回 true，接受方返回 false */
    private volatile boolean dialed;

    /** 最后一次收到该节点报文的时间 */
    private volatile Instant lastSeenAt;

    /** 连接建立时间 */
    private volatile Instant connectedAt;
}
