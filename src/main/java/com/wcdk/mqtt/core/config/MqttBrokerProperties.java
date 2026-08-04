package com.wcdk.mqtt.core.config;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
/**
 * MQTT Broker配置属性类
 * 绑定wcdk.mqtt.broker前缀下的所有配置项
 *
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
@Data
@ConfigurationProperties(prefix = "wcdk.mqtt.broker")
public class MqttBrokerProperties {

    /** 是否启用MQTT Broker服务 */
    private boolean enabled = true;

    /** Broker监听地址 */
    private String host = "0.0.0.0";

    /** Broker监听端口 */
    private int port = 1883;

    /** Netty boss线程数，负责接受新连接 */
    private int bossThreads = 1;

    /** Netty worker线程数，负责处理已建立的连接，0表示使用默认值 */
    private int workerThreads = 0;

    /** TCP backlog队列大小 */
    private int backlog = 128;

    /** 最大消息负载大小（字节），默认1MB */
    private int maxPayloadSize = 1024 * 1024;

    /** 最大客户端ID长度 */
    private int maxClientIdLength = 128;

    /** 是否允许匿名连接 */
    private boolean anonymous = true;

    /** 认证用户名，为空则不启用认证 */
    private String username;

    /** 认证密码 */
    private String password;

    /** 是否支持保留消息 */
    private boolean retainedMessages = true;

    /** 是否启用消息持久化 */
    private boolean persistence;

    /** 集群配置 */
    private Cluster cluster = new Cluster();

    /** 访问控制列表配置 */
    private Acl acl = new Acl();

    /**
     * 集群配置类
     * 包含MQTT Broker集群相关的所有配置项
     */
    @Data
    public static class Cluster {

        /** 是否启用集群模式 */
        private boolean enabled;

        /** 当前节点ID，用于集群中唯一标识本节点 */
        private String nodeId;

        /** 集群通信主题，默认wcdk:mqtt:broker:cluster */
        private String topic = "wcdk:mqtt:broker:cluster";

        /** 集群通信通道类型，支持TCP和GRPC */
        private Channel channel = Channel.TCP;

        /** 节点角色，MASTER为从节点，SLAVE为从节点 */
        private NodeRole role = NodeRole.SLAVE;

        /** 主节点配置，从节点需要连接主节点 */
        private MasterNode master = new MasterNode();

        /** 多个主节点地址，从节点会分别建立集群连接 */
        private List<MasterNode> masters = new ArrayList<>();

        /** 集群通信绑定地址 */
        private String bindHost = "0.0.0.0";

        /** 集群通信绑定端口 */
        private int bindPort = 28883;

        /** 集群通信认证用户名 */
        private String username;

        /** 集群通信认证密码 */
        private String password;

        /** 对等节点列表，主节点需要配置所有从节点地址 */
        private List<String> peers = new ArrayList<>();

        /** 是否启用全局会话，开启后客户端会话在集群中共享 */
        private boolean globalSession = false;

        /** 是否启用分布式保留消息，开启后保留消息在集群中同步 */
        private boolean distributedRetained = true;

        /** 是否启用离线消息队列，开启后为离线客户端缓存消息 */
        private boolean offlineQueue = true;

        /** 离线消息队列最大消息数 */
        private int maxOfflineMessages = 1000;

        /** 心跳间隔（毫秒），节点定期向集群广播状态 */
        private long heartbeatIntervalMillis = 10000;

        /** 节点超时时间（毫秒），超过此时间未收到心跳则认为节点下线 */
        private long nodeTimeoutMillis = 30000;

        /** 投递确认超时时间（毫秒），等待其他节点确认消息投递 */
        private long deliveryAckTimeoutMillis = 3000;

        /** 是否启用集群消息投递确认（ACK），开启后集群间消息投递会等待确认 */
        private boolean deliveryAckEnabled = true;

        /** 是否启用客户端消息确认（ACK），开启后会等待客户端确认消息接收 */
        private boolean clientAckEnabled = true;

        /** 负载均衡配置 */
        private LoadBalance loadBalance = new LoadBalance();
    }

    /**
     * 负载均衡配置类
     * 配置集群负载均衡策略
     */
    @Data
    public static class LoadBalance {

        /** 是否启用负载均衡 */
        private boolean enabled;

        /** 是否将主节点包含在负载均衡候选中 */
        private boolean includeMaster;
    }

    /**
     * 主节点配置类
     * 从节点通过此配置连接主节点
     */
    @Data
    public static class MasterNode {

        /** 主节点地址 */
        private String host;

        /** 主节点集群通信端口 */
        private int port = 28883;

        /** 连接主节点的认证用户名 */
        private String username;

        /** 连接主节点的认证密码 */
        private String password;
    }

    /**
     * 节点角色枚举
     */
    public enum NodeRole {
        /** 主节点，负责协调集群和负载均衡 */
        MASTER,
        /** 从节点，连接主节点并同步状态 */
        SLAVE
    }

    /**
     * 集群通信通道类型枚举
     */
    public enum Channel {
        /** TCP通道，基于Netty实现 */
        TCP,
        /** gRPC通道（暂未实现） */
        GRPC
    }

    /**
     * 访问控制列表配置类
     * 控制客户端的发布订阅权限
     */
    @Data
    public static class Acl {

        /** 是否启用访问控制 */
        private boolean enabled;

        /** 默认策略，当没有匹配规则时使用的策略 */
        private Policy defaultPolicy = Policy.ALLOW;

        /** 访问控制规则列表 */
        private List<Rule> rules = new ArrayList<>();
    }

    /**
     * 访问控制规则类
     * 定义具体的访问控制规则
     */
    @Data
    public static class Rule {

        /** 是否启用此规则 */
        private boolean enabled = true;

        /** 策略：允许或拒绝 */
        private Policy policy = Policy.ALLOW;

        /** 作用范围：全部、发布或订阅 */
        private Action action = Action.ALL;

        /** 匹配的用户名列表，为空则匹配所有用户 */
        private List<String> usernames = new ArrayList<>();

        /** 匹配的客户端ID列表，为空则匹配所有客户端 */
        private List<String> clientIds = new ArrayList<>();

        /** 匹配的主题过滤器列表 */
        private List<String> topicFilters = new ArrayList<>();
    }

    /**
     * 访问控制策略枚举
     */
    public enum Policy {
        /** 允许访问 */
        ALLOW,
        /** 拒绝访问 */
        DENY
    }

    /**
     * 访问控制动作枚举
     */
    public enum Action {
        /** 所有操作（发布和订阅） */
        ALL,
        /** 仅发布操作 */
        PUBLISH,
        /** 仅订阅操作 */
        SUBSCRIBE
    }
}
