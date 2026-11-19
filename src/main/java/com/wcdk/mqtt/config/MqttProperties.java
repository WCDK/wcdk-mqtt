package com.wcdk.mqtt.config;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/***
* MQTT Broker 全局配置，绑定 wcdk.mqtt 前缀下的配置项。
* @author wcdk
***/
@Data
@ConfigurationProperties(prefix = "wcdk.mqtt")
public class MqttProperties {

    /** MQTT 接入配置 */
    private Broker broker = new Broker();

    /** 认证与控制台安全配置 */
    private Security security = new Security();

    /** 访问控制列表配置 */
    private Acl acl = new Acl();

    /** 集群配置 */
    private Cluster cluster = new Cluster();

    /** 管理控制台配置 */
    private Console console = new Console();

    /***
    * MQTT 接入相关配置。
    * @author wcdk
    ***/
    @Data
    public static class Broker {

        /** 是否启用 MQTT Broker */
        private boolean enabled = true;

        /** MQTT 监听地址 */
        private String host = "0.0.0.0";

        /** MQTT 监听端口 */
        private int port = 1883;

        /** 单条消息最大负载字节数，默认按遥测场景收紧到 64KB，降低大消息攻击面与内存放大 */
        private int maxPayloadSize = 64 * 1024;

        /** 客户端 ID 最大长度 */
        private int maxClientIdLength = 128;

        /** 是否支持保留消息 */
        private boolean retainedMessages = true;

        /** 是否为离线持久会话缓存消息 */
        private boolean offlineQueue = true;

        /** 单个离线会话最大缓存消息数，默认 100，避免大量离线会话时内存被成倍放大 */
        private int maxOfflineMessages = 100;

        /** 出站消息确认超时毫秒，超过后重投 */
        private long retransmitTimeoutMillis = 10_000;

        /** 出站消息最大重投次数 */
        private int maxRetransmitAttempts = 3;

        /** 会话扫描间隔秒数，用于 Keep Alive 与重投检查 */
        private int sweepIntervalSeconds = 5;

        /** 单连接出站缓冲消息数上限，默认 256，超出判定为慢消费者并断开连接 */
        private int maxOutboundBuffer = 256;

        /** 最近日志消息容量 */
        private int messageLogCapacity = 500;
    }

    /***
    * 认证与控制台安全配置。
    * @author wcdk
    ***/
    @Data
    public static class Security {

        /** 是否允许匿名 MQTT 连接 */
        private boolean anonymous = true;

        /** MQTT 认证用户名 */
        private String username;

        /** MQTT 认证密码 */
        private String password;

        /** 控制台登录用户名，为空时复用 MQTT 用户名 */
        private String consoleUsername;

        /** 控制台登录密码，为空时复用 MQTT 密码 */
        private String consolePassword;
    }

    /***
    * 访问控制列表配置。
    * @author wcdk
    ***/
    @Data
    public static class Acl {

        /** 是否启用访问控制 */
        private boolean enabled;

        /** 未命中任何规则时的默认策略 */
        private Policy defaultPolicy = Policy.ALLOW;

        /** 规则列表，按顺序匹配，首个命中生效 */
        private List<AclRule> rules = new ArrayList<>();
    }

    /***
    * 单条访问控制规则。
    * @author wcdk
    ***/
    @Data
    public static class AclRule {

        /** 是否启用该规则 */
        private boolean enabled = true;

        /** 命中后的策略 */
        private Policy policy = Policy.ALLOW;

        /** 作用的操作类型 */
        private Action action = Action.ALL;

        /** 匹配的用户名列表，支持 * 通配，为空表示不限制 */
        private List<String> usernames = new ArrayList<>();

        /** 匹配的客户端 ID 列表，支持 * 通配，为空表示不限制 */
        private List<String> clientIds = new ArrayList<>();

        /** 匹配的主题过滤器列表，为空表示不限制 */
        private List<String> topicFilters = new ArrayList<>();
    }

    /***
    * 集群配置，所有节点对等，无主从角色之分。
    * @author wcdk
    ***/
    @Data
    public static class Cluster {

        /** 是否启用集群 */
        private boolean enabled;

        /** 本节点唯一 ID，为空时自动生成 */
        private String nodeId;

        /** 集群端口监听地址 */
        private String bindHost = "0.0.0.0";

        /** 集群端口监听端口 */
        private int bindPort = 28883;

        /** 对外公布的主机地址，为空时按监听地址自动推断 */
        private String advertiseHost;

        /** 对等节点地址列表，格式 host:port */
        private List<String> peers = new ArrayList<>();

        /** 集群通道认证用户名，为空表示不认证 */
        private String username;

        /** 集群通道认证密码 */
        private String password;

        /**
         * 是否强制集群通道认证：启用集群时必须配置 username/password，
         * 否则启动失败，避免无凭据节点注入会话与消息事件。
         */
        private boolean requireAuth = true;

        /** 是否启用集群通道 TLS */
        private boolean tlsEnabled;

        /** 集群通道证书路径（PEM，含证书链） */
        private String tlsCertPath;

        /** 集群通道私钥路径（PEM，PKCS#8） */
        private String tlsKeyPath;

        /** 集群互信 CA 证书路径（PEM），配置后服务端要求客户端证书（双向认证） */
        private String tlsTrustCertPath;

        /**
         * 保留消息/会话删除墓碑保留小时数。
         * 必须大于节点最长允许离线时间，否则长时间离线节点回归后可能复活已删除数据。
         */
        private long tombstoneTtlHours = 168;

        /** 心跳间隔毫秒 */
        private long heartbeatIntervalMillis = 5000;

        /** 节点判定下线的超时毫秒 */
        private long nodeTimeoutMillis = 15000;

        /** 对等节点断线重连间隔毫秒 */
        private long reconnectDelayMillis = 3000;

        /** 是否启用多数派校验，关闭后退化为尽力而为的集群同步 */
        private boolean quorumEnabled = true;

        /**
         * 集群固定成员数（含本节点），0 表示按 peers 非空数量 + 1 计算。
         * 固定成员数用于在分区时计算多数派阈值，不能依赖动态发现结果，
         * 否则孤立节点会误认为自己是多数派。
         */
        private int expectedNodes;

        /** 集群编解码/事件处理线程数，0 表示按 CPU 自动计算（2–4） */
        private int codecThreads;

        /** 每个对等连接的待处理事件队列上限，溢出时断开连接并触发重新同步 */
        private int eventQueueCapacity = 4096;
    }

    /***
    * 管理控制台配置。
    * @author wcdk
    ***/
    @Data
    public static class Console {

        /** 是否启用网页控制台 */
        private boolean enabled = true;

        /** 概览统计缓存毫秒数，0 表示每次请求实时重新计算 */
        private long overviewCacheMillis = 1000;
    }

    /***
    * 访问控制策略枚举。
    * @author wcdk
    ***/
    public enum Policy {
        /** 允许 */
        ALLOW,
        /** 拒绝 */
        DENY
    }

    /***
    * 访问控制操作类型枚举。
    * @author wcdk
    ***/
    public enum Action {
        /** 发布与订阅 */
        ALL,
        /** 仅发布 */
        PUBLISH,
        /** 仅订阅 */
        SUBSCRIBE
    }
}
