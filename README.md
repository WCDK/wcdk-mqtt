# wcdk-mqtt

基于 **Java Reactor（Spring Boot + Reactor Netty）** 从零实现的响应式 MQTT Broker，支持 **MQTT 3.1 / 3.1.1 / 5.0** 协议与**对等集群**，内置 REST 管理接口与网页控制台。

- 全链路非阻塞：MQTT 接入、集群通道、管理接口均为 Reactor Netty 响应式实现，无 `.block()`、无手动线程阻塞等待。
- **纯内存存储**：会话状态、保留消息、离线队列全部保存在内存中，重启即重置，集群节点之间自动同步；**不依赖任何外部存储（无数据库、无 InfluxDB、无消息队列）**。
- 对等集群：所有节点地位相同，**无 MASTER/WORKER 之分**，配置 `node-id` / `bind-port` / `peers` 即可组网，单实例可关闭集群。

## 一、功能特性

| 分类     | 说明                                                                 |
| -------- | -------------------------------------------------------------------- |
| 协议     | MQTT 3.1 / 3.1.1 / 5.0，CONNECT / PUBLISH / SUBSCRIBE / UNSUBSCRIBE / PUBACK / PUBREC / PUBREL / PUBCOMP / PINGREQ / DISCONNECT |
| QoS      | QoS 0、1、2 全支持，含入站/出站双方向的报文去重与超时重投            |
| 会话     | Clean Session / 持久会话、MQTT5 Session Expiry Interval（过期后自动清理会话）、Keep Alive 超时断开、同 ID 重连自动接管、离线消息缓冲 |
| 消息     | 保留消息（Retain）、遗嘱消息（Will）、通配符订阅 `+` / `#`、按 QoS 降级投递 |
| 安全     | 用户名密码认证、匿名开关、主题级读写 ACL                              |
| 集群     | 对等节点互拨、心跳保活、重复连接自动消解、会话/保留消息/发布实时同步、跨节点点对点投递与强制下线 |
| 管理端   | REST 管理接口 + 网页控制台（表单登录、概览、客户端、消息、保留消息、集群、模拟发布），SSE 实时推送 |

## 二、快速开始

### 环境要求

- JDK 21+
- Maven 3.9+

### 本地构建运行

```bash
mvn -o clean package -DskipTests
java -jar target/wcdk-mqtt-*.jar
```

或直接开发模式启动：

```bash
mvn -o spring-boot:run
```

启动后：

- 网页控制台：<http://localhost:38082/> （默认账号 `admin/admin`，生产环境请修改）
- MQTT 接入地址：`localhost:1883`
- 集群通道地址：`localhost:28883`（默认单实例不启用集群）

### 运行测试

```bash
mvn -o test
```

## 三、端口说明

| 端口  | 用途                                |
| ----- | ----------------------------------- |
| 38082 | HTTP 管理接口与网页控制台           |
| 1883  | MQTT 接入端口                       |
| 28883 | 集群对等通道端口                    |

## 四、配置说明

配置前缀 `wcdk.mqtt`，完整示例见 `src/main/resources/application.yml`。

```yaml
wcdk:
  mqtt:
    broker:
      enabled: true              # 是否启用 MQTT 接入
      host: 0.0.0.0
      port: 1883
      max-payload-size: 1048576   # 单条消息最大负载（字节）
      max-offline-messages: 1000  # 单个离线会话最大缓存
      max-outbound-buffer: 4096   # 单连接出站缓冲上限，写满判定慢消费者并断开
      retransmit-timeout-millis: 10000
      max-retransmit-attempts: 3
      message-log-capacity: 500   # 控制台最近消息容量
    security:
      anonymous: true             # 是否允许匿名 MQTT 连接
      username:                   # MQTT 认证账号（为空则不校验）
      password:
      console-username: admin     # 控制台登录账号
      console-password: admin
    acl:
      enabled: false              # 主题级访问控制开关
      default-policy: allow
      rules: []
    cluster:
      enabled: false              # 单实例保持 false
      node-id: node-1             # 节点唯一 ID
      bind-host: 0.0.0.0
      bind-port: 28883
      advertise-host:             # 对外公布地址，多节点部署必填
      peers: []                   # 对等地址列表，格式 host:port
      username:                   # 集群通道认证账号（为空则不校验，配置后所有节点必须一致）
      password:
      heartbeat-interval-millis: 5000
      node-timeout-millis: 15000
      reconnect-delay-millis: 3000
    console:
      enabled: true
```

Spring Boot 标准的环境变量覆盖同样适用，例如：

```bash
WCDK_MQTT_CLUSTER_ENABLED=true \
WCDK_MQTT_CLUSTER_NODE-ID=node-1 \
WCDK_MQTT_CLUSTER_PEERS_0=192.168.1.11:28883 \
java -jar target/wcdk-mqtt-*.jar
```

### ACL 规则示例

```yaml
wcdk:
  mqtt:
    acl:
      enabled: true
      default-policy: deny
      rules:
        - policy: allow
          action: all
          usernames: [ "admin" ]
          topicFilters: [ "#" ]
        - policy: allow
          action: subscribe
          usernames: [ "sensor" ]
          topicFilters: [ "sensors/#" ]
```

## 五、集群部署

集群为**对等模型**，任意两个节点互相配置 `peers` 即可成组：

```yaml
# 节点 A
wcdk.mqtt.cluster.enabled=true
wcdk.mqtt.cluster.node-id=node-a
wcdk.mqtt.cluster.advertise-host=192.168.1.10
wcdk.mqtt.cluster.peers[0]=192.168.1.11:28883

# 节点 B
wcdk.mqtt.cluster.enabled=true
wcdk.mqtt.cluster.node-id=node-b
wcdk.mqtt.cluster.advertise-host=192.168.1.11
wcdk.mqtt.cluster.peers[0]=192.168.1.10:28883
```

工作方式：

1. 每个节点在 `bind-port` 上监听，并按 `peers` 周期性主动拨号；
2. 双方通过 `HELLO` 握手交换节点身份，配置了 `username` 时校验集群凭据，认证失败即断开；若出现重复连接，按「节点 ID 字典序较小的一方保留自己拨出的连接」自动消解；
3. 心跳由写空闲触发，读空闲超时即判定对端下线并断开，守护线程周期重拨；
4. 客户端会话归属、保留消息、消息发布、强制下线指令在节点间实时同步；
5. 客户端连接任意节点均可服务，跨节点的点对点消息按会话归属路由。

Docker 单节点与多节点部署详见 [docker.md](docker.md)。

## 六、管理接口

所有接口需登录（表单登录或 HTTP Basic），未认证访问 `/api/**` 返回 `401` JSON。

| 方法   | 路径                              | 说明                             |
| ------ | --------------------------------- | -------------------------------- |
| GET    | `/api/overview`                   | Broker 运行概览统计              |
| GET    | `/api/me`                         | 当前登录用户                     |
| GET    | `/api/clients`                    | 客户端列表（支持关键字与在线过滤） |
| GET    | `/api/clients/{clientId}`         | 客户端详情                       |
| GET    | `/api/clients/{clientId}/subscriptions` | 客户端订阅明细             |
| DELETE | `/api/clients/{clientId}`         | 强制下线，`clear=true` 同时清除会话 |
| GET    | `/api/subscriptions`              | 全部订阅聚合查询                 |
| GET    | `/api/messages`                   | 最近消息查询                     |
| DELETE | `/api/messages`                   | 按条件或 ID 清理最近消息         |
| GET    | `/api/retained`                   | 保留消息列表                     |
| DELETE | `/api/retained`                   | 删除指定主题保留消息             |
| POST   | `/api/publish`                    | 模拟发布（支持 Base64 负载与点对点投递） |
| GET    | `/api/cluster`                    | 集群与对等节点状态               |
| GET    | `/api/stream`                     | 消息与会话实时事件流（SSE）      |

响应统一包装：

```json
{ "code": 0, "message": "操作成功", "data": { } }
```

## 七、Docker 部署

```bash
# 构建镜像
docker build -t wcdk1024/wcdk-mqtt:1.0.0 .

# 单节点
docker compose up -d

# 三节点集群
docker compose --profile cluster up -d
```

详见 [docker.md](docker.md)。

## 八、项目结构

```
src/main/java/com/wcdk/mqtt/
├── MqttBrokerApplication.java   # 应用入口
├── config/                      # 配置属性、安全链、Web 路由、Bean 装配
├── core/                        # 会话、注册表、主题匹配、ACL、保留消息、消息日志
├── protocol/                    # MQTT 接入服务端、连接协议状态机、会话巡检
├── cluster/                     # 对等集群：握手、心跳、事件路由、状态同步
├── controller/                  # REST 管理接口
├── service/                     # 发布与概览业务门面
└── web/                         # 控制台 SSE 事件总线
src/main/resources/
├── application.yml              # 默认配置
└── static/                      # 网页控制台（index.html / login.html / app.js）
```

## 九、测试

```bash
# 全量测试
mvn -o test

# 单个测试
mvn -o -q test -Dtest=MqttPipelineSpikeTest
```

测试报告位于 `target/surefire-reports/`。

## 十、License

MIT
