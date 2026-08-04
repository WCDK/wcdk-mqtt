# wcdk-mqtt

## 概述

`wcdk-mqtt` 是基于 Spring Boot、Netty 和 WebFlux 实现的 MQTT Broker 服务，提供 MQTT 终端接入、发布订阅、会话管理、集群通信、消息持久化和 HTTP 管理接口。

默认端口：

| 用途 | 默认端口 |
| --- | ---: |
| MQTT 接入 | `1883` |
| HTTP 管理接口 | `38082` |
| 集群 TCP 通道 | `28883` |

## 特性

- 支持 MQTT 3.1、MQTT 3.1.1 和 MQTT 5 CONNECT 协议版本。
- 支持 QoS 0、QoS 1、QoS 2 发布和订阅。
- 支持 Clean Session、Keep Alive、自动重连和遗嘱消息。
- 支持保留消息、离线消息队列和消息持久化。
- 支持用户名密码认证、匿名连接配置和 ACL 访问控制。
- 支持集群/主从、多主节点和主从节点自动重连。
- 支持主节点之间的对等连接和客户端连接代理。
- 支持主从节点负载
- 主节点可设为非工作节点  include-master: false
- 支持消息投递 ACK、客户端 ACK 和 QoS 状态恢复。
- 提供集群节点、集群配置、集群统计和消息管理接口。
- 支持 InfluxDB 消息持久化扩展。
- 支持大 payload 限制、客户端 ID 长度限制、连接 backlog 和 Netty 线程配置。

## 使用方法

### 环境要求

- JDK 21
- Maven 3.9+
- 可选：InfluxDB 2.x

### 编译和启动

```powershell
mvn -f wcdk-mqtt/pom.xml -DskipTests package
java -jar wcdk-mqtt/target/wcdk-mqtt-3.5.16.jar
```

也可以使用 Spring Boot Maven 插件启动：

```powershell
mvn -f wcdk-mqtt/pom.xml spring-boot:run
```

### 单 Broker 配置

```yaml
wcdk:
  mqtt:
    broker:
      enabled: true
      host: 0.0.0.0
      port: 1883
      anonymous: true
      username: admin
      password: wcdk@2026
      max-payload-size: 1048576
      max-client-id-length: 128
      retained-messages: true
      persistence: true
```

客户端使用 `tcp://127.0.0.1:1883` 连接，认证信息为 `admin / wcdk@2026`。

### 两主一从集群

三个实例的 MQTT 端口和集群端口分别为：

| 实例 | 角色 | MQTT 端口 | 集群端口 | 节点 ID |
| --- | --- | ---: | ---: | --- |
| `wcdk-mqtt` | MASTER | `1883` | `28883` | `mqtt-master-1` |
| `wcdk-mqtt2` | MASTER | `1884` | `28884` | `mqtt-master-2` |
| `wcdk-mqtt3` | SLAVE | `1885` | `28885` | `mqtt-slave-1` |

主节点需要互相配置 `peers`：

```yaml
wcdk:
  mqtt:
    broker:
      cluster:
        enabled: true
        role: MASTER
        node-id: mqtt-master-1
        bind-host: 0.0.0.0
        bind-port: 28883
        username: admin
        password: wcdk@2026
        peers:
           - 127.0.0.1:28884
```

第二个主节点将自身信息改为 `mqtt-master-2`、`28884`，并将 `peers` 改为：

```yaml
peers:
  - 127.0.0.1:28883
```

从节点配置多个主节点：

```yaml
wcdk:
  mqtt:
    broker:
      cluster:
        enabled: true
        role: SLAVE
        node-id: mqtt-slave-1
        bind-host: 0.0.0.0
        bind-port: 28885
        username: admin
        password: wcdk@2026
        masters:
          - host: 127.0.0.1
            port: 28883
            username: admin
            password: wcdk@2026
          - host: 127.0.0.1
            port: 28884
            username: admin
            password: wcdk@2026
```

如果节点不在同一台机器，将 `127.0.0.1` 替换为实际主机 IP。集群认证使用 `cluster.username` 和 `cluster.password`，三个节点必须保持一致。

建议启动顺序为两个主节点，再启动从节点。查看集群状态：

```text
GET http://127.0.0.1:38082/wcdk/mqtt/cluster/nodes
GET http://127.0.0.1:38082/wcdk/mqtt/cluster/config
GET http://127.0.0.1:38082/wcdk/mqtt/cluster/stats
```

### MQTT 客户端多 Broker

依赖 `wcdk-iot-protocol-mqtt` 的客户端可以配置多个 Broker：

```yaml
wcdk:
  mqtt:
    server-uri: tcp://127.0.0.1:1883
    server-uris:
      - tcp://127.0.0.1:1883
      - tcp://127.0.0.1:1884
      - tcp://127.0.0.1:1885
    client-id: wcdk-iot-client
    username: admin
    password: wcdk@2026
```

`server-uri` 保留为首选地址和兼容配置。`server-uris` 配置多个地址后，Paho 客户端会在连接失败或自动重连时尝试其他 Broker。

### ACL 配置

```yaml
wcdk:
  mqtt:
    broker:
      acl:
        enabled: true
        default-policy: DENY
        rules:
          - enabled: true
            policy: ALLOW
            action: SUBSCRIBE
            usernames:
              - admin
            topic-filters:
              - devices/+/telemetry
    
```

ACL 支持 `PUBLISH`、`SUBSCRIBE` 和 `ALL` 操作，并可按用户名、客户端 ID 和 Topic 过滤器匹配。

### 常用管理接口

管理接口默认由 Spring WebFlux 提供，完整接口可通过 Knife4j 或 OpenAPI 查看：

```text
http://127.0.0.1:38082/swagger-ui.html
http://127.0.0.1:38082/doc.html
```

集群接口前缀为 `/wcdk/mqtt/cluster`，消息和客户端管理接口可通过 Swagger 页面查看。

## 优势

- **协议接入性能高**：使用 Netty NIO 事件驱动模型，减少线程阻塞和连接开销。
- **故障转移能力强**：Broker 集群支持主节点互连、从节点多主连接和重连；客户端支持多 Broker 地址。
- **消息可靠性完整**：覆盖 QoS、ACK、持久化、离线队列和会话状态恢复。
- **部署方式灵活**：可以单 Broker 部署，也可以按两主一从或多主节点部署。
- **配置粒度细**：可独立配置 MQTT、HTTP、集群、ACL、InfluxDB、线程池和 payload 限制。
- **可观测性完善**：提供节点状态、连接状态、会话、消息、保留消息和离线消息统计。
- **扩展成本低**：Broker 事件、消息持久化和配置同步通过监听器及服务组件扩展。

## 已有能力

### MQTT 协议能力

- CONNECT、CONNACK、PUBLISH、PUBACK、PUBREC、PUBREL、PUBCOMP。
- SUBSCRIBE、SUBACK、UNSUBSCRIBE、UNSUBACK。
- PINGREQ、PINGRESP、DISCONNECT。
- QoS 0/1/2 状态管理和消息确认。
- Topic 名称及通配符过滤器校验。
- Retain、Will、Clean Session 和 Keep Alive。

### 会话和消息能力

- 客户端在线会话注册和注销。
- 订阅关系维护。
- 保留消息存储和订阅时推送。
- 离线消息队列。
- QoS 1、QoS 2 未完成消息状态恢复。
- 消息入队、重试、过期和容量限制。
- 集群消息广播、定向投递和投递确认。

### 集群能力

- MASTER 和 SLAVE 节点角色。
- 主节点 peers 对等连接。
- 从节点 `masters` 多主节点连接。
- 集群 TCP 通道认证。
- 心跳、节点存活判断和自动重连。
- 客户端会话归属和集群节点状态同步。
- 主节点负载均衡和 MQTT TCP 连接代理。
- 集群节点、配置和统计 HTTP 查询。

### 安全与运维能力

- 用户名密码认证。
- 匿名连接开关。
- Topic ACL。
- 客户端 ID 和 payload 长度限制。
- 日志输出连接地址、代理目标、失败原因和集群节点状态。
- InfluxDB 消息持久化扩展。

## 实现技术

| 技术 | 用途 |
| --- | --- |
| Spring Boot | 应用启动、配置绑定和 Bean 装配 |
| Spring WebFlux | HTTP 管理接口和响应式 Web 服务 |
| Netty | MQTT TCP 接入、协议编解码、集群 TCP 通道和连接代理 |
| Netty MQTT Codec | MQTT 报文解码与编码 |
| Java 21 | 运行时和并发基础能力 |
| Lombok | 配置对象和数据对象样板代码 |
| ConcurrentHashMap / ConcurrentLinkedQueue | 会话、消息、节点和 ACK 状态的并发存储 |
| InfluxDB Client | 可选消息时序持久化 |
| Swagger / Knife4j | HTTP 接口文档和调试 |
| Maven | 模块构建和依赖管理 |

核心代码分层：

- `core.core.ReactorMqttBroker`：MQTT TCP 服务启动和连接接入。
- `core.core.MqttBrokerChannelHandler`：CONNECT、PUBLISH、SUBSCRIBE 等 MQTT 行为处理。
- `core.core.MqttBrokerSession`：客户端会话、订阅和 QoS 状态。
- `core.core.MqttBrokerSessionRegistry`：会话注册、保留消息和消息分发。
- `core.cluster.MqttClusterTcpChannel`：集群节点连接、认证、心跳和重连。
- `core.core.MqttBrokerClusterManager`：节点状态、集群消息和会话协调。
- `core.core.MqttTcpProxyHandler`：主节点间 MQTT TCP 连接代理。
- `controller.MqttClusterController`：集群查询和管理接口。

## 注意事项

- 多实例部署时，每个节点的 MQTT 端口、集群端口和节点 ID 必须唯一。
- 集群认证账号必须在所有节点保持一致。
- 使用 `0.0.0.0` 作为绑定地址时，应通过 `bind-host` 或实际主机地址配置可被其他节点访问的地址。
- 集群端口必须在防火墙和容器网络中放行。
- 修改配置后必须重启对应 Broker 实例。
- 生产环境不建议使用默认密码，应通过外部配置覆盖账号密码。