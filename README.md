# wcdk-mqtt

> 基于 Spring Boot 3 + Reactor Netty 的响应式 MQTT Broker，内置对等集群、可视化管理控制台与集群风险评估。

`wcdk-mqtt` 是一个面向物联网与遥测场景的轻量级 MQTT Broker。它使用 Java 21 与 Reactor Netty 构建，所有协议处理都在事件循环中完成，不依赖阻塞式 IO；集群采用无主对等模型，节点之间自动同步会话、订阅与消息，并支持多数派只读保护、定向转发、状态版本对账与可选的双向 TLS。

---

## 目录

- [项目简介](#项目简介)
- [核心特性](#核心特性)
- [技术栈](#技术栈)
- [系统架构](#系统架构)
- [快速开始](#快速开始)
- [网页控制台](#网页控制台)
- [REST API](#rest-api)
- [配置说明](#配置说明)
- [集群设计](#集群设计)
- [可靠性设计](#可靠性设计)
- [测试与验证](#测试与验证)
- [项目结构](#项目结构)
- [生产部署建议](#生产部署建议)
- [已知限制](#已知限制)
- [相关文档](#相关文档)

---

## 项目简介

| 项目 | 说明 |
|------|------|
| 项目名称 | `wcdk-mqtt` |
| 运行形态 | 单实例或对等集群（无 MASTER/WORKER） |
| 协议支持 | MQTT 3.1 / 3.1.1 / 5.0 |
| 默认端口 | `1883` MQTT、`38082` HTTP 管理接口与控制台、`28883` 集群通道 |
| 状态存储 | 内存（会话、订阅、保留消息、离线队列、消息日志） |
| 部署方式 | 本地 JAR、Docker、Docker Compose |
| 技术基线 | Java 21、Spring Boot 3.5.16、Reactor Netty、Netty MQTT Codec |

典型使用场景：

- 设备接入与遥测数据采集；
- 多节点水平扩展的 MQTT 接入层；
- 需要网页化管理客户端、订阅、消息与保留消息的运维场景；
- 需要感知集群分区、节点心跳与脑裂风险的部署环境。

---

## 核心特性

### MQTT 协议能力

- **完整 QoS 支持**：QoS 0 / 1 / 2 的发布、订阅与确认流程，包含 QoS 2 两阶段握手与去重。
- **MQTT 版本**：支持 MQTT 3.1、3.1.1、5.0；MQTT 5 支持会话过期时间与连接返回码。
- **会话管理**：持久会话、清理会话、会话接管、离线消息队列、会话过期清理。
- **保留消息**：主题级最新保留消息，支持空负载墓碑删除。
- **遗嘱消息**：连接异常断开时按配置发布 Will 消息，并进行主题与 ACL 校验。
- **订阅通配符**：支持 `+`、`#` 与 `$SYS` 系统主题隔离语义。
- **Keep Alive**：按客户端心跳与扫描周期清理超时会话。
- **消息大小限制**：默认单条负载上限 64KB，可按业务调整。
- **慢消费者保护**：单连接出站缓冲默认 256 条，超限主动断开，避免拖垮节点内存。

### 管理与可观测能力

- **网页控制台**：概览、客户端、消息、保留消息、集群、模拟发布六个页签。
- **实时事件流**：通过 SSE 推送消息发布、会话上下线等事件，断线自动重连。
- **最近消息日志**：按主题、QoS、时间范围查询，支持按 ID 或条件清理。
- **集群状态查询**：节点连接数、连接状态、多数派状态、待补发消息数与丢弃计数。
- **脑裂风险评估**：0–100 风险评分，输出 `SAFE` / `LOW` / `MEDIUM` / `HIGH` / `EXTREME` 等级与处置建议。
- **管理 API**：客户端踢下线、订阅查询、消息查询、保留消息删除、模拟发布等。

### 集群能力

- **对等无主架构**：每个节点都可独立读写，节点间两两互连。
- **自动重连与去重**：心跳保活、断线重拨，重复连接按节点 ID 字典序自动消解。
- **会话与订阅同步**：新节点加入时分批同步全量快照，运行期增量同步会话变化。
- **定向发布转发**：仅向存在匹配远端订阅者的节点转发，避免全集群广播放大。
- **多数派保护**：未达成多数派时节点进入只读，拒绝新连接与持久化写入，降低脑裂影响。
- **状态版本对账**：会话快照与保留消息携带单调版本，迟到旧快照不会覆盖新状态。
- **删除墓碑**：保留消息与会话删除动作保留 7 天（可配置），避免离线节点回归后复活已删除数据。
- **发布补发缓冲**：对等节点暂不可达时按节点暂存发布消息，重连后补发，并暴露丢弃计数。
- **集群通道认证**：默认强制要求用户名/密码，未配置时启动失败。
- **集群通道 TLS**：支持单向/双向 TLS（PEM 证书、私钥、互信 CA）。
- **快照批量同步**：会话与保留消息按 100 条/批同步，避免新节点加入时瞬时撑爆对端缓冲。

### 安全能力

- **MQTT 认证**：默认允许匿名，可切换为固定用户名/密码认证。
- **ACL 访问控制**：按用户名、客户端 ID、主题过滤器与操作类型（发布/订阅）匹配规则，支持 `*` 通配与默认策略。
- **控制台安全**：Spring Security 表单登录，默认账号 `admin/admin`，支持与 MQTT 凭据复用。
- **集群通道安全**：认证默认开启，TLS 可选，证书缺失时快速失败而不是静默明文启动。

---

## 技术栈

| 组件 | 版本/说明 |
|------|-----------|
| Java | 21 |
| Spring Boot | 3.5.16 |
| 响应式 Web | Spring WebFlux（Netty） |
| MQTT 编解码 | `netty-codec-mqtt` |
| 网络层 | Reactor Netty `TcpServer` / `TcpClient` |
| 安全 | Spring Security |
| 构建 | Maven |
| 测试 | JUnit 5、AssertJ、Eclipse Paho（集成测试客户端） |
| 部署 | Docker 多阶段构建、Docker Compose |

---

## 系统架构

```text
                     ┌─────────────────────────────────────────┐
                     │              wcdk-mqtt 节点              │
                     │                                         │
  MQTT 客户端 ──────▶│  MqttBrokerServer (Reactor Netty)       │
  1883/tcp           │        │                                │
                     │        ▼                                │
                     │  MqttClientConnection (协议状态机)       │
                     │        │                                │
                     │        ▼                                │
                     │  MqttBrokerService ──▶ MqttSessionRegistry
                     │        │                  │    │        │
                     │        │                  │    └──▶ MqttRetainedStore
                     │        │                  └───────▶ MqttSession
                     │        │                                │
                     │        ├──▶ MqttMessageLog               │
                     │        └──▶ MqttClusterManager ──────────┼──▶ 集群通道 28883
                     │                                         │
  浏览器/运维 ──────▶│  Spring WebFlux + Security              │
  38082/http         │  REST API / SSE / 静态控制台             │
                     └─────────────────────────────────────────┘
```

发布路径：

1. 客户端发布消息 → `MqttBrokerService.publish`；
2. 校验主题与多数派状态；
3. 本地按订阅索引投递，并写入保留消息/离线队列/消息日志；
4. 按远端订阅索引定向广播到命中的对等节点；
5. 对等节点只做本地投递，不再回环广播，避免消息风暴。

集群通道使用 JSON 行协议，编解码与 Base64 转换在独立有序线程池中执行，事件循环只负责读写与心跳刷新。

---

## 快速开始

### 环境要求

- JDK 21+
- Maven 3.9+
- Docker 24+（可选）

### 本地运行

```bash
mvn test
mvn spring-boot:run
```

启动后：

- MQTT 接入：`localhost:1883`
- 网页控制台：<http://localhost:38082/>
- 默认控制台账号：`admin/admin`

### Docker 单节点

```bash
docker build -t wcdk1024/wcdk-mqtt:1.0.0 .
docker compose up -d
```

仓库自带 `.env`（`COMPOSE_PROFILES=single`），等价于：

```bash
docker compose --profile single up -d
```

查看状态与日志：

```bash
docker compose ps
docker compose logs -f mqtt-broker
```

### Docker 三节点集群

```bash
docker compose --profile cluster up -d
```

节点端口映射：

| 节点 | 控制台 | MQTT | 集群通道 |
|------|--------|------|----------|
| node-1 | 38082 | 1883 | 28883 |
| node-2 | 38083 | 1884 | 28884 |
| node-3 | 38084 | 1885 | 28885 |

集群节点已在 compose 中配置一致的默认凭据：

```yaml
WCDK_MQTT_CLUSTER_USERNAME: wcdk-cluster
WCDK_MQTT_CLUSTER_PASSWORD: change-me-cluster-secret
```

生产环境务必替换为高强度随机值，并保证所有节点一致。

### 连接测试

使用 `mosquitto` 客户端：

```bash
# 订阅
mosquitto_sub -h localhost -p 1883 -t 'demo/#' -v

# 发布
mosquitto_pub -h localhost -p 1883 -t 'demo/hello' -m 'hello wcdk-mqtt'
```

默认允许匿名连接；关闭匿名后需通过 `-u` / `-P` 指定凭据。

---

## 网页控制台

访问 `http://localhost:38082/`，默认账号 `admin/admin`。

| 页签 | 功能 |
|------|------|
| 概览 | 运行时长、在线/离线会话、订阅总数、保留消息、离线缓冲、节点信息 |
| 客户端 | 本地与远端客户端列表、在线状态、订阅详情、踢下线 |
| 消息 | 最近消息查询、按主题/QoS/时间过滤、批量清理 |
| 保留消息 | 查看全部保留消息、按主题删除 |
| 集群 | 节点连接状态、多数派与只读状态、待补发/丢弃计数、风险评分 |
| 发布消息 | 通过页面模拟发布，支持 QoS、Retained、Base64 负载与指定客户端 |

控制台通过 `/api/stream` 的 SSE 事件流实时刷新，页面无需手动轮询消息。

---

## REST API

所有管理接口默认返回统一结构：

```json
{
  "success": true,
  "data": {},
  "timestamp": "2026-11-19T03:00:00Z"
}
```

### 概览与会话

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/overview` | Broker 运行概览 |
| GET | `/api/me` | 当前登录用户与控制台状态 |
| GET | `/api/clients?keyword=&online=` | 客户端列表（含集群远端会话） |
| GET | `/api/clients/{clientId}` | 客户端详情 |
| GET | `/api/clients/{clientId}/subscriptions` | 客户端订阅列表 |
| DELETE | `/api/clients/{clientId}` | 踢下线指定客户端 |
| GET | `/api/subscriptions` | 全部订阅 |

### 消息与保留消息

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/messages?topicFilter=&qos=&from=&to=&limit=` | 查询最近消息 |
| DELETE | `/api/messages?ids=&topicFilter=&qos=&from=&to=` | 按 ID 或条件清理消息 |
| GET | `/api/retained` | 查询保留消息 |
| DELETE | `/api/retained?topic=` | 删除指定主题的保留消息 |
| POST | `/api/publish` | 模拟发布消息 |

发布示例：

```bash
curl -X POST http://localhost:38082/api/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"demo/hello","payload":"hello","qos":1,"retained":false}'
```

指定 `clientId` 时进行点对点投递；本地不存在该客户端时自动经集群转发：

```json
{
  "clientId": "device-001",
  "topic": "cmd/device-001",
  "payload": "reboot",
  "qos": 1
}
```

### 实时事件

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/stream` | SSE 消息与会话事件流，每 15 秒发送心跳 |

### 集群

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/cluster` | 集群节点、多数派、补发/丢弃计数、认证与 TLS 状态 |
| GET | `/api/cluster/risk` | 完整脑裂风险评估 |
| GET | `/api/cluster/risk/refresh` | 刷新风险评估 |
| GET | `/api/cluster/risk/nodes` | 节点详细状态 |
| GET | `/api/cluster/risk/peers` | 对等连接详情 |
| GET | `/api/cluster/risk/config` | 集群配置信息 |

风险等级：

| 分数 | 等级 | 含义 |
|------|------|------|
| 0–19 | `SAFE` | 集群正常 |
| 20–39 | `LOW` | 连接率轻度下降 |
| 40–59 | `MEDIUM` | 部分节点响应延迟 |
| 60–79 | `HIGH` | 心跳超时或接近单点 |
| 80–100 | `EXTREME` | 检测到网络分区或多数派丢失 |

---

## 配置说明

配置前缀为 `wcdk.mqtt`，支持 `application.yml` 或环境变量（Spring 宽松绑定，例如 `wcdk.mqtt.broker.port` 对应 `WCDK_MQTT_BROKER_PORT`）。

### MQTT 接入

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `broker.enabled` | `true` | 是否启用 MQTT 接入 |
| `broker.host` | `0.0.0.0` | 监听地址 |
| `broker.port` | `1883` | 监听端口 |
| `broker.max-payload-size` | `65536` | 单条消息最大负载字节数 |
| `broker.max-client-id-length` | `128` | 客户端 ID 最大长度 |
| `broker.retained-messages` | `true` | 是否支持保留消息 |
| `broker.offline-queue` | `true` | 是否为持久会话缓存离线消息 |
| `broker.max-offline-messages` | `100` | 单会话离线消息上限 |
| `broker.max-outbound-buffer` | `256` | 单连接出站缓冲上限 |
| `broker.retransmit-timeout-millis` | `10000` | QoS 消息确认超时，超时重投 |
| `broker.max-retransmit-attempts` | `3` | 最大重投次数 |
| `broker.sweep-interval-seconds` | `5` | Keep Alive 与重投检查周期 |
| `broker.message-log-capacity` | `500` | 最近消息日志容量 |

### 认证与控制台

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `security.anonymous` | `true` | 是否允许匿名 MQTT 连接 |
| `security.username` | 空 | MQTT 认证用户名 |
| `security.password` | 空 | MQTT 认证密码 |
| `security.console-username` | `admin` | 控制台账号，为空时复用 MQTT 用户名 |
| `security.console-password` | `admin` | 控制台密码，为空时复用 MQTT 密码 |

### ACL

默认关闭。启用后按规则顺序匹配，首个命中生效：

```yaml
wcdk:
  mqtt:
    acl:
      enabled: true
      default-policy: deny
      rules:
        - policy: allow
          action: all
          usernames: ["device-*"]
          client-ids: ["device-*"]
          topic-filters: ["devices/+/telemetry", "devices/+/status"]
        - policy: allow
          action: subscribe
          usernames: ["ops"]
          topic-filters: ["#"]
```

规则字段：

| 字段 | 说明 |
|------|------|
| `policy` | `ALLOW` / `DENY` |
| `action` | `ALL` / `PUBLISH` / `SUBSCRIBE` |
| `usernames` | 用户名匹配，支持 `*` 通配 |
| `client-ids` | 客户端 ID 匹配，支持 `*` 通配 |
| `topic-filters` | 主题过滤器匹配，订阅场景按 ACL 过滤器校验 |

### 集群

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `cluster.enabled` | `false` | 是否启用集群 |
| `cluster.node-id` | 自动生成 | 本节点唯一 ID |
| `cluster.bind-host` | `0.0.0.0` | 集群监听地址 |
| `cluster.bind-port` | `28883` | 集群监听端口 |
| `cluster.advertise-host` | 自动推断 | 对其他节点公布的地址 |
| `cluster.peers` | `[]` | 对等节点列表，格式 `host:port` |
| `cluster.username` / `password` | 空 | 集群通道认证凭据 |
| `cluster.require-auth` | `true` | 强制集群认证，未配置凭据时启动失败 |
| `cluster.tls-enabled` | `false` | 是否启用集群 TLS |
| `cluster.tls-cert-path` | 空 | 证书链 PEM 路径 |
| `cluster.tls-key-path` | 空 | 私钥 PEM（PKCS#8）路径 |
| `cluster.tls-trust-cert-path` | 空 | 互信 CA，配置后要求客户端证书 |
| `cluster.tombstone-ttl-hours` | `168` | 删除墓碑保留小时数 |
| `cluster.heartbeat-interval-millis` | `5000` | 心跳间隔 |
| `cluster.node-timeout-millis` | `15000` | 节点下线判定超时 |
| `cluster.reconnect-delay-millis` | `3000` | 断线重连间隔 |
| `cluster.quorum-enabled` | `true` | 是否启用多数派校验 |
| `cluster.expected-nodes` | `0` | 固定成员数（含本节点），`0` 表示按 peers 推导 |
| `cluster.codec-threads` | `0` | 编解码线程数，`0` 表示按 CPU 自动 |
| `cluster.event-queue-capacity` | `4096` | 单对等连接待处理事件队列上限 |

### 控制台

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `console.enabled` | `true` | 是否启用网页控制台 |
| `console.overview-cache-millis` | `1000` | 概览统计缓存窗口，`0` 表示实时计算 |

---

## 集群设计

### 对等模型

- 所有节点地位相同，无主从之分；
- 每个节点监听 `bind-port`，同时按 `peers` 主动拨号；
- 两个节点可能互相拨号产生重复连接，按节点 ID 字典序保留一方拨出的连接；
- 任一对等节点断开不影响其他节点继续提供服务。

### 多数派与只读

集群按固定成员数计算多数派阈值：

```text
quorum = expected-nodes / 2 + 1
```

- 达成多数派：正常接受连接与读写；
- 未达成多数派：拒绝新 MQTT 连接、拒绝 QoS>0 与 retained 写入，QoS0 直接丢弃；
- 三节点集群建议显式设置 `expected-nodes: 3`，避免孤立节点误判自己仍是多数派。

### 数据同步

- **全量同步**：新节点握手后分批接收会话快照与保留消息（100 条/批）。
- **增量同步**：会话上下线、订阅变化、保留消息更新实时广播。
- **定向发布**：发布时查询远端订阅索引，只转发给真正有订阅者的节点。
- **版本对账**：会话与保留消息携带单调版本，接收端丢弃过期快照。
- **删除墓碑**：删除动作保留 7 天，防止长时间离线节点回归后复活旧数据。
- **断线补发**：节点不可达期间的发布按节点暂存，重连后按序补发；缓冲溢出时丢弃最旧消息并累计计数。

### 连接与线程模型

```text
集群事件循环（Reactor Netty）
        │
        ├── 入站读取 / 心跳刷新
        │        │
        │        ▼
        │   mqtt-cluster-inbound 有序线程池（JSON 解码 + 路由）
        │
        └── 出站写入
                 ▲
                 │
        mqtt-cluster-outbound 有序线程池（JSON/Base64 编码）
```

每个对等连接拥有独立 FIFO 队列，保证同一节点事件顺序；跨节点并行处理。队列达到上限时断开连接并触发重连全量同步，避免无界内存增长。

### 集群安全

- 默认强制认证，集群启用但未配置 `username/password` 时**启动失败**；
- 启用 TLS 后，服务端与客户端均使用配置的证书；配置 `tls-trust-cert-path` 时启用双向认证；
- 证书或私钥缺失会在启动阶段直接失败，不会静默降级为明文。

### 脑裂风险评估

风险评分综合以下指标：

- 已连接对等节点占配置节点数的比例；
- 节点最后心跳时间与超时阈值；
- 是否仅剩单个节点；
- 多数派是否丢失；
- 固定成员数中不可达节点数量。

评估结果同时返回风险因子与分级处置建议，可用于控制台展示或外部监控告警。

---

## 可靠性设计

| 设计点 | 说明 |
|--------|------|
| 单写者串行化 | 集群连接与客户端连接的出站写入均由单写者锁串行化，避免 `FAIL_NON_SERIALIZED` 丢包 |
| 有界缓冲 | 客户端出站 256 条、集群事件队列 4096 条，超限断开慢消费者，防止内存膨胀 |
| 背压触发重同步 | 集群事件队列溢出时断开对等连接，重连后全量同步恢复一致性 |
| 会话版本 | 会话状态版本以时间种子初始化，节点重启后新版本仍高于历史版本 |
| 保留消息版本 | 本地版本时钟会采纳远端高版本，降低节点时钟偏差导致的覆盖丢失 |
| 删除墓碑 | 保留消息与会话删除保留 7 天，避免离线节点回归复活旧数据 |
| 锁顺序 | `registrationLock → routingIndexLock → sessionLock → sendLock`，无反向获取路径 |
| 优雅停机 | Spring Boot graceful shutdown，等待连接与出站队列收敛 |
| 健康检查 | 容器健康检查探测 MQTT 端口，避免仅检测 JVM 存活 |

---

## 测试与验证

当前测试覆盖协议流程、会话管理、路由索引、集群握手、集群认证、多数派、保留消息版本、并发队列等：

```bash
mvn test
```

最近一次结果：**59 个测试全部通过**。

主要集成测试：

| 测试类 | 覆盖内容 |
|--------|----------|
| `MqttProtocolIntegrationTest` | 连接、订阅、发布、QoS、保留消息、遗嘱等协议流程 |
| `MqttClusterIntegrationTest` | 两节点握手、状态视图、跨节点发布与会话迁移 |
| `MqttClusterAuthIntegrationTest` | 正确凭据握手成功、错误凭据持续拒绝 |
| `MqttSessionExpiryIntegrationTest` | MQTT 5 会话过期与清理 |
| `MqttAclIntegrationTest` | ACL 发布/订阅权限 |
| `ConsoleApiIntegrationTest` | 控制台 REST API |
| `MqttSessionRegistryRoutingTest` | 本地路由索引、远端订阅定向转发、统计缓存 |
| `MqttRetainedStoreTest` | 保留消息版本合并、墓碑与版本时钟 |
| `MqttClusterQuorumTest` | 多数派、发布缓冲、认证启动校验 |

---

## 项目结构

```text
src/main/java/com/wcdk/mqtt
├── cluster/                 集群管理、对等连接、事件编解码、风险评分
├── config/                  Spring 配置、属性绑定、安全配置
├── controller/              管理 REST API 与 SSE
├── core/                    会话、注册表、路由、ACL、保留消息、消息日志
├── protocol/                MQTT 接入、连接状态机、会话扫描
├── service/                 发布、投递、概览服务
├── web/                     控制台事件总线
└── MqttBrokerApplication.java

src/main/resources
├── application.yml          默认配置
└── static/                  网页控制台（HTML/CSS/JS）

src/test/java/com/wcdk/mqtt   单元测试与集成测试
```

---

## 生产部署建议

1. **修改默认凭据**：控制台 `admin/admin` 与集群默认凭据必须替换。
2. **开启集群认证**：保持 `cluster.require-auth=true`；仅在集群端口已由网络策略完全隔离时才考虑关闭。
3. **启用集群 TLS**：跨主机部署建议配置证书与互信 CA，开启双向认证。
4. **同步时钟**：所有集群节点启用 NTP，版本号与心跳判定都依赖时间。
5. **显式固定成员数**：三节点集群设置 `expected-nodes: 3`，避免动态发现导致多数派误判。
6. **按业务收紧负载上限**：默认单条消息 64KB，如业务需要大消息再单独放宽。
7. **合理设置墓碑窗口**：`tombstone-ttl-hours` 必须大于节点最长允许离线时间。
8. **监控关键指标**：`/api/cluster` 的 `pendingPublishes`、`droppedPublishes`、`hasQuorum`，以及 `/api/cluster/risk` 的风险分。
9. **容量规划**：大连接场景请先按 `CAPACITY-ASSESSMENT-90K.md` 做分阶段压测，再调整 JVM 与操作系统参数。

---

## 已知限制

- **状态保存在内存**：会话、订阅、保留消息与离线队列在进程重启后丢失；集群只能从仍在线节点恢复快照，全集群重启会丢失全部状态。
- **集群发布为尽力补发**：断线期间的消息缓冲不是端到端确认，节点永久下线或缓冲溢出时仍可能丢失。
- **TLS 默认关闭**：集群通道与 MQTT 接入端口默认明文，生产需自行启用集群 TLS 或通过网络层保护。
- **MQTT 接入端口未内置 TLS/WSS**：如需加密设备连接，请在反向代理或网关层终止 TLS。
- **版本号依赖时钟**：节点必须启用 NTP；时钟偏差过大可能影响新旧数据裁决。
- **大规模压测待补**：9 万连接级别的容量结论需在独立压测环境验证。

---

## 相关文档

- [Docker 部署指南](docker.md)
- [并发安全评估与修复记录](CONCURRENCY-ASSESSMENT.md)
- [9 万连接容量评估](CAPACITY-ASSESSMENT-90K.md)

---

## License

请参考仓库中的 [LICENSE](LICENSE)。
