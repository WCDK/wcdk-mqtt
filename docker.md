# Docker 部署指南

`wcdk-mqtt` 是基于 Spring Boot + Reactor Netty 的响应式 MQTT Broker，所有状态（会话、保留消息、离线队列）保存在内存中，重启后重置，集群节点之间自动同步。

默认端口：

| 端口  | 用途                                       |
| ----- | ------------------------------------------ |
| 38082 | HTTP 管理接口与网页控制台                  |
| 1883  | MQTT 接入（MQTT 3.1 / 3.1.1 / 5.0）        |
| 28883 | 集群对等通道                                |

## 一、构建镜像

```bash
docker build -t wcdk1024/wcdk-mqtt:1.0.0 .
```

## 二、单节点部署

仓库自带 `.env`（`COMPOSE_PROFILES=single`），因此直接执行下面的命令即可启动单节点：

```bash
docker compose up -d
```

等价于显式指定 profile：

```bash
docker compose --profile single up -d
```

启动后访问：

- 网页控制台：`http://localhost:38082/`（默认账号 `admin/admin`）
- MQTT 接入：`localhost:1883`

查看日志与停止：

```bash
docker compose logs -f mqtt-broker
docker compose down
```

## 三、多节点集群部署

集群为**对等模型**，所有节点地位相同，不存在 MASTER/WORKER 之分：

- 每个节点都在自己的 `bind-port`（默认 28883）上监听；
- 每个节点按 `peers` 列表主动拨号其他节点，同时接受别人的拨号；
- 重复连接由两侧按「节点 ID 字典序较小的一方保留自己拨出的连接」自动消解；
- 会话、保留消息、消息发布在节点间实时同步。

启动内置的三节点集群：

```bash
docker compose --profile cluster up -d
```

三个节点映射到宿主机的端口：

| 节点     | 控制台 | MQTT  | 集群  |
| -------- | ------ | ----- | ----- |
| node-1   | 38082  | 1883  | 28883 |
| node-2   | 38083  | 1884  | 28884 |
| node-3   | 38084  | 1885  | 28885 |

验证集群连通：打开任一节点的控制台 → 「集群」页签，应看到另外两个节点状态为「在线」。

```bash
docker compose --profile cluster ps
docker compose --profile cluster logs -f mqtt-node1
```

## 四、集群关键配置

多节点部署必须正确配置以下三项（compose 已给出示例）：

| 环境变量                                  | 说明                                                     |
| ----------------------------------------- | -------------------------------------------------------- |
| `WCDK_MQTT_CLUSTER_ENABLED`               | 是否启用集群，单实例设为 `false`                         |
| `WCDK_MQTT_CLUSTER_NODE_ID`               | 节点唯一 ID，同一集群内不能重复                          |
| `WCDK_MQTT_CLUSTER_ADVERTISE_HOST`        | 本节点对外公布的地址，其他节点据此回连；容器网络填服务名 |
| `WCDK_MQTT_CLUSTER_PEERS_0`、`_1`、`_2`…  | 对等地址列表，格式 `host:port`，下标从 0 开始            |
| `WCDK_MQTT_CLUSTER_BIND_PORT`             | 集群监听端口，默认 28883                                 |
| `WCDK_MQTT_CLUSTER_HEARTBEAT_INTERVAL_MILLIS` | 心跳间隔，默认 5000ms                                |
| `WCDK_MQTT_CLUSTER_NODE_TIMEOUT_MILLIS`   | 节点判定超时断开的毫秒数，默认 15000ms                   |

注意事项：

1. `advertise-host` 不能填 `0.0.0.0`，否则对等节点无法回连；跨宿主机部署时填宿主机 IP。
2. 节点间时钟不必严格同步，但偏差过大会影响 KeepAlive 判定，建议启用 NTP。
3. 集群通道当前为明文 TCP，跨不可信网络部署时请置于内网或叠加网络层加密。

## 五、自定义对等拓扑

在宿主机上编写自己的 compose 文件，只依赖镜像即可：

```yaml
services:
  mqtt-a:
    image: wcdk1024/wcdk-mqtt:1.0.0
    ports:
      - "38082:38082"
      - "1883:1883"
      - "28883:28883"
    environment:
      WCDK_MQTT_CLUSTER_ENABLED: "true"
      WCDK_MQTT_CLUSTER_NODE_ID: node-a
      WCDK_MQTT_CLUSTER_ADVERTISE_HOST: 192.168.1.10
      WCDK_MQTT_CLUSTER_PEERS_0: 192.168.1.11:28883

  mqtt-b:
    image: wcdk1024/wcdk-mqtt:1.0.0
    ports:
      - "38083:38082"
      - "1884:1883"
      - "28884:28883"
    environment:
      WCDK_MQTT_CLUSTER_ENABLED: "true"
      WCDK_MQTT_CLUSTER_NODE_ID: node-b
      WCDK_MQTT_CLUSTER_BIND_PORT: 28883
      WCDK_MQTT_CLUSTER_ADVERTISE_HOST: 192.168.1.11
      WCDK_MQTT_CLUSTER_PEERS_0: 192.168.1.10:28883
```

> `bind-port` 是容器内监听端口，`ports` 映射决定宿主机端口；当两个节点跑在同一宿主机时，容器内端口保持一致、宿主机端口错开即可，`peers` 填的是**容器内端口**（同网络下）或宿主机映射端口（跨主机），以实际可达为准。

## 六、安全与持久化说明

- 控制台账号：`WCDK_MQTT_SECURITY_CONSOLE_USERNAME` / `WCDK_MQTT_SECURITY_CONSOLE_PASSWORD`，默认 `admin/admin`，生产环境务必修改。
- MQTT 认证：`WCDK_MQTT_SECURITY_USERNAME` / `WCDK_MQTT_SECURITY_PASSWORD`，并可用 `WCDK_MQTT_SECURITY_ANONYMOUS=false` 禁止匿名接入。
- 访问控制：通过 `wcdk.mqtt.acl.*` 配置主题级读写规则。
- **纯内存存储**：重启后会话、保留消息、离线队列全部重置，无数据卷需要挂载；如需保留状态请在应用层做快照。
