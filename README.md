# wcdk-mqtt - MQTT Broker 集群风险评估

## 集群脑裂风险评估

本项目现已集成 **集群脑裂风险评估** 功能，用于监控 MQTT 集群中的网络分区（脑裂）风险。

### 核心功能

- **实时风险评分**：基于集群状态计算 0-100 的风险分数
- **分层风险等级**：
  - `SAFE` (0-19) - 正常运行
  - `LOW` (20-39) - 连接率轻度下降
  - `MEDIUM` (40-59) - 部分节点响应延迟
  - `HIGH` (60-79) - 高风险，心跳超时节点
  - `EXTREME` (80-100) - 极危，检测到网络分区
- **自动缓解建议**：每个风险等级提供具体的恢复操作建议

### REST API 端点

```
GET  /api/cluster/risk          - 获取完整风险评估结果
GET  /api/cluster/risk/refresh  - 刷新风险评估
GET  /api/cluster/risk/nodes    - 获取节点详细状态
GET  /api/cluster/risk/peers    - 获取对等连接详情
GET  /api/cluster/risk/config   - 获取集群配置信息
```

### 风险指标分析

- **连接率**：已连接对等节点 vs 配置对等节点数量
- **心跳超时**：基于心跳间隔和节点超时阈值检测不响应节点
- **单点风险**：当活跃节点数少于配置节点数时的风险评估
- **网络分区检测**：识别配置的对等节点是否实际在线

### 实现文件

- `src/main/java/com/wcdk/mqtt/cluster/ClusterSplitRiskAssessment.java` - 核心评估类
- `src/main/java/com/wcdk/mqtt/controller/ClusterSplitRiskController.java` - REST API 控制器

### 使用示例

```bash
# 获取风险评估
curl http://localhost:38082/api/cluster/risk

# 示例响应结构
{
  "success": true,
  "data": {
    "riskScore": 65,
    "riskLevel": "HIGH",
    "riskFactors": [
      "部分对等节点心跳超时，网络可能存在抖动",
      "风险进入中高水平，建议监控网络状况"
    ],
    "mitigationAdvice": "高风险预警：\n1. 尽快排查导致心跳超时的网络原因\n2. 重启或重新连接超时的节点\n3. 在节点恢复前，谨慎进行新的持久化写入操作"
  },
  "timestamp": "2026-11-13T...",
  "nodeDetails": { ... },
  "peerDetails": { ... }
}
```

### 构建状态

- ✅ Maven 编译通过
- ✅ 现有测试通过
- ✅ 无依赖冲突 (使用现有项目结构)