Vue.component('cluster-panel', {
    props: {
        clusterNodes: {
            type: Array,
            default: function () {
                return [];
            }
        },
        clusterConfig: {
            type: Object,
            default: function () {
                return {};
            }
        },
        clusterStats: {
            type: Object,
            default: function () {
                return {};
            }
        },
        loading: {
            type: Boolean,
            default: false
        }
    },
    template: `
        <section class="panel">
            <div class="panel-header">
                <h3 class="panel-title">集群管理</h3>
                <el-tag :type="clusterConfig.enabled ? 'success' : 'info'" size="small">
                    {{ clusterConfig.enabled ? '集群已启用' : '集群未启用' }}
                </el-tag>
            </div>
            <div class="panel-body">
                <el-tabs v-model="activeTab" type="border-card">
                    <el-tab-pane label="节点状态" name="nodes">
                        <el-table :data="clusterNodes" v-loading="loading" stripe>
                            <el-table-column prop="nodeId" label="节点 ID" min-width="200">
                                <template slot-scope="{ row }">
                                    <span class="mono">{{ row.nodeId }}</span>
                                    <el-tag v-if="row.role === 'MASTER'" type="warning" size="mini" style="margin-left: 8px;">主节点</el-tag>
                                    <el-tag v-if="row.isCurrentNode" type="success" size="mini" style="margin-left: 8px;">当前节点</el-tag>
                                </template>
                            </el-table-column>
                            <el-table-column prop="role" label="角色" width="100">
                                <template slot-scope="{ row }">
                                    <el-tag :type="row.role === 'MASTER' ? 'warning' : 'info'" size="small">
                                        {{ row.role === 'MASTER' ? '主节点' : '子节点' }}
                                    </el-tag>
                                </template>
                            </el-table-column>
                            <el-table-column prop="host" label="主机" width="150">
                                <template slot-scope="{ row }">
                                    <span class="mono">{{ row.host }}</span>
                                </template>
                            </el-table-column>
                            <el-table-column prop="port" label="端口" width="100" />
                            <el-table-column label="MQTT 地址" min-width="180">
                                <template slot-scope="{ row }">
                                    <span class="mono">{{ row.mqttHost || row.host }}:{{ row.mqttPort || '-' }}</span>
                                </template>
                            </el-table-column>
                            <el-table-column prop="sessionCount" label="会话数" width="100" />
                            <el-table-column prop="active" label="状态" width="100">
                                <template slot-scope="{ row }">
                                    <el-tag :type="row.active ? 'success' : 'danger'" size="small">
                                        {{ row.active ? '在线' : '离线' }}
                                    </el-tag>
                                </template>
                            </el-table-column>
                            <el-table-column prop="updatedAt" label="最后心跳" width="180">
                                <template slot-scope="{ row }">
                                    {{ formatTime(row.updatedAt) }}
                                </template>
                            </el-table-column>
                        </el-table>
                    </el-tab-pane>
                    
                    <el-tab-pane label="集群配置" name="config">
                        <el-descriptions :column="2" border size="small">
                            <el-descriptions-item label="集群启用">
                                <el-tag :type="clusterConfig.enabled ? 'success' : 'info'" size="small">
                                    {{ clusterConfig.enabled ? '是' : '否' }}
                                </el-tag>
                            </el-descriptions-item>
                            <el-descriptions-item label="节点角色">
                                <el-tag :type="clusterConfig.role === 'MASTER' ? 'warning' : 'info'" size="small">
                                    {{ clusterConfig.role === 'MASTER' ? '主节点' : '子节点' }}
                                </el-tag>
                            </el-descriptions-item>
                            <el-descriptions-item label="节点 ID">
                                <span class="mono">{{ clusterConfig.nodeId }}</span>
                            </el-descriptions-item>
                            <el-descriptions-item label="通道类型">
                                {{ clusterConfig.channel }}
                            </el-descriptions-item>
                            <el-descriptions-item label="监听地址">
                                <span class="mono">{{ clusterConfig.bindHost }}:{{ clusterConfig.bindPort }}</span>
                            </el-descriptions-item>
                            <el-descriptions-item v-if="clusterConfig.role === 'SLAVE' && clusterConfig.master" label="主节点地址">
                                <span class="mono">{{ clusterConfig.master.host }}:{{ clusterConfig.master.port }}</span>
                            </el-descriptions-item>
                            <el-descriptions-item v-if="clusterConfig.peers && clusterConfig.peers.length > 0" label="对等节点" :span="2">
                                <div>
                                    <el-tag v-for="peer in clusterConfig.peers" :key="peer" size="small" style="margin-right: 8px;">
                                        {{ peer }}
                                    </el-tag>
                                </div>
                            </el-descriptions-item>
                            <el-descriptions-item label="全局会话">
                                <el-tag :type="clusterConfig.globalSession ? 'success' : 'info'" size="small">
                                    {{ clusterConfig.globalSession ? '启用' : '禁用' }}
                                </el-tag>
                            </el-descriptions-item>
                            <el-descriptions-item label="分布式 Retained">
                                <el-tag :type="clusterConfig.distributedRetained ? 'success' : 'info'" size="small">
                                    {{ clusterConfig.distributedRetained ? '启用' : '禁用' }}
                                </el-tag>
                            </el-descriptions-item>
                            <el-descriptions-item label="离线队列">
                                <el-tag :type="clusterConfig.offlineQueue ? 'success' : 'info'" size="small">
                                    {{ clusterConfig.offlineQueue ? '启用' : '禁用' }}
                                </el-tag>
                            </el-descriptions-item>
                            <el-descriptions-item label="最大离线消息数">
                                {{ clusterConfig.maxOfflineMessages }}
                            </el-descriptions-item>
                            <el-descriptions-item label="心跳间隔">
                                {{ clusterConfig.heartbeatIntervalMillis }} 毫秒
                            </el-descriptions-item>
                            <el-descriptions-item label="节点超时">
                                {{ clusterConfig.nodeTimeoutMillis }} 毫秒
                            </el-descriptions-item>
                            <el-descriptions-item label="投递 ACK 超时">
                                {{ clusterConfig.deliveryAckTimeoutMillis }} 毫秒
                            </el-descriptions-item>
                        </el-descriptions>
                    </el-tab-pane>
                    
                    <el-tab-pane label="统计信息" name="stats">
                        <el-descriptions :column="2" border size="small">
                            <el-descriptions-item label="集群状态">
                                <el-tag :type="clusterStats.clusterEnabled ? 'success' : 'info'" size="small">
                                    {{ clusterStats.clusterEnabled ? '已启用' : '未启用' }}
                                </el-tag>
                            </el-descriptions-item>
                            <el-descriptions-item label="当前节点">
                                <span class="mono">{{ clusterStats.currentNodeId || '-' }}</span>
                            </el-descriptions-item>
                            <el-descriptions-item label="节点数量">
                                {{ clusterStats.activeNodes }} / {{ clusterStats.totalNodes }}
                                <span class="empty-note">在线 / 总计</span>
                            </el-descriptions-item>
                            <el-descriptions-item label="总会话数">
                                {{ clusterStats.totalSessions }}
                            </el-descriptions-item>
                            <el-descriptions-item label="活动会话">
                                {{ clusterStats.activeSessions }}
                            </el-descriptions-item>
                            <el-descriptions-item label="客户端归属">
                                {{ clusterStats.totalClientOwners }}
                            </el-descriptions-item>
                            <el-descriptions-item label="会话快照">
                                {{ clusterStats.totalSessionSnapshots }}
                            </el-descriptions-item>
                            <el-descriptions-item label="分布式 Retained">
                                {{ clusterStats.totalRetainedMessages }}
                                <span class="empty-note">跨节点保留消息</span>
                            </el-descriptions-item>
                            <el-descriptions-item label="离线消息队列">
                                {{ clusterStats.totalOfflineMessages }}
                                <span class="empty-note">跨节点离线消息</span>
                            </el-descriptions-item>
                        </el-descriptions>
                    </el-tab-pane>
                </el-tabs>
            </div>
        </section>
    `,
    data: function () {
        return {
            activeTab: 'nodes'
        };
    },
    methods: {
        formatTime: function (timestamp) {
            if (!timestamp) {
                return '-';
            }
            var date = new Date(timestamp);
            var year = date.getFullYear();
            var month = String(date.getMonth() + 1).padStart(2, '0');
            var day = String(date.getDate()).padStart(2, '0');
            var hours = String(date.getHours()).padStart(2, '0');
            var minutes = String(date.getMinutes()).padStart(2, '0');
            var seconds = String(date.getSeconds()).padStart(2, '0');
            return year + '-' + month + '-' + day + ' ' + hours + ':' + minutes + ':' + seconds;
        }
    }
});