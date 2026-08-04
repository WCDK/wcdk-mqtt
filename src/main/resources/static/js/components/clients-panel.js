Vue.component('clients-panel', {
    props: {
        clients: {
            type: Array,
            required: true
        },
        loading: {
            type: Boolean,
            required: true
        },
        clientFilters: {
            type: Object,
            required: true
        }
    },
    template: `
        <div class="panel">
            <div class="panel-header">
                <h2 class="panel-title">客户端会话</h2>
                <div class="toolbar">
                    <el-input
                            v-model.trim="clientFilters.keyword"
                            size="small"
                            clearable
                            placeholder="筛选客户端 ID 或主题"
                            style="width: 240px;">
                    </el-input>
                </div>
            </div>
            <div class="panel-body">
                <el-table :data="clients" size="small" border stripe v-loading="loading">
                    <el-table-column label="客户端" min-width="220">
                        <template slot-scope="scope">
                            <div class="mono">{{ scope.row.clientId }}</div>
                            <div v-if="scope.row.sessionUrl" class="empty-note mono">{{ scope.row.sessionUrl }}</div>
                        </template>
                    </el-table-column>
                    <el-table-column label="分配节点" min-width="170">
                        <template slot-scope="scope">
                            <span class="mono">{{ scope.row.nodeId || '-' }}</span>
                        </template>
                    </el-table-column>
                    <el-table-column label="状态" width="112">
                        <template slot-scope="scope">
                            <el-tag size="small" :type="scope.row.active ? 'success' : 'info'">
                                {{ scope.row.active ? '在线' : '离线' }}
                            </el-tag>
                        </template>
                    </el-table-column>
                    <el-table-column label="会话" width="110">
                        <template slot-scope="scope">
                            {{ scope.row.cleanSession ? '清理会话' : '持久会话' }}
                        </template>
                    </el-table-column>
                    <el-table-column label="待 ACK" width="88" prop="pendingAckMessages"></el-table-column>
                    <el-table-column label="待补发" width="88" prop="queuedMessages"></el-table-column>
                    <el-table-column label="订阅" min-width="280">
                        <template slot-scope="scope">
                            <div v-if="scope.row.subscriptions.length" class="subscription-tags">
                                <el-tag
                                        v-for="item in scope.row.subscriptions"
                                        :key="scope.row.clientId + '-' + item.topicFilter"
                                        size="mini"
                                        effect="plain">
                                    {{ item.topicFilter }} / QoS {{ item.qos }}
                                </el-tag>
                            </div>
                            <span v-else class="empty-note">无订阅</span>
                        </template>
                    </el-table-column>
                </el-table>
            </div>
        </div>
    `
});
