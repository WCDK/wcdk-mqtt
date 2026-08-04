Vue.component('app-header', {
    props: {
        overview: {
            type: Object,
            required: true
        },
        brokerAddress: {
            type: String,
            required: true
        },
        settings: {
            type: Object,
            required: true
        },
        loading: {
            type: Object,
            required: true
        },
        activePanel: {
            type: String,
            default: 'main'
        }
    },
    template: `
        <header class="topbar">
            <div class="topbar-title">
                <h1>IOT-MQTT 控制台</h1>
                <el-tag size="small" :type="overview.brokerRunning ? 'success' : 'danger'">
                    {{ overview.brokerRunning ? 'Broker 运行中' : 'Broker 未运行' }}
                </el-tag>
                <span class="topbar-meta mono">{{ brokerAddress }}</span>
            </div>
            <div class="topbar-actions">
                <el-button size="small" :type="activePanel === 'cluster' ? 'primary' : 'default'" icon="el-icon-office-building" @click="$emit('toggle-panel', 'cluster')">
                    集群管理
                </el-button>
                <el-switch
                        v-model="settings.autoRefresh"
                        active-text="自动刷新"
                        inactive-text="手动刷新">
                </el-switch>
                <el-select v-model="settings.refreshInterval" size="small" style="width: 116px" :disabled="!settings.autoRefresh">
                    <el-option label="1 秒" :value="1000"></el-option>
                    <el-option label="3 秒" :value="3000"></el-option>
                    <el-option label="5 秒" :value="5000"></el-option>
                    <el-option label="10 秒" :value="10000"></el-option>
                </el-select>
                <el-button size="small" icon="el-icon-refresh" @click="$emit('refresh')" :loading="loading.overview || loading.clients || loading.messages">
                    刷新
                </el-button>
            </div>
        </header>
    `
});
