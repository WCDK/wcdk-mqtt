Vue.component('broker-config-panel', {
    props: {
        overview: {
            type: Object,
            required: true
        }
    },
    template: `
        <div class="panel">
            <div class="panel-header">
                <h2 class="panel-title">Broker 配置</h2>
            </div>
            <div class="panel-body">
                <div class="status-stack">
                    <el-tag size="small" :type="overview.anonymous ? 'warning' : 'info'">
                        {{ overview.anonymous ? '匿名连接已开启' : '匿名连接已关闭' }}
                    </el-tag>
                    <el-tag size="small" :type="overview.retainedMessages ? 'success' : 'info'">
                        {{ overview.retainedMessages ? '保留消息已开启' : '保留消息已关闭' }}
                    </el-tag>
                    <el-tag size="small" type="info">入站 QoS2 暂存 {{ overview.inboundQos2Messages }}</el-tag>
                    <el-tag size="small" type="info">队列订阅 {{ overview.queueSubscriptions }}</el-tag>
                </div>
            </div>
        </div>
    `
});
