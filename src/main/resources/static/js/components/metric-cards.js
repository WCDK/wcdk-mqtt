Vue.component('metric-cards', {
    props: {
        overview: {
            type: Object,
            required: true
        }
    },
    template: `
        <section class="metric-grid">
            <article class="metric-card">
                <div class="metric-label">活动连接</div>
                <div class="metric-value">{{ overview.activeSessions }}</div>
                <div class="metric-foot">总会话 {{ overview.totalSessions }}</div>
            </article>
            <article class="metric-card">
                <div class="metric-label">持久会话</div>
                <div class="metric-value">{{ overview.persistentSessions }}</div>
                <div class="metric-foot">cleanSession=false</div>
            </article>
            <article class="metric-card">
                <div class="metric-label">客户端订阅</div>
                <div class="metric-value">{{ overview.totalClientSubscriptions }}</div>
                <div class="metric-foot">已注册主题过滤器</div>
            </article>
            <article class="metric-card">
                <div class="metric-label">待 ACK 消息</div>
                <div class="metric-value">{{ overview.pendingAckMessages }}</div>
                <div class="metric-foot">QoS1/QoS2 出站确认</div>
            </article>
            <article class="metric-card">
                <div class="metric-label">离线待补发</div>
                <div class="metric-value">{{ overview.queuedMessages }}</div>
                <div class="metric-foot">持久会话离线队列</div>
            </article>
            <article class="metric-card">
                <div class="metric-label">队列消息</div>
                <div class="metric-value">{{ overview.queueMessages }}</div>
                <div class="metric-foot">容量 {{ overview.queueCapacity }}</div>
            </article>
        </section>
    `
});
