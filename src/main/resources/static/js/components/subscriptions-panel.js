Vue.component('subscriptions-panel', {
    props: {
        subscriptions: {
            type: Array,
            required: true
        },
        loading: {
            type: Boolean,
            required: true
        },
        subscriptionInputState: {
            type: Object,
            required: true
        }
    },
    template: `
        <div class="panel">
            <div class="panel-header">
                <h2 class="panel-title">订阅</h2>
                <el-tag size="small">{{ subscriptions.length }} 项</el-tag>
            </div>
            <div class="panel-body">
                <div class="toolbar" style="margin-bottom: 14px;">
                    <el-input
                            v-model.trim="subscriptionInputState.value"
                            size="small"
                            placeholder="例如 wcdk/iot/#"
                            style="min-width: 220px; flex: 1 1 220px;"
                            @keyup.enter.native="$emit('submit')">
                    </el-input>
                    <el-button type="primary" size="small" icon="el-icon-plus" :loading="loading" @click="$emit('submit')">
                        添加
                    </el-button>
                </div>
                <div v-if="subscriptions.length" class="subscription-tags">
                    <el-tag
                            v-for="item in subscriptions"
                            :key="item"
                            closable
                            @close="$emit('remove', item)">
                        {{ item }}
                    </el-tag>
                </div>
                <div v-else class="empty-note">还没有订阅过滤器。</div>
            </div>
        </div>
    `
});
