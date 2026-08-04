Vue.component('publish-panel', {
    props: {
        form: {
            type: Object,
            required: true
        },
        loading: {
            type: Boolean,
            required: true
        }
    },
    template: `
        <div class="panel">
            <div class="panel-header">
                <h2 class="panel-title">消息发布</h2>
            </div>
            <div class="panel-body">
                <el-form :model="form" label-position="top" size="small" class="compact-form">
                    <el-form-item label="发布方式">
                        <el-radio-group v-model="form.mode">
                            <el-radio-button label="broker">当前 Broker 广播</el-radio-button>
                            <el-radio-button label="client">按客户端推送</el-radio-button>
                            <el-radio-button label="url">按 MQTT URL 推送</el-radio-button>
                        </el-radio-group>
                    </el-form-item>
                    <el-form-item v-if="form.mode === 'client'" label="客户端 ID">
                        <el-input v-model.trim="form.clientId" placeholder="优先按 clientId 定向推送"></el-input>
                    </el-form-item>
                    <el-form-item v-if="form.mode === 'client'" label="会话 URL">
                        <el-input v-model.trim="form.sessionUrl" placeholder="clientId 为空时可按 /127.0.0.1:59012 定向推送"></el-input>
                    </el-form-item>
                    <el-form-item v-if="form.mode === 'url'" label="MQTT URL">
                        <el-input v-model.trim="form.brokerUrl" placeholder="例如 tcp://127.0.0.1:1883"></el-input>
                    </el-form-item>
                    <el-row v-if="form.mode === 'url'" :gutter="12">
                        <el-col :xs="24" :sm="12">
                            <el-form-item label="发布客户端 ID">
                                <el-input v-model.trim="form.remoteClientId" placeholder="为空则自动生成"></el-input>
                            </el-form-item>
                        </el-col>
                        <el-col :xs="24" :sm="12">
                            <el-form-item label="用户名">
                                <el-input v-model.trim="form.username" placeholder="可选"></el-input>
                            </el-form-item>
                        </el-col>
                    </el-row>
                    <el-form-item v-if="form.mode === 'url'" label="密码">
                        <el-input v-model="form.password" show-password placeholder="可选"></el-input>
                    </el-form-item>
                    <el-form-item label="主题">
                        <el-input v-model.trim="form.topic" placeholder="例如 wcdk/iot/demo"></el-input>
                    </el-form-item>
                    <el-form-item label="消息内容">
                        <el-input v-model="form.payload" type="textarea" :rows="4" placeholder="输入要发送的消息"></el-input>
                    </el-form-item>
                    <el-row :gutter="12">
                        <el-col :xs="24" :sm="12">
                            <el-form-item label="QoS">
                                <el-select v-model="form.qos" style="width: 100%">
                                    <el-option label="0" :value="0"></el-option>
                                    <el-option label="1" :value="1"></el-option>
                                    <el-option label="2" :value="2"></el-option>
                                </el-select>
                            </el-form-item>
                        </el-col>
                        <el-col :xs="24" :sm="12">
                            <el-form-item label="保留消息">
                                <el-switch v-model="form.retained"></el-switch>
                            </el-form-item>
                        </el-col>
                    </el-row>
                    <el-button type="primary" icon="el-icon-position" :loading="loading" @click="$emit('submit')">
                        发布消息
                    </el-button>
                </el-form>
            </div>
        </div>
    `
});
