Vue.component('messages-panel', {
    props: {
        messages: {
            type: Array,
            required: true
        },
        loading: {
            type: Object,
            required: true
        },
        filters: {
            type: Object,
            required: true
        },
        pageNo: {
            type: Number,
            required: true
        },
        pageSize: {
            type: Number,
            required: true
        },
        total: {
            type: Number,
            required: true
        }
    },
    data: function () {
        return {
            selectedMessages: []
        };
    },
    methods: {
        formatTime: function (value) {
            if (!value) {
                return '-';
            }
            return new Date(value).toLocaleString('zh-CN', { hour12: false });
        },
        handleSelectionChange: function (selection) {
            this.selectedMessages = selection || [];
        },
        handleDeleteAction: function () {
            if (this.selectedMessages.length > 0) {
                this.$emit('delete-selected', this.selectedMessages.slice());
                return;
            }
            this.$emit('delete-by-filter');
        },
        handlePageChange: function (pageNo) {
            this.$emit('page-change', pageNo);
        },
        handlePageSizeChange: function (pageSize) {
            this.$emit('page-size-change', pageSize);
        }
    },
    template: `
        <div class="panel">
            <div class="panel-header">
                <h2 class="panel-title">消息列表</h2>
                <div class="toolbar">
                    <el-input
                            v-model.trim="filters.topicFilter"
                            size="small"
                            clearable
                            placeholder="按主题过滤器查询"
                            style="width: 220px;">
                    </el-input>
                    <el-select v-model="filters.qos" size="small" clearable placeholder="QoS" style="width: 90px;">
                        <el-option label="0" :value="0"></el-option>
                        <el-option label="1" :value="1"></el-option>
                        <el-option label="2" :value="2"></el-option>
                    </el-select>
                    <el-date-picker
                            v-model="filters.timeRange"
                            size="small"
                            type="datetimerange"
                            format="yyyy-MM-dd HH:mm:ss"
                            range-separator="至"
                            start-placeholder="发送开始时间"
                            end-placeholder="发送结束时间"
                            value-format="yyyy-MM-dd HH:mm:ss"
                            style="width: 360px;">
                    </el-date-picker>
                    <el-select v-model="filters.sortBy" size="small" style="width: 120px;">
                        <el-option label="发送时间" value="receivedAt"></el-option>
                        <el-option label="主题" value="topic"></el-option>
                        <el-option label="QoS" value="qos"></el-option>
                    </el-select>
                    <el-select v-model="filters.sortDirection" size="small" style="width: 100px;">
                        <el-option label="降序" value="desc"></el-option>
                        <el-option label="升序" value="asc"></el-option>
                    </el-select>
                    <el-button size="small" icon="el-icon-search" @click="$emit('search')">查询</el-button>
                    <el-button
                            size="small"
                            type="danger"
                            icon="el-icon-delete"
                            :loading="loading.deleteMessages"
                            @click="handleDeleteAction">
                        {{ selectedMessages.length ? '删除选中内容' : '按条件清除' }}
                    </el-button>
                    <el-button size="small" icon="el-icon-delete-solid"  type="danger" :loading="loading.deleteAllMessages" @click="$emit('delete-all')">清除所有</el-button>
                </div>
            </div>
            <div class="panel-body">
                <el-table
                        :data="messages"
                        size="small"
                        border
                        stripe
                        v-loading="loading.messages"
                        @selection-change="handleSelectionChange">
                    <el-table-column type="selection" width="48"></el-table-column>
                    <el-table-column label="发送时间" width="176">
                        <template slot-scope="scope">
                            {{ formatTime(scope.row.receivedAt) }}
                        </template>
                    </el-table-column>
                    <el-table-column label="主题" min-width="180">
                        <template slot-scope="scope">
                            <span class="mono">{{ scope.row.topic }}</span>
                        </template>
                    </el-table-column>
                    <el-table-column label="QoS" width="72" prop="qos"></el-table-column>
                    <el-table-column label="保留" width="72">
                        <template slot-scope="scope">
                            {{ scope.row.retained ? '是' : '否' }}
                        </template>
                    </el-table-column>
                    <el-table-column label="匹配订阅" min-width="180">
                        <template slot-scope="scope">
                            <div v-if="scope.row.matchedSubscriptions.length" class="subscription-tags">
                                <el-tag
                                        v-for="item in scope.row.matchedSubscriptions"
                                        :key="scope.row.receivedAt + '-' + item"
                                        size="mini"
                                        effect="plain">
                                    {{ item }}
                                </el-tag>
                            </div>
                            <span v-else class="empty-note">无</span>
                        </template>
                    </el-table-column>
                    <el-table-column label="消息内容" min-width="260">
                        <template slot-scope="scope">
                            <div class="message-payload">{{ scope.row.payload }}</div>
                        </template>
                    </el-table-column>
                </el-table>
                <div class="table-pagination">
                    <el-pagination
                            :key="pageNo + '-' + pageSize + '-' + total"
                            background
                            layout="total, sizes, prev, pager, next"
                            :current-page="pageNo"
                            :page-size="pageSize"
                            :page-sizes="[10, 20, 50, 100]"
                            :total="total"
                            @current-change="handlePageChange"
                            @size-change="handlePageSizeChange">
                    </el-pagination>
                </div>
            </div>
        </div>
    `
});
