new Vue({
    el: '#app',
    data: function () {
        return {
            activePanel: 'main',
            overview: {
                brokerRunning: false,
                host: '0.0.0.0',
                port: 1883,
                anonymous: true,
                retainedMessages: true,
                totalSessions: 0,
                activeSessions: 0,
                persistentSessions: 0,
                totalClientSubscriptions: 0,
                pendingAckMessages: 0,
                queuedMessages: 0,
                inboundQos2Messages: 0,
                queueSubscriptions: 0,
                queueMessages: 0,
                queueCapacity: 500
            },
            clients: [],
            subscriptions: [],
            messages: [],
            publishForm: {
                mode: 'broker',
                clientId: '',
                sessionUrl: '',
                brokerUrl: '',
                remoteClientId: '',
                username: '',
                password: '',
                topic: 'wcdk/iot/demo',
                payload: 'hello mqtt',
                qos: 0,
                retained: false
            },
            subscriptionInputState: {
                value: 'wcdk/iot/#'
            },
            clientFilters: {
                keyword: ''
            },
            messageFilters: {
                topicFilter: '',
                qos: null,
                timeRange: null,
                sortBy: 'receivedAt',
                sortDirection: 'desc'
            },
            messagePagination: {
                total: 0,
                pageNo: 1,
                pageSize: 10
            },
            settings: {
                autoRefresh: true,
                refreshInterval: 3000
            },
            timerId: null,
            loading: {
                overview: false,
                clients: false,
                subscriptions: false,
                messages: false,
                publish: false,
                clearMessages: false,
                deleteMessages: false,
                deleteAllMessages: false,
                cluster: false
            },
            clusterNodes: [],
            clusterConfig: {
                enabled: false,
                nodeId: '',
                channel: 'TCP',
                bindHost: '0.0.0.0',
                bindPort: 28883,
                peers: [],
                globalSession: true,
                distributedRetained: true,
                offlineQueue: true,
                maxOfflineMessages: 1000,
                heartbeatIntervalMillis: 10000,
                nodeTimeoutMillis: 30000,
                deliveryAckTimeoutMillis: 3000
            },
            clusterStats: {
                clusterEnabled: false,
                currentNodeId: '',
                totalNodes: 0,
                activeNodes: 0,
                totalSessions: 0,
                activeSessions: 0,
                totalClientOwners: 0,
                totalSessionSnapshots: 0,
                totalRetainedMessages: 0,
                totalOfflineMessages: 0
            }
        };
    },
    computed: {
        brokerAddress: function () {
            return this.overview.host + ':' + this.overview.port;
        },
        filteredClients: function () {
            var keyword = this.clientFilters.keyword.trim().toLowerCase();
            if (!keyword) {
                return this.clients;
            }
            return this.clients.filter(function (client) {
                if (client.clientId.toLowerCase().indexOf(keyword) >= 0) {
                    return true;
                }
                return client.subscriptions.some(function (item) {
                    return item.topicFilter.toLowerCase().indexOf(keyword) >= 0;
                });
            });
        }
    },
    watch: {
        'settings.autoRefresh': function () {
            this.configureTimer();
        },
        'settings.refreshInterval': function () {
            this.configureTimer();
        }
    },
    created: function () {
        this.refreshAll();
    },
    mounted: function () {
        this.configureTimer();
    },
    beforeDestroy: function () {
        this.clearTimer();
    },
    methods: {
        request: async function (url, options) {
            var requestOptions = Object.assign({
                credentials: 'same-origin',
                headers: {
                    'Content-Type': 'application/json'
                }
            }, options || {});
            if (options && options.headers) {
                requestOptions.headers = Object.assign({
                    'Content-Type': 'application/json'
                }, options.headers);
            }
            var response = await fetch(url, requestOptions);
            var text = await response.text();
            var data = null;
            var contentType = response.headers.get('Content-Type') || '';
            if (response.status === 401 || this.isLoginPageResponse(response, text)) {
                window.location.href = '/login.html?expired=true';
                throw new Error('登录状态已失效，请重新登录');
            }
            if (text) {
                try {
                    data = JSON.parse(text);
                } catch (e) {
                    throw new Error('服务器返回了无效的数据格式');
                }
            }
            if (text && contentType && contentType.toLowerCase().indexOf('json') < 0) {
                throw new Error('服务器返回了无效的数据格式');
            }
            if (!response.ok) {
                throw new Error(data && (data.message || data.error) ? (data.message || data.error) : '请求失败');
            }
            return data;
        },
        isLoginPageResponse: function (response, text) {
            var responseUrl = response.url || '';
            if (response.redirected && responseUrl.indexOf('/login.html') >= 0) {
                return true;
            }
            return !!(text && text.indexOf('<form class="login-form"') >= 0 && text.indexOf('wcdk-MQTT') >= 0);
        },
        refreshAll: async function () {
            await Promise.all([
                this.loadOverview(),
                this.loadClients(),
                this.loadSubscriptions(),
                this.loadMessages()
            ]);
            if (this.activePanel === 'cluster') {
                await this.loadClusterData();
            }
        },
        togglePanel: function (panel) {
            if (this.activePanel === panel) {
                this.activePanel = 'main';
            } else {
                this.activePanel = panel;
                if (panel === 'cluster') {
                    this.loadClusterData();
                }
            }
        },
        loadClusterData: async function () {
            this.loading.cluster = true;
            try {
                var results = await Promise.all([
                    this.request('/wcdk/mqtt/cluster/nodes'),
                    this.request('/wcdk/mqtt/cluster/config'),
                    this.request('/wcdk/mqtt/cluster/stats')
                ]);
                this.clusterNodes = results[0] || [];
                this.clusterConfig = results[1] || this.clusterConfig;
                this.clusterStats = results[2] || this.clusterStats;
            } finally {
                this.loading.cluster = false;
            }
        },
        loadOverview: async function () {
            this.loading.overview = true;
            try {
                this.overview = await this.request('/wcdk/mqtt/overview');
            } finally {
                this.loading.overview = false;
            }
        },
        loadClients: async function () {
            this.loading.clients = true;
            try {
                this.clients = await this.request('/wcdk/mqtt/clients');
            } finally {
                this.loading.clients = false;
            }
        },
        loadSubscriptions: async function () {
            this.loading.subscriptions = true;
            try {
                this.subscriptions = await this.request('/wcdk/mqtt/subscriptions');
            } finally {
                this.loading.subscriptions = false;
            }
        },
        reloadMessages: function () {
            this.messagePagination.pageNo = 1;
            return this.loadMessages();
        },
        handleMessagePageChange: function (pageNo) {
            this.messagePagination.pageNo = pageNo;
            this.loadMessages();
        },
        handleMessagePageSizeChange: function (pageSize) {
            this.messagePagination.pageSize = pageSize;
            this.messagePagination.pageNo = 1;
            this.loadMessages();
        },
        loadMessages: async function () {
            this.loading.messages = true;
            try {
                var params = this.buildMessageQueryParams();
                params.set('pageNo', String(this.messagePagination.pageNo));
                params.set('pageSize', String(this.messagePagination.pageSize));
                params.set('sortBy', this.messageFilters.sortBy);
                params.set('sortDirection', this.messageFilters.sortDirection);
                var response = await this.request('/wcdk/mqtt/messages/list?' + params.toString());
                this.messages = response.records || [];
                this.messagePagination.total = response.total || 0;
                this.messagePagination.pageNo = response.pageNo || this.messagePagination.pageNo;
                this.messagePagination.pageSize = response.pageSize || this.messagePagination.pageSize;
            } finally {
                this.loading.messages = false;
            }
        },
        buildMessageQueryParams: function () {
            var params = new URLSearchParams();
            if (this.messageFilters.topicFilter.trim()) {
                params.set('topicFilter', this.messageFilters.topicFilter.trim());
            }
            if (this.messageFilters.qos !== null && this.messageFilters.qos !== undefined && this.messageFilters.qos !== '') {
                params.set('qos', String(this.messageFilters.qos));
            }
            if (this.messageFilters.timeRange && this.messageFilters.timeRange.length === 2) {
                params.set('sentFrom', this.messageFilters.timeRange[0]);
                params.set('sentTo', this.messageFilters.timeRange[1]);
            }
            return params;
        },
        publishMessage: async function () {
            if (this.publishForm.mode !== 'client' && !this.publishForm.topic.trim()) {
                this.$message.warning('请输入 MQTT 主题');
                return;
            }
            if (this.publishForm.mode === 'client' && !this.publishForm.clientId.trim() && !this.publishForm.sessionUrl.trim()) {
                this.$message.warning('请输入 clientId 或 sessionUrl');
                return;
            }
            if (this.publishForm.mode === 'url' && !this.publishForm.brokerUrl.trim()) {
                this.$message.warning('请输入 MQTT URL');
                return;
            }
            this.loading.publish = true;
            try {
                var url = '/wcdk/mqtt/publish';
                var body = {
                    topic: this.publishForm.topic,
                    payload: this.publishForm.payload,
                    qos: this.publishForm.qos,
                    retained: this.publishForm.retained
                };
                if (this.publishForm.mode === 'client') {
                    url = '/wcdk/mqtt/publish/client';
                    body.clientId = this.publishForm.clientId.trim();
                    body.sessionUrl = this.publishForm.sessionUrl.trim();
                } else if (this.publishForm.mode === 'url') {
                    url = '/wcdk/mqtt/publish/url';
                    body.brokerUrl = this.publishForm.brokerUrl.trim();
                    body.clientId = this.publishForm.remoteClientId.trim();
                    body.username = this.publishForm.username.trim();
                    body.password = this.publishForm.password;
                }
                await this.request(url, {
                    method: 'POST',
                    body: JSON.stringify(body)
                });
                this.$message.success('消息已发布');
                await Promise.all([this.loadOverview(), this.loadMessages()]);
            } catch (error) {
                this.$message.error(error.message);
            } finally {
                this.loading.publish = false;
            }
        },
        addSubscription: async function () {
            if (!this.subscriptionInputState.value.trim()) {
                this.$message.warning('请输入主题过滤器');
                return;
            }
            this.loading.subscriptions = true;
            try {
                var params = new URLSearchParams();
                params.set('topicFilter', this.subscriptionInputState.value.trim());
                this.subscriptions = await this.request('/wcdk/mqtt/subscriptions?' + params.toString(), {
                    method: 'POST'
                });
                this.$message.success('订阅已添加');
                await this.loadOverview();
            } catch (error) {
                this.$message.error(error.message);
            } finally {
                this.loading.subscriptions = false;
            }
        },
        removeSubscription: async function (topicFilter) {
            this.loading.subscriptions = true;
            try {
                var params = new URLSearchParams();
                params.set('topicFilter', topicFilter);
                this.subscriptions = await this.request('/wcdk/mqtt/subscriptions?' + params.toString(), {
                    method: 'DELETE'
                });
                this.$message.success('订阅已删除');
                await this.loadOverview();
            } catch (error) {
                this.$message.error(error.message);
            } finally {
                this.loading.subscriptions = false;
            }
        },
        clearMessages: async function () {
            this.loading.clearMessages = true;
            try {
                await this.request('/wcdk/mqtt/messages', {
                    method: 'DELETE'
                });
                this.$message.success('消息已清空');
                await Promise.all([this.loadOverview(), this.loadMessages()]);
            } catch (error) {
                this.$message.error(error.message);
            } finally {
                this.loading.clearMessages = false;
            }
        },
        deleteMessagesByFilter: async function () {
            this.loading.deleteMessages = true;
            try {
                var params = this.buildMessageQueryParams();
                var response = await this.request('/wcdk/mqtt/messages/list?' + params.toString(), {
                    method: 'DELETE'
                });
                this.$message.success('Deleted ' + (response && response.deleted ? response.deleted : 0) + ' message records');
                this.messagePagination.pageNo = 1;
                await Promise.all([this.loadOverview(), this.loadMessages()]);
            } catch (error) {
                this.$message.error(error.message);
            } finally {
                this.loading.deleteMessages = false;
            }
        },
        deleteSelectedMessages: async function (selectedMessages) {
            if (!selectedMessages || !selectedMessages.length) {
                this.$message.warning('请先选择要删除的消息');
                return;
            }
            this.loading.deleteMessages = true;
            try {
                var response = await this.request('/wcdk/mqtt/messages/list/selected', {
                    method: 'DELETE',
                    body: JSON.stringify(selectedMessages.map(function (message) {
                        return {
                            id: message.id,
                            topic: message.topic,
                            receivedAt: message.receivedAt
                        };
                    }))
                });
                this.$message.success('已删除 ' + (response && response.deleted ? response.deleted : 0) + ' 条选中消息');
                this.messagePagination.pageNo = 1;
                await Promise.all([this.loadOverview(), this.loadMessages()]);
            } catch (error) {
                this.$message.error(error.message);
            } finally {
                this.loading.deleteMessages = false;
            }
        },
        deleteAllMessages: async function () {
            this.loading.deleteAllMessages = true;
            try {
                var response = await this.request('/wcdk/mqtt/messages/list?all=true', {
                    method: 'DELETE'
                });
                this.$message.success('Deleted all ' + (response && response.deleted ? response.deleted : 0) + ' message records');
                this.messagePagination.pageNo = 1;
                await Promise.all([this.loadOverview(), this.loadMessages()]);
            } catch (error) {
                this.$message.error(error.message);
            } finally {
                this.loading.deleteAllMessages = false;
            }
        },
        configureTimer: function () {
            this.clearTimer();
            if (!this.settings.autoRefresh) {
                return;
            }
            var self = this;
            this.timerId = window.setInterval(function () {
                self.refreshAll();
            }, this.settings.refreshInterval);
        },
        clearTimer: function () {
            if (this.timerId) {
                window.clearInterval(this.timerId);
                this.timerId = null;
            }
        }
    }
});
