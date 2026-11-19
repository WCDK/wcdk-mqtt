/**
 * MQTT Broker 网页控制台前端脚本：REST 查询 + SSE 实时刷新。
 * 作者：wcdk
 */
(function () {
  'use strict';

  var sessionEventLimit = 50;
  var messageLimit = 200;
  var eventSource = null;

  /* ---------- 基础工具 ---------- */

  function $(id) {
    return document.getElementById(id);
  }

  function escapeHtml(text) {
    if (text === null || text === undefined) {
      return '';
    }
    return String(text)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
  }

  function toast(message, isError) {
    var el = $('toast');
    el.textContent = message;
    el.className = 'toast show' + (isError ? ' error' : '');
    window.clearTimeout(el._timer);
    el._timer = window.setTimeout(function () {
      el.className = 'toast';
    }, 2600);
  }

  function formatTime(value) {
    if (!value) {
      return '—';
    }
    var date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    var pad = function (n) { return n < 10 ? '0' + n : '' + n; };
    return date.getFullYear() + '-' + pad(date.getMonth() + 1) + '-' + pad(date.getDate()) +
        ' ' + pad(date.getHours()) + ':' + pad(date.getMinutes()) + ':' + pad(date.getSeconds());
  }

  /**
   * 统一请求封装：非 401/403 时返回解析后的 JSON，401 跳转登录页。
   */
  function request(path, options) {
    return fetch(path, Object.assign({ credentials: 'same-origin' }, options || {}))
        .then(function (response) {
          if (response.status === 401 || response.status === 403) {
            window.location.href = '/login.html';
            return Promise.reject(new Error('未登录'));
          }
          return response.json().catch(function () {
            return { code: response.status, message: '响应解析失败' };
          });
        })
        .then(function (body) {
          if (body && body.code !== undefined && body.code !== 0) {
            throw new Error(body.message || '请求失败');
          }
          return body ? body.data : null;
        });
  }

  /* ---------- 标签页切换 ---------- */

  var tabs = document.querySelectorAll('.tab');
  tabs.forEach(function (tab) {
    tab.addEventListener('click', function () {
      tabs.forEach(function (item) { item.classList.remove('active'); });
      tab.classList.add('active');
      document.querySelectorAll('.panel').forEach(function (panel) {
        panel.classList.remove('active');
      });
      $('panel-' + tab.dataset.tab).classList.add('active');
      refreshActiveTab(tab.dataset.tab);
    });
  });

  function refreshActiveTab(name) {
    if (name === 'overview') {
      loadOverview();
    } else if (name === 'clients') {
      loadClients();
    } else if (name === 'messages') {
      loadMessages();
    } else if (name === 'retained') {
      loadRetained();
    } else if (name === 'cluster') {
      loadCluster();
    }
  }

  /* ---------- 概览 ---------- */

  function card(label, value, small) {
    return '<div class="card"><div class="label">' + escapeHtml(label) +
        '</div><div class="value' + (small ? ' small' : '') + '">' + escapeHtml(value) + '</div></div>';
  }

  function loadOverview() {
    request('/api/overview').then(function (data) {
      if (!data) {
        return;
      }
      $('overviewCards').innerHTML =
          card('在线会话', data.activeSessions) +
          card('离线会话', data.offlineSessions) +
          card('远端会话', data.remoteSessions) +
          card('订阅总数', data.totalSubscriptions) +
          card('保留消息', data.retainedMessages) +
          card('离线缓冲客户端', data.bufferedOfflineClients) +
          card('消息日志', data.messageLogSize + ' / ' + data.messageLogCapacity) +
          card('MQTT 监听', data.mqttEnabled ? data.mqttHost + ':' + data.mqttPort : '已关闭', true) +
          card('集群', data.clusterEnabled ? ('已开启 · ' + data.nodeId + ' · 对等 ' + data.clusterPeerCount) : '单实例', true) +
          card('运行时长', formatDuration(data.uptimeSeconds), true);
    }).catch(function (error) {
      toast(error.message, true);
    });
  }

  function formatDuration(seconds) {
    if (seconds === undefined || seconds === null) {
      return '—';
    }
    var s = Number(seconds);
    var days = Math.floor(s / 86400);
    var hours = Math.floor((s % 86400) / 3600);
    var minutes = Math.floor((s % 3600) / 60);
    var rest = s % 60;
    var parts = [];
    if (days) { parts.push(days + ' 天'); }
    if (hours) { parts.push(hours + ' 小时'); }
    if (minutes) { parts.push(minutes + ' 分'); }
    parts.push(rest + ' 秒');
    return parts.join(' ');
  }

  /* ---------- 客户端 ---------- */

  function loadClients() {
    var params = new URLSearchParams();
    var keyword = $('clientKeyword').value.trim();
    var online = $('clientOnline').value;
    if (keyword) {
      params.set('keyword', keyword);
    }
    if (online) {
      params.set('online', online);
    }
    request('/api/clients?' + params.toString()).then(function (list) {
      var rows = (list || []).map(function (item) {
        var subscriptionCount = item.subscriptions ? Object.keys(item.subscriptions).length : 0;
        return '<tr>' +
            '<td>' + escapeHtml(item.clientId) + '</td>' +
            '<td>' + escapeHtml(item.username || '—') + '</td>' +
            '<td><span class="badge ' + (item.online ? 'on' : 'off') + '">' +
            (item.online ? '在线' : '离线') + '</span></td>' +
            '<td><span class="badge ' + (item.source === 'remote' ? 'remote' : 'local') + '">' +
            escapeHtml(item.source) + '</span></td>' +
            '<td>' + escapeHtml(item.ownerNodeId || '—') + '</td>' +
            '<td>' + escapeHtml(item.remoteAddress || '—') + '</td>' +
            '<td>' + (item.keepAliveSeconds || 0) + ' 秒</td>' +
            '<td>' + subscriptionCount + '</td>' +
            '<td>' + (item.offlineQueueSize || 0) + '</td>' +
            '<td>' + (item.outboundPendingCount || 0) + '</td>' +
            '<td><div class="row-actions">' +
            '<button data-action="subs" data-client="' + escapeHtml(item.clientId) + '">订阅</button>' +
            '<button data-action="kick" data-client="' + escapeHtml(item.clientId) + '" class="danger">下线</button>' +
            '</div></td>' +
            '</tr>';
      });
      $('clientRows').innerHTML = rows.length
          ? rows.join('')
          : '<tr><td colspan="11" class="empty">暂无客户端</td></tr>';
    }).catch(function (error) {
      toast(error.message, true);
    });
  }

  $('clientSearch').addEventListener('click', loadClients);
  $('clientKeyword').addEventListener('keydown', function (event) {
    if (event.key === 'Enter') {
      loadClients();
    }
  });

  $('clientRows').addEventListener('click', function (event) {
    var button = event.target.closest('button');
    if (!button) {
      return;
    }
    var clientId = button.dataset.client;
    if (button.dataset.action === 'subs') {
      showSubscriptions(clientId);
    } else if (button.dataset.action === 'kick') {
      var clear = window.confirm('确认强制下线「' + clientId + '」？点击「确定」同时清除会话，点击「取消」仅断开连接。');
      request('/api/clients/' + encodeURIComponent(clientId) + '?clear=' + clear, { method: 'DELETE' })
          .then(function () {
            toast(clear ? '已下线并清除会话' : '已强制下线');
            loadClients();
          })
          .catch(function (error) {
            toast(error.message, true);
          });
    }
  });

  function showSubscriptions(clientId) {
    request('/api/clients/' + encodeURIComponent(clientId) + '/subscriptions').then(function (list) {
      $('subscriptionBox').hidden = false;
      $('subscriptionOwner').textContent = clientId;
      var rows = (list || []).map(function (item) {
        return '<tr><td>' + escapeHtml(item.topicFilter) + '</td><td>' + item.qos + '</td></tr>';
      });
      $('subscriptionRows').innerHTML = rows.length
          ? rows.join('')
          : '<tr><td colspan="2" class="empty">该客户端暂无订阅</td></tr>';
    }).catch(function (error) {
      toast(error.message, true);
    });
  }

  /* ---------- 消息 ---------- */

  function messageRow(item) {
    return '<tr>' +
        '<td>' + formatTime(item.receivedAt) + '</td>' +
        '<td>' + escapeHtml(item.topic) + '</td>' +
        '<td>' + item.qos + '</td>' +
        '<td>' + (item.retained ? '是' : '否') + '</td>' +
        '<td class="wrap">' +
        (item.binary ? '<span class="badge">Base64</span> ' : '') +
        escapeHtml(item.payload) + '</td>' +
        '<td class="wrap">' + escapeHtml((item.matchedSubscriptions || []).join(', ')) + '</td>' +
        '</tr>';
  }

  function loadMessages() {
    var params = new URLSearchParams();
    var topic = $('msgTopic').value.trim();
    var qos = $('msgQos').value;
    if (topic) {
      params.set('topicFilter', topic);
    }
    if (qos) {
      params.set('qos', qos);
    }
    params.set('limit', messageLimit);
    request('/api/messages?' + params.toString()).then(function (list) {
      var rows = (list || []).map(messageRow);
      $('messageRows').innerHTML = rows.length
          ? rows.join('')
          : '<tr><td colspan="6" class="empty">暂无消息</td></tr>';
    }).catch(function (error) {
      toast(error.message, true);
    });
  }

  $('msgSearch').addEventListener('click', loadMessages);
  $('msgClear').addEventListener('click', function () {
    if (!window.confirm('确认清空当前条件下的消息日志？')) {
      return;
    }
    var params = new URLSearchParams();
    var topic = $('msgTopic').value.trim();
    var qos = $('msgQos').value;
    if (topic) {
      params.set('topicFilter', topic);
    }
    if (qos) {
      params.set('qos', qos);
    }
    request('/api/messages?' + params.toString(), { method: 'DELETE' })
        .then(function (data) {
          toast('已删除 ' + (data ? data.deleted : 0) + ' 条消息');
          loadMessages();
        })
        .catch(function (error) {
          toast(error.message, true);
        });
  });

  /* ---------- 保留消息 ---------- */

  function loadRetained() {
    request('/api/retained').then(function (list) {
      var rows = (list || []).map(function (item) {
        return '<tr>' +
            '<td>' + escapeHtml(item.topic) + '</td>' +
            '<td>' + item.qos + '</td>' +
            '<td class="wrap">' + escapeHtml(item.payloadText) + '</td>' +
            '<td><div class="row-actions">' +
            '<button data-action="del-retained" data-topic="' + escapeHtml(item.topic) + '" class="danger">删除</button>' +
            '</div></td>' +
            '</tr>';
      });
      $('retainedRows').innerHTML = rows.length
          ? rows.join('')
          : '<tr><td colspan="4" class="empty">暂无保留消息</td></tr>';
    }).catch(function (error) {
      toast(error.message, true);
    });
  }

  $('retainedRefresh').addEventListener('click', loadRetained);
  $('retainedRows').addEventListener('click', function (event) {
    var button = event.target.closest('button');
    if (!button || button.dataset.action !== 'del-retained') {
      return;
    }
    var topic = button.dataset.topic;
    if (!window.confirm('确认删除保留消息「' + topic + '」？')) {
      return;
    }
    request('/api/retained?topic=' + encodeURIComponent(topic), { method: 'DELETE' })
        .then(function () {
          toast('保留消息已删除');
          loadRetained();
        })
        .catch(function (error) {
          toast(error.message, true);
        });
  });

  /* ---------- 集群 ---------- */

  function loadCluster() {
    request('/api/cluster').then(function (data) {
      if (!data) {
        return;
      }
      $('clusterCards').innerHTML =
          card('集群状态', data.enabled ? '已开启' : '单实例') +
          card('本节点 ID', data.nodeId || '—', true) +
          card('公布地址', data.advertiseAddress || '—', true) +
          card('监听端口', data.bindHost + ':' + data.bindPort, true) +
          card('已连接对等节点', data.connectedPeers) +
          card('多数派状态', data.enabled
              ? (data.hasQuorum ? '已达成' : '未达成 · 只读')
              : '单实例无需多数派', true) +
          card('可达 / 固定成员', data.reachableNodes + ' / ' + data.expectedNodes, true) +
          card('多数派阈值', data.quorumSize, true);
      var rows = (data.nodes || []).map(function (node) {
        return '<tr>' +
            '<td>' + escapeHtml(node.nodeId) + '</td>' +
            '<td>' + escapeHtml(node.address || '—') + '</td>' +
            '<td><span class="badge ' + (node.connected ? 'on' : 'off') + '">' +
            (node.connected ? '在线' : '断开') + '</span></td>' +
            '<td>' + (node.dialed ? '本节点拨出' : '被动接入') + '</td>' +
            '<td>' + formatTime(node.lastSeenAt) + '</td>' +
            '</tr>';
      });
      $('clusterRows').innerHTML = rows.length
          ? rows.join('')
          : '<tr><td colspan="5" class="empty">暂无对等节点</td></tr>';
      var peers = data.configuredPeers || [];
      $('configuredPeers').innerHTML = peers.length
          ? peers.map(function (address) {
            return '<span class="peer-item">' + escapeHtml(address) + '</span>';
          }).join('')
          : '<span class="peer-item">未配置对等地址</span>';
    }).catch(function (error) {
      toast(error.message, true);
    });
  }

  /* ---------- 发布 ---------- */

  $('pubSubmit').addEventListener('click', function () {
    var body = {
      topic: $('pubTopic').value.trim(),
      payload: $('pubPayload').value,
      payloadEncoding: $('pubEncoding').value,
      qos: Number($('pubQos').value),
      retained: $('pubRetained').checked,
      clientId: $('pubClientId').value.trim() || null
    };
    request('/api/publish', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    }).then(function (data) {
      $('pubResult').className = 'form-result';
      $('pubResult').textContent = '发布成功，消息 ID：' + data.messageId +
          (data.directDelivered ? '（已点对点投递）' : '');
      toast('消息已发布');
    }).catch(function (error) {
      $('pubResult').className = 'form-result error';
      $('pubResult').textContent = error.message;
      toast(error.message, true);
    });
  });

  /* ---------- SSE 实时事件 ---------- */

  function appendSessionEvent(action, data) {
    var tbody = $('sessionEvents');
    var row = document.createElement('tr');
    row.innerHTML = '<td>' + formatTime(new Date().toISOString()) + '</td>' +
        '<td>' + escapeHtml(actionLabel(action)) + '</td>' +
        '<td>' + escapeHtml(data.clientId || '—') + '</td>' +
        '<td>' + escapeHtml(data.username || '—') + '</td>' +
        '<td>' + escapeHtml(data.ownerNodeId || '—') + '</td>' +
        '<td>' + escapeHtml(data.remoteAddress || '—') + '</td>';
    tbody.insertBefore(row, tbody.firstChild);
    while (tbody.children.length > sessionEventLimit) {
      tbody.removeChild(tbody.lastChild);
    }
  }

  function actionLabel(action) {
    if (action === 'online') {
      return '上线';
    }
    if (action === 'offline') {
      return '离线';
    }
    if (action === 'removed') {
      return '会话销毁';
    }
    return action;
  }

  function appendLiveMessage(data) {
    if (!$('msgLive').checked) {
      return;
    }
    var panelActive = $('panel-messages').classList.contains('active');
    var topicFilter = panelActive ? $('msgTopic').value.trim() : '';
    var qosFilter = panelActive ? $('msgQos').value : '';
    if (topicFilter && !topicMatch(topicFilter, data.topic)) {
      return;
    }
    if (qosFilter && String(data.qos) !== qosFilter) {
      return;
    }
    var tbody = $('messageRows');
    var wrapper = document.createElement('tbody');
    wrapper.innerHTML = messageRow({
      topic: data.topic,
      qos: data.qos,
      retained: data.retained,
      payload: data.payload,
      matchedSubscriptions: data.matchedSubscriptions,
      receivedAt: data.receivedAt
    });
    var row = wrapper.firstChild;
    if (tbody.querySelector('.empty')) {
      tbody.innerHTML = '';
    }
    tbody.insertBefore(row, tbody.firstChild);
    while (tbody.children.length > messageLimit) {
      tbody.removeChild(tbody.lastChild);
    }
  }

  function topicMatch(filter, topic) {
    var parts = filter.split('/');
    var topics = topic.split('/');
    for (var i = 0; i < parts.length; i++) {
      if (parts[i] === '#') {
        return true;
      }
      if (i >= topics.length) {
        return false;
      }
      if (parts[i] !== '+' && parts[i] !== topics[i]) {
        return false;
      }
    }
    return parts.length === topics.length;
  }

  function connectStream() {
    if (!window.EventSource) {
      $('liveState').textContent = '浏览器不支持实时推送';
      $('liveState').className = 'live-state offline';
      return;
    }
    if (eventSource) {
      eventSource.close();
    }
    eventSource = new EventSource('/api/stream');
    eventSource.onopen = function () {
      $('liveState').textContent = '实时连接中';
      $('liveState').className = 'live-state';
    };
    eventSource.onerror = function () {
      $('liveState').textContent = '实时连接断开，自动重连中…';
      $('liveState').className = 'live-state offline';
    };
    eventSource.addEventListener('message-append', function (event) {
      handleEvent(event);
    });
    ['session-online', 'session-offline', 'session-removed'].forEach(function (name) {
      eventSource.addEventListener(name, handleEvent);
    });
  }

  function handleEvent(event) {
    var parsed;
    try {
      parsed = JSON.parse(event.data);
    } catch (error) {
      return;
    }
    if (!parsed) {
      return;
    }
    if (parsed.type === 'message' && parsed.action === 'append') {
      appendLiveMessage(parsed.data || {});
    } else if (parsed.type === 'session') {
      appendSessionEvent(parsed.action, parsed.data || {});
      loadOverviewThrottled();
      if ($('panel-clients').classList.contains('active')) {
        loadClientsThrottled();
      }
    }
  }

  var overviewTimer = null;
  function loadOverviewThrottled() {
    if (overviewTimer) {
      return;
    }
    overviewTimer = window.setTimeout(function () {
      overviewTimer = null;
      loadOverview();
    }, 1500);
  }

  var clientsTimer = null;
  function loadClientsThrottled() {
    if (clientsTimer) {
      return;
    }
    clientsTimer = window.setTimeout(function () {
      clientsTimer = null;
      loadClients();
    }, 2000);
  }

  /* ---------- 当前用户与退出登录 ---------- */

  function loadCurrentUser() {
    request('/api/me').then(function (data) {
      if (data) {
        $('currentUser').textContent = data.username || '已登录';
      }
    }).catch(function () {
      /* 401 由 request 统一跳转 */
    });
  }

  document.querySelector('.logout').addEventListener('click', function (event) {
    event.preventDefault();
    fetch('/logout', { method: 'POST', credentials: 'same-origin' })
        .finally(function () {
          window.location.href = '/login.html?logout';
        });
  });

  /* ---------- 启动 ---------- */

  loadCurrentUser();
  loadOverview();
  connectStream();
})();
