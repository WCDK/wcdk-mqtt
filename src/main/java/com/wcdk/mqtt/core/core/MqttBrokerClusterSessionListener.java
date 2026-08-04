package com.wcdk.mqtt.core.core;

import org.springframework.beans.factory.ObjectProvider;

/**
 * @auther WCDK
 * @date 2026/7/29
 * @version 1.0
 **/
public class MqttBrokerClusterSessionListener implements MqttBrokerSessionListener {

    /** 集群管理器提供者，用于获取集群管理器实例 */
    private final ObjectProvider<MqttBrokerClusterManager> clusterManagerProvider;

    /**
     * 构造函数，注入集群管理器提供者
     *
     * @param clusterManagerProvider 集群管理器提供者
     */
    public MqttBrokerClusterSessionListener(ObjectProvider<MqttBrokerClusterManager> clusterManagerProvider) {
        this.clusterManagerProvider = clusterManagerProvider;
    }

    /**
     * 会话上线回调
     * 当客户端会话上线时，同步会话状态到集群
     *
     * @param session 会话对象
     * @param sessionPresent 会话是否已存在
     */
    @Override
    public void onSessionOnline(MqttBrokerSession session, boolean sessionPresent) {
        updateSession(session);
    }

    /**
     * 会话更新回调
     * 当会话状态发生变化时，同步更新到集群
     *
     * @param session 会话对象
     */
    @Override
    public void onSessionUpdated(MqttBrokerSession session) {
        updateSession(session);
    }

    /**
     * 会话离线回调
     * 当客户端会话离线时，同步会话状态到集群
     *
     * @param session 会话对象
     */
    @Override
    public void onSessionOffline(MqttBrokerSession session) {
        MqttBrokerClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        if (clusterManager != null) {
            clusterManager.updateSessionSnapshot(session);
        }
    }

    /**
     * 更新会话状态到集群
     * 通用方法，用于将会话状态同步到集群管理器
     *
     * @param session 需要同步的会话对象
     */
    private void updateSession(MqttBrokerSession session) {
        MqttBrokerClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        if (clusterManager != null) {
            clusterManager.updateSessionSnapshot(session);
        }
    }
}
