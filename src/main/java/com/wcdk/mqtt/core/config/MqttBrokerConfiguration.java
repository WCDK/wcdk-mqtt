package com.wcdk.mqtt.core.config;

import com.wcdk.mqtt.core.cluster.MqttClusterConfigSyncService;
import com.wcdk.mqtt.core.core.MqttBrokerAcl;
import com.wcdk.mqtt.core.core.MqttBrokerClusterManager;
import com.wcdk.mqtt.core.core.MqttBrokerClusterSessionListener;
import com.wcdk.mqtt.core.core.MqttBrokerPublishListener;
import com.wcdk.mqtt.core.core.MqttBrokerSessionListener;
import com.wcdk.mqtt.core.core.MqttBrokerSessionRegistry;
import com.wcdk.mqtt.core.core.ReactorMqttBroker;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import reactor.netty.tcp.TcpServer;

/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(TcpServer.class)
@ConditionalOnProperty(prefix = "wcdk.mqtt.broker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MqttBrokerConfiguration {

    private final Environment environment;

    public MqttBrokerConfiguration(Environment environment) {
        this.environment = environment;
    }

    @Bean
    public MqttBrokerAcl mqttBrokerAcl(MqttBrokerProperties properties) {
        bindBrokerProperties(properties);
        return new MqttBrokerAcl(properties);
    }

    @Bean
    @ConditionalOnProperty(prefix = "wcdk.mqtt.broker.cluster", name = "enabled", havingValue = "true")
    public MqttBrokerSessionListener mqttBrokerClusterSessionListener(ObjectProvider<MqttBrokerClusterManager> clusterManagerProvider) {
        return new MqttBrokerClusterSessionListener(clusterManagerProvider);
    }

    @Bean
    public MqttBrokerSessionRegistry mqttBrokerSessionRegistry(MqttBrokerProperties properties,
                                                               MqttBrokerAcl mqttBrokerAcl,
                                                               ObjectProvider<MqttBrokerPublishListener> publishListeners,
                                                               ObjectProvider<MqttBrokerSessionListener> sessionListeners) {
        bindBrokerProperties(properties);
        return new MqttBrokerSessionRegistry(
                properties,
                mqttBrokerAcl,
                publishListeners.orderedStream().toList(),
                sessionListeners.orderedStream().toList());
    }

    @Bean
    @ConditionalOnProperty(prefix = "wcdk.mqtt.broker.cluster", name = "enabled", havingValue = "true")
    public MqttBrokerClusterManager mqttBrokerClusterManager(MqttBrokerProperties properties,
                                                             MqttBrokerSessionRegistry sessionRegistry,
                                                             ObjectProvider<MqttClusterConfigSyncService> configSyncServiceProvider) {
        bindBrokerProperties(properties);
        return new MqttBrokerClusterManager(properties, sessionRegistry, configSyncServiceProvider);
    }

    @Bean
    public ReactorMqttBroker reactorMqttBroker(MqttBrokerProperties properties,
                                               MqttBrokerSessionRegistry sessionRegistry,
                                               ObjectProvider<MqttBrokerClusterManager> clusterManagerProvider) {
        bindBrokerProperties(properties);
        return new ReactorMqttBroker(properties, sessionRegistry, clusterManagerProvider);
    }

    private void bindBrokerProperties(MqttBrokerProperties properties) {
        Binder.get(environment).bind("wcdk.mqtt.broker", Bindable.ofInstance(properties));
    }
}