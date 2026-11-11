package com.wcdk.mqtt.config;

import com.wcdk.mqtt.core.MqttAcl;
import com.wcdk.mqtt.core.MqttMessageLog;
import com.wcdk.mqtt.core.MqttRetainedStore;
import com.wcdk.mqtt.core.MqttSessionRegistry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/***
* MQTT Broker 核心组件装配配置。
* @author wcdk
***/
@Configuration
@EnableConfigurationProperties(MqttProperties.class)
public class MqttBrokerConfiguration {

    /***
    * 访问控制列表 Bean。
    * @author wcdk
    * @param properties 配置
    * @return 访问控制列表
    ***/
    @Bean
    public MqttAcl mqttAcl(MqttProperties properties) {
        return new MqttAcl(properties);
    }

    /***
    * 保留消息存储 Bean。
    * @author wcdk
    * @return 保留消息存储
    ***/
    @Bean
    public MqttRetainedStore mqttRetainedStore() {
        return new MqttRetainedStore();
    }

    /***
    * 最近消息日志 Bean，并注册为发布监听器。
    * @author wcdk
    * @param properties 配置
    * @return 消息日志
    ***/
    @Bean
    public MqttMessageLog mqttMessageLog(MqttProperties properties) {
        return new MqttMessageLog(Math.max(10, properties.getBroker().getMessageLogCapacity()));
    }

    /***
    * 会话注册表 Bean，装配消息日志监听器（集群与控制台监听器由各自组件自注册）。
    * @author wcdk
    * @param properties 配置
    * @param acl 访问控制列表
    * @param retainedStore 保留消息存储
    * @param messageLog 消息日志
    * @return 会话注册表
    ***/
    @Bean
    public MqttSessionRegistry mqttSessionRegistry(MqttProperties properties,
                                                   MqttAcl acl,
                                                   MqttRetainedStore retainedStore,
                                                   MqttMessageLog messageLog) {
        MqttSessionRegistry registry = new MqttSessionRegistry(properties, acl, retainedStore);
        registry.addPublishListener(messageLog);
        return registry;
    }
}
