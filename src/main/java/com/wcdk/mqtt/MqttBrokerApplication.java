package com.wcdk.mqtt;

import com.wcdk.mqtt.config.MqttProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/***
* MQTT Broker 应用启动入口。
* @author wcdk
***/
@SpringBootApplication
@EnableConfigurationProperties(MqttProperties.class)
@EnableScheduling
public class MqttBrokerApplication {

    /***
    * 应用主入口。
    * @author wcdk
    * @param args 命令行参数
    ***/
    public static void main(String[] args) {
        SpringApplication.run(MqttBrokerApplication.class, args);
    }
}
