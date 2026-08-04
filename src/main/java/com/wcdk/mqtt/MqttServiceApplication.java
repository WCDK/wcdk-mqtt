package com.wcdk.mqtt;

import com.wcdk.mqtt.core.config.MqttBrokerProperties;
import com.wcdk.mqtt.influx.InfluxDbUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.event.EventListener;

/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
@SpringBootApplication(scanBasePackages = "com.wcdk.mqtt")
@EnableConfigurationProperties(MqttBrokerProperties.class)
public class MqttServiceApplication {

    private static final Logger log = LoggerFactory.getLogger(MqttServiceApplication.class);

    private final MqttBrokerProperties mqttBrokerProperties;

    private final ObjectProvider<InfluxDbUtil> influxDbUtilProvider;

    public MqttServiceApplication(MqttBrokerProperties mqttBrokerProperties,
                                  ObjectProvider<InfluxDbUtil> influxDbUtilProvider) {
        this.mqttBrokerProperties = mqttBrokerProperties;
        this.influxDbUtilProvider = influxDbUtilProvider;
    }

    public static void main(String[] args) {
        log.info("wcdk-mqtt-service 正在启动");
        SpringApplication.run(MqttServiceApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady(ApplicationReadyEvent event) {
        if (!(event.getApplicationContext() instanceof WebServerApplicationContext context)) {
            return;
        }
        int httpPort = context.getWebServer().getPort();
        boolean mqttEnabled = mqttBrokerProperties.isEnabled();
        String mqttHost = mqttBrokerProperties.getHost();
        int mqttPort = mqttBrokerProperties.getPort();
        int clusterPort = mqttBrokerProperties.getCluster().getBindPort();
        initializeInflux();

        log.info("wcdk-mqtt-service 启动完成，服务运行中");
        log.info("HTTP 端口: {}", httpPort);
        log.info("MQTT 监听: {}", mqttEnabled ? mqttHost + ":" + mqttPort : "未启用");
        log.info("Cluster 端口: {}", clusterPort);
    }
    private void initializeInflux() {
        InfluxDbUtil influxDbUtil = influxDbUtilProvider.getIfAvailable();
        if (influxDbUtil == null) {
            return;
        }
        try {
            influxDbUtil.createOrganizationIfMissing();
            influxDbUtil.createBucketIfMissing();
            log.info("InfluxDB org and bucket are ready");
        } catch (RuntimeException ex) {
            log.warn("Initialize InfluxDB org/bucket failed", ex);
        }
    }
}
