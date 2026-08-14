package com.wcdk.mqtt.influx;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
@Data
@ConfigurationProperties(prefix = "wcdk.influx")
public class InfluxProperties {

    private boolean enabled;

    private String url = "http://localhost:8086";

    private String token;

    private String username;

    private String password;

    private String org;

    private String bucket;

    private long timeoutSeconds = 120;
    private int batchSize = 500;
    private int flushIntervalMs = 1000;
    private int maxRetries = 1;

    private String queryRange = "-7d";


}
