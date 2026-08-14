package com.wcdk.mqtt.influx;

import com.influxdb.client.*;
import com.influxdb.client.write.events.WriteErrorEvent;
import java.time.Duration;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/***
 * InfluxDB 配置。
 * @author wcdk
 ***/
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(InfluxDBClient.class)
@EnableConfigurationProperties(InfluxProperties.class)
@ConditionalOnProperty(prefix = "wcdk.influx", name = "enabled", havingValue = "true")
public class InfluxConfiguration {

    private static final Logger log = LoggerFactory.getLogger(InfluxConfiguration.class);

    @Bean(destroyMethod = "close")
    public InfluxDBClient influxDBClient(InfluxProperties properties) {
        OkHttpClient.Builder httpClient = new OkHttpClient.Builder()
                .connectTimeout(Duration.ofSeconds(properties.getTimeoutSeconds()))
                .readTimeout(Duration.ofSeconds(properties.getTimeoutSeconds()))
                .writeTimeout(Duration.ofSeconds(properties.getTimeoutSeconds()));
        InfluxDBClientOptions.Builder options = InfluxDBClientOptions.builder()
                .url(properties.getUrl()).org(properties.getOrg()).bucket(properties.getBucket())
                .okHttpClient(httpClient);
        if (StringUtils.hasText(properties.getToken())) {
            options.authenticateToken(properties.getToken().toCharArray());
        } else if (StringUtils.hasText(properties.getUsername()) && StringUtils.hasText(properties.getPassword())) {
            options.authenticate(properties.getUsername(), properties.getPassword().toCharArray());
        }
        return InfluxDBClientFactory.create(options.build());
    }

    @Bean(destroyMethod = "close")
    public WriteApi influxWriteApi(InfluxDBClient influxDBClient, InfluxProperties properties) {
        WriteOptions writeOptions = WriteOptions.builder().batchSize(Math.max(1, properties.getBatchSize())).flushInterval(Math.max(100, properties.getFlushIntervalMs())).maxRetries(Math.max(1, properties.getMaxRetries())).build();
        WriteApi writeApi = influxDBClient.makeWriteApi(writeOptions);
        writeApi.listenEvents(WriteErrorEvent.class, event -> log.warn(
                "InfluxDB 异步写入失败，MQTT 消息不会因此断开连接", event.getThrowable()));
        return writeApi;
    }

    @Bean
    public WriteApiBlocking influxWriteApiBlocking(InfluxDBClient influxDBClient) {
        return influxDBClient.getWriteApiBlocking();
    }

    @Bean
    public BucketsApi influxBucketsApi(InfluxDBClient client) { return client.getBucketsApi(); }

    @Bean
    public OrganizationsApi influxOrganizationsApi(InfluxDBClient client) { return client.getOrganizationsApi(); }

    @Bean
    public QueryApi influxQueryApi(InfluxDBClient client) { return client.getQueryApi(); }

    @Bean
    public DeleteApi influxDeleteApi(InfluxDBClient client) { return client.getDeleteApi(); }

    @Bean
    public InfluxDbUtil influxDbUtil(InfluxProperties properties, WriteApi writeApi,
                                     WriteApiBlocking writeApiBlocking, QueryApi queryApi,
                                     DeleteApi deleteApi, BucketsApi bucketsApi,
                                     OrganizationsApi organizationsApi) {
        return new InfluxDbUtil(properties, writeApi, writeApiBlocking, queryApi,
                deleteApi, bucketsApi, organizationsApi);
    }
}
