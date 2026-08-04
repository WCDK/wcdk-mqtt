package com.wcdk.mqtt.influx;

import com.influxdb.client.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(InfluxDBClient.class)
@EnableConfigurationProperties(InfluxProperties.class)
@ConditionalOnProperty(prefix = "wcdk.influx", name = "enabled", havingValue = "true")
public class InfluxConfiguration {

    @Bean(destroyMethod = "close")
    
    public InfluxDBClient influxDBClient(InfluxProperties properties) {
        if (StringUtils.hasText(properties.getToken())) {
            return InfluxDBClientFactory.create(
                    properties.getUrl(),
                    properties.getToken().toCharArray(),
                    properties.getOrg(),
                    properties.getBucket()
            );
        }
        if (StringUtils.hasText(properties.getUsername()) && StringUtils.hasText(properties.getPassword())) {
            return InfluxDBClientFactory.create(
                    properties.getUrl(),
                    properties.getUsername(),
                    properties.getPassword().toCharArray()
            );
        }
        return InfluxDBClientFactory.create(properties.getUrl());
    }

    @Bean
    
    public WriteApiBlocking influxWriteApiBlocking(InfluxDBClient influxDBClient) {
        return influxDBClient.getWriteApiBlocking();
    }

    @Bean
    
    public BucketsApi influxBucketsApi(InfluxDBClient influxDBClient) {
        return influxDBClient.getBucketsApi();
    }

    @Bean
    
    public OrganizationsApi influxOrganizationsApi(InfluxDBClient influxDBClient) {
        return influxDBClient.getOrganizationsApi();
    }

    @Bean
    
    public QueryApi influxQueryApi(InfluxDBClient influxDBClient) {
        return influxDBClient.getQueryApi();
    }

    @Bean
    
    public DeleteApi influxDeleteApi(InfluxDBClient influxDBClient) {
        return influxDBClient.getDeleteApi();
    }

    @Bean
    
    public InfluxDbUtil influxDbUtil(InfluxProperties properties,
                                     WriteApiBlocking writeApiBlocking,
                                     QueryApi queryApi,
                                     DeleteApi deleteApi,
                                     BucketsApi bucketsApi,
                                     OrganizationsApi organizationsApi) {
        return new InfluxDbUtil(properties, writeApiBlocking, queryApi, deleteApi, bucketsApi, organizationsApi);
    }
}
