package com.wcdk.mqtt.core;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
@ConditionalOnClass(OpenAPI.class)
@ConditionalOnProperty(prefix = "knife4j", name = "enabled", havingValue = "true")
public class Knife4jConfiguration {

    @Value("${server.port}")
    private int port;
    @Value("${spring.application.name}")
    private String serverName;
    @Value("${spring.application.name-cn:}")
    private String serverNameCN;
    @Value("${wcdk.swagger.title}")
    private String title;
    @Value("${wcdk.swagger.version}")
    private String version;


    @Bean
    @ConditionalOnMissingBean
    public OpenAPI wcdkOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title(title+serverNameCN)
                        .description(serverNameCN)
                        .version(version));
    }

}
