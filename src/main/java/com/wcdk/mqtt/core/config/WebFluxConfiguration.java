package com.wcdk.mqtt.core.config;

import java.net.URI;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.reactive.function.server.RouterFunctions;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;

/**
 * @auther WCDK
 * @date 2026/8/3
 * @version 1.0
 **/
@Configuration
public class WebFluxConfiguration {

    /**
     * 将根路径和 /index 重定向到 index.html
     */
    @Bean
    public RouterFunction<ServerResponse> indexRouterFunction() {
        URI indexUri = URI.create("/index.html");
        return RouterFunctions.route(GET("/"), request ->
                        ServerResponse.temporaryRedirect(indexUri).build())
                .andRoute(GET("/index"), request ->
                        ServerResponse.temporaryRedirect(indexUri).build());
    }
}