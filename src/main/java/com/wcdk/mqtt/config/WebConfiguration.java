package com.wcdk.mqtt.config;

import java.net.URI;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

/***
* Web 路由配置，根路径重定向到控制台首页。
* @author wcdk
***/
@Configuration
public class WebConfiguration {

    /***
    * 根路径到控制台首页的重定向路由。
    * @author wcdk
    * @return 路由函数
    ***/
    @Bean
    public RouterFunction<ServerResponse> rootRedirectRouter() {
        return RouterFunctions.route(
                org.springframework.web.reactive.function.server.RequestPredicates.GET("/"),
                request -> ServerResponse.temporaryRedirect(URI.create("/index.html")).build());
    }

    /***
    * 无扩展名的 /login 访问重定向到登录页，兼容旧链接与表单提交入口。
    * @author wcdk
    * @return 路由函数
    ***/
    @Bean
    public RouterFunction<ServerResponse> loginRedirectRouter() {
        return RouterFunctions.route(
                org.springframework.web.reactive.function.server.RequestPredicates.GET("/login"),
                request -> ServerResponse.temporaryRedirect(URI.create("/login.html")).build());
    }
}
