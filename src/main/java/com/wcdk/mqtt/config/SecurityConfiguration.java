package com.wcdk.mqtt.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService;
import org.springframework.security.core.userdetails.ReactiveUserDetailsService;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

/***
* 控制台与管理接口安全配置，采用表单登录并为脚本提供 HTTP Basic。
* @author wcdk
***/
@Configuration
public class SecurityConfiguration {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfiguration.class);

    /***
    * 构建 Web 安全过滤链：登录页与静态资源放行，其余接口需要认证。
    * @author wcdk
    * @param http 安全构建器
    * @param passwordEncoder 密码编码器
    * @return 过滤链
    ***/
    @Bean
    @Order(1)
    public SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http, PasswordEncoder passwordEncoder) {
        http
                // 控制台为同源简单应用，关闭 CSRF 以简化接口调用
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .authorizeExchange(exchanges -> exchanges
                        .pathMatchers("/login.html", "/login", "/app.js", "/favicon.ico",
                                "/css/**", "/js/**", "/images/**", "/error")
                        .permitAll()
                        .pathMatchers("/api/**").authenticated()
                        .anyExchange().authenticated())
                .exceptionHandling(exceptions -> exceptions.authenticationEntryPoint((exchange, denied) -> {
                    // API 未认证返回 401 JSON，页面跳转登录页
                    var response = exchange.getResponse();
                    String path = exchange.getRequest().getPath().value();
                    if (path.startsWith("/api/")) {
                        response.setStatusCode(org.springframework.http.HttpStatus.UNAUTHORIZED);
                        response.getHeaders().setContentType(org.springframework.http.MediaType.APPLICATION_JSON);
                        byte[] body = "{\"code\":401,\"message\":\"未登录或会话已过期\"}"
                                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
                    }
                    response.setStatusCode(org.springframework.http.HttpStatus.FOUND);
                    response.getHeaders().setLocation(java.net.URI.create("/login.html"));
                    return response.setComplete();
                }))
                .formLogin(formLogin -> formLogin.loginPage("/login.html"))
                .logout(logout -> logout.logoutSuccessHandler(successHandler()))
                .httpBasic(Customizer.withDefaults());
        return http.build();
    }

    /***
    * 登出成功后跳回登录页。
    * @author wcdk
    * @return 登出成功处理器
    ***/
    private org.springframework.security.web.server.authentication.logout.RedirectServerLogoutSuccessHandler successHandler() {
        var handler =
                new org.springframework.security.web.server.authentication.logout.RedirectServerLogoutSuccessHandler();
        handler.setLogoutSuccessUrl(java.net.URI.create("/login.html"));
        return handler;
    }

    /***
    * 控制台用户详情服务，凭据优先级：控制台账号 > MQTT 账号 > 默认 admin/admin。
    * @author wcdk
    * @param properties 配置
    * @return 用户详情服务
    ***/
    @Bean
    public ReactiveUserDetailsService userDetailsService(MqttProperties properties, PasswordEncoder passwordEncoder) {
        MqttProperties.Security security = properties.getSecurity();
        String username = firstNonBlank(security.getConsoleUsername(), security.getUsername(), "admin");
        String password = firstNonBlank(security.getConsolePassword(), security.getPassword(), "admin");
        if ("admin".equals(username) && "admin".equals(password)) {
            log.warn("控制台使用默认账号 admin/admin，生产环境请通过 wcdk.mqtt.security.console-username/console-password 修改");
        }
        UserDetails user = User.withUsername(username)
                .password(passwordEncoder.encode(password))
                .roles("ADMIN")
                .build();
        return new MapReactiveUserDetailsService(user);
    }

    /***
    * 密码编码器，支持 {noop} 等多种编码格式。
    * @author wcdk
    * @return 密码编码器
    ***/
    @Bean
    public PasswordEncoder passwordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (StringUtils.hasText(value)) {
                return value.trim();
            }
        }
        return null;
    }
}
