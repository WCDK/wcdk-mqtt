package com.wcdk.mqtt.core.config;

import java.net.URI;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.ReactiveAuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.authentication.RedirectServerAuthenticationSuccessHandler;
import org.springframework.security.web.server.util.matcher.ServerWebExchangeMatchers;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

/**
 * @auther WCDK
 * @date 2026/8/3
 * @version 1.0
 **/
@Configuration
@EnableWebFluxSecurity
public class SecurityConfiguration {

    private final MqttBrokerProperties mqttBrokerProperties;

    public SecurityConfiguration(MqttBrokerProperties mqttBrokerProperties) {
        this.mqttBrokerProperties = mqttBrokerProperties;
    }

    @Bean
    public SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return http
                .authenticationManager(authenticationManager())
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .authorizeExchange(exchanges -> exchanges
                        .pathMatchers("/login.html", "/css/**", "/js/**", "/vendor/**", "/favicon.ico", "/favicon2.ico").permitAll()
                        .pathMatchers("/swagger-ui/**", "/swagger-ui.html", "/v3/api-docs/**", "/doc.html", "/webjars/**").permitAll()
                        .pathMatchers("/wcdk/mqtt/cluster/sync/**").permitAll()
                        .anyExchange().authenticated()
                )
                .exceptionHandling(exceptionHandling -> exceptionHandling
                        .authenticationEntryPoint((exchange, exception) -> {
                            String path = exchange.getRequest().getPath().pathWithinApplication().value();
                            if (path.startsWith("/wcdk/mqtt/")) {
                                exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
                                return exchange.getResponse().setComplete();
                            }
                            exchange.getResponse().setStatusCode(HttpStatus.FOUND);
                            exchange.getResponse().getHeaders().setLocation(URI.create("/login.html"));
                            return exchange.getResponse().setComplete();
                        })
                )
                .formLogin(formLogin -> formLogin
                        .loginPage("/login.html")
                        .requiresAuthenticationMatcher(ServerWebExchangeMatchers.pathMatchers(HttpMethod.POST, "/login"))
                        .authenticationSuccessHandler(new RedirectServerAuthenticationSuccessHandler("/index.html"))
                        .authenticationFailureHandler((exchange, exception) -> {
                            exchange.getExchange().getResponse().setStatusCode(HttpStatus.FOUND);
                            exchange.getExchange().getResponse().getHeaders().setLocation(URI.create("/login.html?error=true"));
                            return exchange.getExchange().getResponse().setComplete();
                        })
                )
                .logout(logout -> logout
                        .logoutUrl("/logout")
                        .logoutSuccessHandler((exchange, authentication) -> {
                            exchange.getExchange().getResponse().setStatusCode(HttpStatus.FOUND);
                            exchange.getExchange().getResponse().getHeaders().setLocation(URI.create("/login.html?logout=true"));
                            return Mono.empty();
                        })
                )
                .build();
    }

    @Bean
    public ReactiveAuthenticationManager authenticationManager() {
        return authentication -> {
            String username = authentication.getName();
            String password = authentication.getCredentials() == null ? "" : authentication.getCredentials().toString();
            String expectedUsername = resolveUsername();
            String expectedPassword = resolvePassword();
            if (expectedUsername.equals(username) && expectedPassword.equals(password)) {
                UserDetails user = new User(expectedUsername, "", List.of(new SimpleGrantedAuthority("ROLE_ADMIN")));
                return Mono.just((Authentication) new UsernamePasswordAuthenticationToken(user, null, user.getAuthorities()));
            }
            return Mono.error(new BadCredentialsException("Invalid username or password"));
        };
    }

    private String resolveUsername() {
        String username = mqttBrokerProperties.getUsername();
        return StringUtils.hasText(username) ? username : "admin";
    }

    private String resolvePassword() {
        String password = mqttBrokerProperties.getPassword();
        return StringUtils.hasText(password) ? password : "admin";
    }
}