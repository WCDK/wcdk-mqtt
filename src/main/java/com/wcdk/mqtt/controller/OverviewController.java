package com.wcdk.mqtt.controller;

import java.util.LinkedHashMap;
import java.util.Map;

import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.service.MqttBrokerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/***
* Broker 运行概览与当前登录用户接口。
* @author wcdk
***/
@RestController
@RequestMapping("/api")
public class OverviewController {

    private final MqttBrokerService brokerService;
    private final MqttProperties properties;

    public OverviewController(MqttBrokerService brokerService, MqttProperties properties) {
        this.brokerService = brokerService;
        this.properties = properties;
    }

    /***
    * 查询 Broker 运行概览统计。
    * @author wcdk
    * @return 概览视图
    ***/
    @GetMapping("/overview")
    public Mono<ApiResponse<MqttBrokerService.Overview>> overview() {
        return Mono.fromSupplier(() -> ApiResponse.ok(brokerService.overview()));
    }

    /***
    * 查询当前登录控制台的用户信息，供网页判断登录状态。
    * @author wcdk
    * @param principal 当前登录用户
    * @return 用户信息
    ***/
    @GetMapping("/me")
    public Mono<ApiResponse<Map<String, Object>>> me(java.security.Principal principal) {
        return Mono.fromSupplier(() -> {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("authenticated", principal != null);
            data.put("username", principal == null ? null : principal.getName());
            data.put("mqttPort", properties.getBroker().getPort());
            data.put("consoleEnabled", properties.getConsole().isEnabled());
            return ApiResponse.ok(data);
        });
    }
}
