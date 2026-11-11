package com.wcdk.mqtt.controller;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.assertj.core.api.Assertions.assertThat;

/***
* 管理控制台接口集成测试：覆盖未认证 401、登录页放行、Basic 认证后的接口可用性。
* @author wcdk
***/
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "wcdk.mqtt.broker.port=0",
        "wcdk.mqtt.cluster.enabled=false",
        "wcdk.mqtt.security.console-username=admin",
        "wcdk.mqtt.security.console-password=admin"
})
class ConsoleApiIntegrationTest {

    @org.springframework.boot.test.web.server.LocalServerPort
    private int serverPort;

    private WebTestClient client;

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToServer()
                .baseUrl("http://127.0.0.1:" + serverPort)
                .build();
    }

    /***
    * 验证未认证访问管理接口返回 401 JSON。
    * @author wcdk
    ***/
    @Test
    void apiShouldReturn401WithoutAuthentication() {
        client.get().uri("/api/overview")
                .exchange()
                .expectStatus().isUnauthorized()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody()
                .jsonPath("$.code").isEqualTo(401)
                .jsonPath("$.message").isNotEmpty();

        client.get().uri("/api/clients")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    /***
    * 验证未认证访问控制台页面跳转登录页，登录页本身可匿名访问。
    * @author wcdk
    ***/
    @Test
    void consolePageShouldRedirectAnonymousToLoginPage() {
        client.get().uri("/")
                .exchange()
                .expectStatus().is3xxRedirection()
                .expectHeader().valueEquals(HttpHeaders.LOCATION, "/login.html");

        client.get().uri("/login.html")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .value(body -> assertThat(body).contains("MQTT Broker 控制台"));
    }

    /***
    * 验证错误账号的 Basic 认证被拒绝。
    * @author wcdk
    ***/
    @Test
    void apiShouldRejectBadCredentials() {
        client.get().uri("/api/overview")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("admin", "wrong-password"))
                .exchange()
                .expectStatus().isUnauthorized();
    }

    /***
    * 验证 Basic 认证后可正常查询概览、客户端、消息与集群接口。
    * @author wcdk
    ***/
    @Test
    void apiShouldServeAfterBasicAuthentication() {
        String auth = basicAuth("admin", "admin");

        client.get().uri("/api/overview")
                .header(HttpHeaders.AUTHORIZATION, auth)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.code").isEqualTo(0)
                .jsonPath("$.data.totalSessions").exists()
                .jsonPath("$.data.mqttPort").exists();

        client.get().uri("/api/me")
                .header(HttpHeaders.AUTHORIZATION, auth)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.code").isEqualTo(0)
                .jsonPath("$.data.username").isEqualTo("admin");

        client.get().uri("/api/clients")
                .header(HttpHeaders.AUTHORIZATION, auth)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.code").isEqualTo(0);

        client.get().uri("/api/cluster")
                .header(HttpHeaders.AUTHORIZATION, auth)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.code").isEqualTo(0)
                .jsonPath("$.data.enabled").isEqualTo(false);

        client.get().uri("/index.html")
                .header(HttpHeaders.AUTHORIZATION, auth)
                .exchange()
                .expectStatus().isOk();
    }

    /***
    * 验证模拟发布接口与消息查询接口联动可用。
    * @author wcdk
    ***/
    @Test
    void publishApiShouldAppendMessageLog() {
        String auth = basicAuth("admin", "admin");
        String topic = "console/test/publish";

        client.post().uri("/api/publish")
                .header(HttpHeaders.AUTHORIZATION, auth)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"topic\":\"" + topic + "\",\"payload\":\"hello-console\",\"qos\":1}")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.code").isEqualTo(0)
                .jsonPath("$.data.topic").isEqualTo(topic);

        client.get().uri(uriBuilder -> uriBuilder.path("/api/messages")
                        .queryParam("topicFilter", topic).build())
                .header(HttpHeaders.AUTHORIZATION, auth)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.code").isEqualTo(0)
                .jsonPath("$.data[0].topic").isEqualTo(topic)
                .jsonPath("$.data[0].payload").isEqualTo("hello-console");
    }

    /***
    * 验证参数非法时返回 400 与中文提示。
    * @author wcdk
    ***/
    @Test
    void publishApiShouldRejectBlankTopic() {
        String auth = basicAuth("admin", "admin");

        client.post().uri("/api/publish")
                .header(HttpHeaders.AUTHORIZATION, auth)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"payload\":\"x\"}")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.code").isEqualTo(400)
                .jsonPath("$.message").value(value ->
                        assertThat(String.valueOf(value)).contains("主题"));
    }

    private String basicAuth(String username, String password) {
        String token = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }
}
