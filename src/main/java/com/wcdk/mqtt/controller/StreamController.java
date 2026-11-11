package com.wcdk.mqtt.controller;

import java.time.Duration;

import com.wcdk.mqtt.web.ConsoleEventBus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/***
* 控制台实时事件流接口（SSE）。
* @author wcdk
***/
@RestController
@RequestMapping("/api")
public class StreamController {

    private final ConsoleEventBus eventBus;

    public StreamController(ConsoleEventBus eventBus) {
        this.eventBus = eventBus;
    }

    /***
    * 订阅消息与会话实时事件流，每 15 秒发送注释心跳维持连接。
    * @author wcdk
    * @return SSE 事件流
    ***/
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<ConsoleEventBus.ConsoleEvent>> stream() {
        Flux<ServerSentEvent<ConsoleEventBus.ConsoleEvent>> events = eventBus.stream().asFlux()
                .map(event -> ServerSentEvent.<ConsoleEventBus.ConsoleEvent>builder(event)
                        .event(event.type() + "-" + event.action())
                        .build());
        Flux<ServerSentEvent<ConsoleEventBus.ConsoleEvent>> heartbeat = Flux.interval(Duration.ofSeconds(15))
                .map(tick -> ServerSentEvent.<ConsoleEventBus.ConsoleEvent>builder()
                        .comment("心跳")
                        .build());
        return Flux.merge(events, heartbeat);
    }
}
