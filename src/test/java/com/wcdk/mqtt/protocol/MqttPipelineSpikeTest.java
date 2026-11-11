package com.wcdk.mqtt.protocol;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;

import static org.assertj.core.api.Assertions.assertThat;

/***
* Reactor Netty MQTT 管道接入方式验证测试。
* @author wcdk
***/
class MqttPipelineSpikeTest {

    /***
    * 验证 addFirst 接入编解码器后，receiveObject 能收到解码后的 MQTT 报文并完成握手。
    * @author wcdk
    ***/
    @Test
    void connectHandshakeShouldSucceed() throws Exception {
        // 启动一个临时 MQTT 服务端，收到 CONNECT 后返回 CONNACK
        DisposableServer server = TcpServer.create()
                .port(0)
                .doOnChannelInit((connection, channel, remoteAddress) -> {
                    // 编解码器必须放在 Reactor Netty 桥接处理器之前，才能正确拦截入站字节
                    channel.pipeline().addFirst("mqttDecoder", new MqttDecoder(1024 * 1024));
                    channel.pipeline().addFirst("mqttEncoder", MqttEncoder.INSTANCE);
                })
                .handle((inbound, outbound) -> {
                    // 每个连接一个出站汇聚点，所有下行报文都经过它串行写出
                    Sinks.Many<MqttMessage> outboundSink = Sinks.many().unicast().onBackpressureBuffer();
                    Mono<Void> writeMono = outbound.sendObject(outboundSink.asFlux()).then();
                    Mono<Void> readMono = inbound.receiveObject()
                            .ofType(MqttMessage.class)
                            .concatMap(message -> {
                                if (message instanceof MqttConnectMessage) {
                                    outboundSink.tryEmitNext(MqttMessageBuilders.connAck()
                                            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                                            .sessionPresent(false)
                                            .build());
                                }
                                return Mono.empty();
                            })
                            .doFinally(signalType -> outboundSink.tryEmitComplete())
                            .then();
                    return Mono.when(writeMono, readMono);
                })
                .bindNow();

        try {
            MqttClient client = new MqttClient(
                    "tcp://127.0.0.1:" + server.port(),
                    "spike-client");
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setConnectionTimeout(5);
            client.connect(options);

            assertThat(client.isConnected()).isTrue();

            client.disconnect();
            client.close();
        } finally {
            server.disposeNow();
        }
    }
}
