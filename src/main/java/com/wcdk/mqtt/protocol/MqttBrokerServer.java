package com.wcdk.mqtt.protocol;

import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.core.MqttSessionRegistry;
import com.wcdk.mqtt.service.MqttBrokerService;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpServer;

/***
* MQTT 接入服务端，基于 Reactor Netty TCP 服务绑定端口并管理连接生命周期。
* @author wcdk
***/
@Component
public class MqttBrokerServer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MqttBrokerServer.class);

    private final MqttProperties properties;
    private final MqttSessionRegistry registry;
    private final MqttBrokerService brokerService;

    private volatile reactor.netty.DisposableServer server;
    private volatile boolean running;

    public MqttBrokerServer(MqttProperties properties,
                            MqttSessionRegistry registry,
                            MqttBrokerService brokerService) {
        this.properties = properties;
        this.registry = registry;
        this.brokerService = brokerService;
    }

    /***
    * 绑定 MQTT 监听端口，编解码器必须插到 Reactor Netty 桥接处理器之前。
    * @author wcdk
    ***/
    @Override
    public void start() {
        if (!properties.getBroker().isEnabled()) {
            log.info("MQTT Broker 已在配置中关闭，跳过端口绑定");
            return;
        }
        String nodeId = properties.getCluster().getNodeId();
        int maxPayloadSize = Math.max(1024, properties.getBroker().getMaxPayloadSize());
        server = TcpServer.create()
                .host(properties.getBroker().getHost())
                .port(properties.getBroker().getPort())
                .doOnChannelInit((connection, channel, remoteAddress) -> {
                    // 编解码器放在管道最前，抢在 Reactor Netty 入站桥接之前拦截字节流
                    channel.pipeline().addFirst("mqttDecoder", new MqttDecoder(maxPayloadSize));
                    channel.pipeline().addFirst("mqttEncoder", MqttEncoder.INSTANCE);
                })
                .handle((inbound, outbound) -> {
                    Connection[] connectionHolder = new Connection[1];
                    inbound.withConnection(connection -> connectionHolder[0] = connection);
                    MqttClientConnection clientConnection = new MqttClientConnection(
                            connectionHolder[0], registry, brokerService, properties, nodeId);
                    return clientConnection.run(inbound, outbound);
                })
                .bindNow();
        running = true;
        log.info("MQTT Broker 启动成功，监听地址：{}:{}",
                properties.getBroker().getHost(), server.port());
    }

    /***
    * 停止 MQTT 服务端并释放端口。
    * @author wcdk
    ***/
    @Override
    public void stop() {
        running = false;
        reactor.netty.DisposableServer current = server;
        if (current != null) {
            current.disposeNow();
            server = null;
            log.info("MQTT Broker 已停止");
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /***
    * 返回实际监听端口，配置为 0 时用于测试动态端口。
    * @author wcdk
    * @return 监听端口
    ***/
    public int port() {
        reactor.netty.DisposableServer current = server;
        return current == null ? properties.getBroker().getPort() : current.port();
    }
}
