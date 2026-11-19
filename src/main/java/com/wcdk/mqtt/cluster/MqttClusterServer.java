package com.wcdk.mqtt.cluster;

import com.wcdk.mqtt.config.MqttProperties;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.tcp.TcpServer;

/***
* 集群对等通道服务端，绑定集群端口并把入站连接交给管理器接管。
* @author wcdk
***/
class MqttClusterServer {

    private static final Logger log = LoggerFactory.getLogger(MqttClusterServer.class);

    private final MqttProperties properties;
    private final MqttClusterManager manager;

    private volatile reactor.netty.DisposableServer server;

    MqttClusterServer(MqttProperties properties, MqttClusterManager manager) {
        this.properties = properties;
        this.manager = manager;
    }

    /***
    * 绑定集群监听端口。
    * @author wcdk
    * @param sslContext TLS 上下文，为空表示明文
    ***/
    void bind(SslContext sslContext) {
        // 单帧上限按消息负载上限放宽，容纳 Base64 编码后的集群事件
        int maxFrameLength = Math.max(1024 * 1024, properties.getBroker().getMaxPayloadSize() * 2 + 64 * 1024);
        TcpServer tcpServer = TcpServer.create()
                .host(properties.getCluster().getBindHost())
                .port(properties.getCluster().getBindPort());
        if (sslContext != null) {
            tcpServer = tcpServer.secure(spec -> spec.sslContext(sslContext));
        }
        server = tcpServer
                .doOnChannelInit((connection, channel, remoteAddress) ->
                        // 换行分帧解码器放在管道最前，逐行产出集群事件
                        channel.pipeline().addFirst("lineDecoder", new LineBasedFrameDecoder(maxFrameLength)))
                .handle(manager::handleConnection)
                .bindNow();
        log.info("集群通道启动成功，监听地址：{}:{}，TLS：{}",
                properties.getCluster().getBindHost(), server.port(), sslContext != null);
    }

    /***
    * 返回集群通道实际监听端口。
    * @author wcdk
    * @return 监听端口
    ***/
    int port() {
        reactor.netty.DisposableServer current = server;
        return current == null ? properties.getCluster().getBindPort() : current.port();
    }

    /***
    * 关闭集群通道。
    * @author wcdk
    ***/
    void dispose() {
        reactor.netty.DisposableServer current = server;
        if (current != null) {
            current.disposeNow();
            server = null;
            log.info("集群通道已关闭");
        }
    }
}
