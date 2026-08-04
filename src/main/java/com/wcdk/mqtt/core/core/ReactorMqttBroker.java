package com.wcdk.mqtt.core.core;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import com.wcdk.mqtt.core.config.MqttBrokerProperties;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;
/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
public class ReactorMqttBroker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ReactorMqttBroker.class);

    private final MqttBrokerProperties properties;

    private final MqttBrokerSessionRegistry sessionRegistry;

    private final ObjectProvider<MqttBrokerClusterManager> clusterManagerProvider;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile EventLoopGroup bossGroup;

    private volatile EventLoopGroup workerGroup;

    private volatile Channel serverChannel;

    public ReactorMqttBroker(MqttBrokerProperties properties,
                             MqttBrokerSessionRegistry sessionRegistry,
                             ObjectProvider<MqttBrokerClusterManager> clusterManagerProvider) {
        this.properties = properties;
        this.sessionRegistry = sessionRegistry;
        this.clusterManagerProvider = clusterManagerProvider;
    }

    @Override
    public synchronized void start() {
        if (running.get()) {
            return;
        }
        int maxPayloadSize = Math.max(1, properties.getMaxPayloadSize());
        int bossThreads = Math.max(0, properties.getBossThreads());
        int workerThreads = Math.max(0, properties.getWorkerThreads());
        int backlog = Math.max(1, properties.getBacklog());
        EventLoopGroup newBossGroup = bossThreads > 0 ? new NioEventLoopGroup(bossThreads) : new NioEventLoopGroup(1);
        EventLoopGroup newWorkerGroup = workerThreads > 0 ? new NioEventLoopGroup(workerThreads) : new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(newBossGroup, newWorkerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, backlog)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            MqttBrokerClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
                            MqttBrokerClusterManager.NodeState targetNode = clusterManager == null
                                    ? null
                                    : clusterManager.selectLoadBalanceTarget().orElse(null);
                            if (targetNode != null && !clusterManager.isLocalNode(targetNode.getNodeId())) {
                                log.info("MQTT client connection proxy target: nodeId={}, host={}, port={}, client={}",
                                        targetNode.getNodeId(), targetNode.getMqttHost(), targetNode.getMqttPort(),
                                        channel.remoteAddress());
                                channel.pipeline().addLast("mqttTcpProxyHandler", new MqttTcpProxyHandler(
                                        targetNode.getNodeId(),
                                        targetNode.getMqttHost(),
                                        targetNode.getMqttPort()));
                                return;
                            }
                            channel.pipeline().addLast("mqttDecoder", new MqttDecoder(maxPayloadSize));
                            channel.pipeline().addLast("mqttEncoder", MqttEncoder.INSTANCE);
                            channel.pipeline().addLast("mqttBrokerHandler",
                                    new MqttBrokerChannelHandler(properties, sessionRegistry, clusterManager));
                        }
                    });
            serverChannel = bootstrap.bind(properties.getHost(), properties.getPort()).syncUninterruptibly().channel();
            bossGroup = newBossGroup;
            workerGroup = newWorkerGroup;
            running.set(true);
            log.info("MQTT 服务已启动，监听地址={}:{}, boss线程={}, worker线程={}, backlog={}",
                    properties.getHost(), properties.getPort(), bossThreads, workerThreads, backlog);
        } catch (RuntimeException ex) {
            newBossGroup.shutdownGracefully().awaitUninterruptibly(Duration.ofSeconds(5).toMillis());
            newWorkerGroup.shutdownGracefully().awaitUninterruptibly(Duration.ofSeconds(5).toMillis());
            throw ex;
        }
    }

    @Override
    public synchronized void stop() {
        Channel currentServerChannel = serverChannel;
        if (currentServerChannel != null) {
            currentServerChannel.close().awaitUninterruptibly(Duration.ofSeconds(5).toMillis());
            serverChannel = null;
        }
        EventLoopGroup currentBossGroup = bossGroup;
        if (currentBossGroup != null) {
            currentBossGroup.shutdownGracefully().awaitUninterruptibly(Duration.ofSeconds(5).toMillis());
            bossGroup = null;
        }
        EventLoopGroup currentWorkerGroup = workerGroup;
        if (currentWorkerGroup != null) {
            currentWorkerGroup.shutdownGracefully().awaitUninterruptibly(Duration.ofSeconds(5).toMillis());
            workerGroup = null;
        }
        running.set(false);
        log.info("MQTT 服务已停止");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE + 100;
    }

    public void publish(String topic, String payload) {
        publish(topic, payload == null ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8), 0, false);
    }

    public void publish(String topic, byte[] payload, int qos, boolean retained) {
        if (!MqttTopicFilter.isValidTopicName(topic)) {
            throw new IllegalArgumentException("MQTT 主题不能为空，且不能包含通配符");
        }
        publish(new MqttBrokerMessage(topic, payload, MqttQoS.valueOf(qos), retained));
    }

    public void publish(MqttBrokerMessage message) {
        MqttBrokerClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        if (clusterManager != null && clusterManager.isClusterEnabled()) {
            clusterManager.publish(message);
            return;
        }
        sessionRegistry.publish(message);
    }
}