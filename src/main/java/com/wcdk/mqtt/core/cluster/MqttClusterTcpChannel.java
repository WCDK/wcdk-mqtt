package com.wcdk.mqtt.core.cluster;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.wcdk.mqtt.core.config.MqttBrokerProperties;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.util.StringUtils;

/**
 * @auther WCDK
 * @date 2026/7/29
 * @version 1.0
 **/
public class MqttClusterTcpChannel {

    private static final Logger log = LoggerFactory.getLogger(MqttClusterTcpChannel.class);

    /** 认证消息前缀 */
    private static final String AUTH_PREFIX = "AUTH:";
    /** 认证成功响应 */
    private static final String AUTH_OK = "AUTH_OK";
    /** 认证失败响应 */
    private static final String AUTH_FAIL = "AUTH_FAIL";
    /** 注册消息前缀 */
    private static final String REGISTER_PREFIX = "REGISTER:";
    /** 心跳消息前缀 */
    private static final String HEARTBEAT_PREFIX = "HEARTBEAT:";
    /** 配置同步消息前缀 */
    private static final String CONFIG_SYNC_PREFIX = "CONFIG_SYNC:";
    /** 配置请求消息 */
    private static final String CONFIG_REQUEST = "CONFIG_REQUEST";

    /** 集群连接配置属性 */
    private final MqttBrokerProperties.Cluster properties;
    /** 消息消费者，用于处理接收到的集群消息 */
    private final Consumer<String> messageConsumer;
    /** 配置同步服务提供者 */
    private final ObjectProvider<MqttClusterConfigSyncService> configSyncServiceProvider;
    /** 已连接的通道集合 */
    private final Set<io.netty.channel.Channel> channels = ConcurrentHashMap.newKeySet();
    /** 已认证的节点ID集合 */
    private final Set<String> authenticatedNodes = ConcurrentHashMap.newKeySet();
    /** 通道对应的已注册节点ID */
    private final Map<io.netty.channel.ChannelId, String> nodeIdsByChannel = new ConcurrentHashMap<>();
    /** 客户端通道对应的连接目标 */
    private final Map<io.netty.channel.ChannelId, ConnectionTarget> targetsByChannel = new ConcurrentHashMap<>();
    /** 已安排重连的目标，避免重复排队 */
    private final Set<ConnectionTarget> reconnectingTargets = ConcurrentHashMap.newKeySet();

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private EventLoopGroup clientGroup;
    private io.netty.channel.Channel serverChannel;

    public MqttClusterTcpChannel(MqttBrokerProperties.Cluster properties, 
                                  Consumer<String> messageConsumer,
                                  ObjectProvider<MqttClusterConfigSyncService> configSyncServiceProvider) {
        this.properties = properties;
        this.messageConsumer = messageConsumer;
        this.configSyncServiceProvider = configSyncServiceProvider;
    }

    /**
     * 启动集群TCP通道
     * 初始化Netty服务端和客户端，根据节点角色连接其他节点
     */
    public synchronized void start() {
        // 初始化Netty事件循环组
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        clientGroup = new NioEventLoopGroup();
        // 启动TCP服务端
        startServer();
        
        // 根据节点角色决定连接策略
        if (properties.getRole() == MqttBrokerProperties.NodeRole.SLAVE) {
            // 从节点连接主节点
            connectToMaster();
        } else {
            // 主节点连接所有对等节点
            connectPeers();
        }
    }

    /**
     * 发布消息到所有已连接的集群节点
     * 将消息广播到所有已认证的集群通道
     *
     * @param payload 消息内容，会自动添加换行符作为消息分隔符
     */
    public void publish(String payload) {
        if (!StringUtils.hasText(payload)) {
            return;
        }
        // 消息内容添加换行符作为分隔符
        byte[] bytes = (payload + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        // 广播到所有活跃的通道
        for (io.netty.channel.Channel channel : channels) {
            if (channel.isActive()) {
                channel.writeAndFlush(Unpooled.wrappedBuffer(bytes));
            }
        }
    }

    public synchronized void stop() {
        for (io.netty.channel.Channel channel : channels) {
            channel.close();
        }
        channels.clear();
        authenticatedNodes.clear();
        nodeIdsByChannel.clear();
        targetsByChannel.clear();
        reconnectingTargets.clear();
        if (serverChannel != null) {
            serverChannel.close();
            serverChannel = null;
        }
        shutdown(clientGroup);
        shutdown(workerGroup);
        shutdown(bossGroup);
    }

    private void startServer() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(initializer());
        ChannelFuture future = bootstrap.bind(new InetSocketAddress(properties.getBindHost(), properties.getBindPort())).syncUninterruptibly();
        serverChannel = future.channel();
        log.info("MQTT Broker TCP集群通道已启动，角色={}, 绑定={}:{}", 
                properties.getRole(), properties.getBindHost(), properties.getBindPort());
    }

    private void connectToMaster() {
        List<MqttBrokerProperties.MasterNode> masters = properties.getMasters();
        if (masters != null && !masters.isEmpty()) {
            for (MqttBrokerProperties.MasterNode master : masters) {
                connectToMaster(master);
            }
            return;
        }

        connectToMaster(properties.getMaster());
    }

    private void connectToMaster(MqttBrokerProperties.MasterNode master) {
        if (master == null || !StringUtils.hasText(master.getHost())) {
            log.warn("MQTT Broker master node configuration is empty");
            return;
        }
        connectToNode(new ConnectionTarget(master.getHost(), master.getPort(), master.getUsername(), master.getPassword()));
    }

    private void connectPeers() {
        List<String> peers = properties.getPeers() == null ? List.of() : properties.getPeers();
        for (String peer : peers) {
            connectPeer(peer);
        }
    }

    private void connectPeer(String peer) {
        if (!StringUtils.hasText(peer)) {
            return;
        }
        String[] hostAndPort = peer.trim().split(":", 2);
        if (hostAndPort.length != 2) {
            log.warn("忽略无效的MQTT集群对等节点，peer={}", peer);
            return;
        }
        int port;
        try {
            port = Integer.parseInt(hostAndPort[1]);
        } catch (NumberFormatException ex) {
            log.warn("忽略无效的MQTT集群对等节点端口，peer={}", peer);
            return;
        }
        connectToNode(new ConnectionTarget(hostAndPort[0], port, properties.getUsername(), properties.getPassword()));
    }

    private void connectToNode(ConnectionTarget target) {
        if (target == null || !StringUtils.hasText(target.host()) || target.port() <= 0) {
            return;
        }
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(clientGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(clientInitializer(target));
        bootstrap.connect(target.host(), target.port()).addListener(future -> {
            if (future.isSuccess()) {
                io.netty.channel.Channel channel = ((ChannelFuture) future).channel();
                targetsByChannel.put(channel.id(), target);
                log.info("MQTT Broker已连接到集群节点 {}:{}", target.host(), target.port());
            } else {
                log.warn("MQTT Broker连接集群节点失败，将重试 {}:{}", target.host(), target.port());
                scheduleReconnect(target);
            }
        });
    }

    private ChannelInitializer<SocketChannel> initializer() {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(SocketChannel channel) {
                channel.pipeline()
                        .addLast(new LineBasedFrameDecoder(1024 * 1024))
                        .addLast(new StringDecoder(java.nio.charset.StandardCharsets.UTF_8))
                        .addLast(new StringEncoder(java.nio.charset.StandardCharsets.UTF_8))
                        .addLast(new ClusterMessageHandler());
            }
        };
    }

    private ChannelInitializer<SocketChannel> clientInitializer(ConnectionTarget target) {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(SocketChannel channel) {
                channel.pipeline()
                        .addLast(new LineBasedFrameDecoder(1024 * 1024))
                        .addLast(new StringDecoder(java.nio.charset.StandardCharsets.UTF_8))
                        .addLast(new StringEncoder(java.nio.charset.StandardCharsets.UTF_8))
                        .addLast(new ClientAuthHandler(target));
            }
        };
    }

    private void scheduleReconnect(ConnectionTarget target) {
        EventLoopGroup group = clientGroup;
        if (group != null && !group.isShuttingDown() && reconnectingTargets.add(target)) {
            long delayMillis = Math.max(1000, Math.min(30000, properties.getHeartbeatIntervalMillis()));
            group.schedule(() -> {
                reconnectingTargets.remove(target);
                connectToNode(target);
            }, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    private void shutdown(EventLoopGroup group) {
        if (group != null) {
            group.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
    }

    private void relayFromMaster(io.netty.channel.Channel sourceChannel, String message) {
        if (properties.getRole() != MqttBrokerProperties.NodeRole.MASTER || !StringUtils.hasText(message)) {
            return;
        }
        byte[] bytes = (message + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        for (io.netty.channel.Channel channel : channels) {
            if (channel != sourceChannel && channel.isActive()) {
                channel.writeAndFlush(Unpooled.wrappedBuffer(bytes));
            }
        }
    }

    /**
     * 主节点消息处理器 - 处理子节点连接和认证
     */
    private class ClusterMessageHandler extends SimpleChannelInboundHandler<String> {

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            // 新连接等待认证消息
            log.debug("来自{}的新集群连接", ctx.channel().remoteAddress());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            channels.remove(ctx.channel());
            String nodeId = findNodeIdByChannel(ctx.channel());
            nodeIdsByChannel.remove(ctx.channel().id());
            if (nodeId != null) {
                authenticatedNodes.remove(nodeId);
                log.info("集群节点已断开连接: {}", nodeId);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("MQTT Broker集群服务端通道异常，远程地址={}", ctx.channel().remoteAddress(), cause);
            ctx.close();
        }

        /**
         * 处理接收到的集群消息
         * 根据消息类型分发处理：认证、注册、心跳或业务消息
         *
         * @param ctx 通道处理器上下文
         * @param message 接收到的消息内容
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String message) {
            if (message == null) {
                return;
            }
            
            String trimmedMessage = message.trim();
            
            // 处理认证消息：来自从节点的认证请求
            if (trimmedMessage.startsWith(AUTH_PREFIX)) {
                handleAuthMessage(ctx, trimmedMessage);
                return;
            }
            
            // 处理注册消息：从节点认证成功后发送注册信息
            if (trimmedMessage.startsWith(REGISTER_PREFIX)) {
                handleRegisterMessage(ctx, trimmedMessage);
                return;
            }
            
            // 处理心跳消息：节点状态同步
            if (trimmedMessage.startsWith(HEARTBEAT_PREFIX)) {
                handleHeartbeatMessage(ctx, trimmedMessage);
                return;
            }
            
            // 处理业务消息：仅转发已认证节点的消息
            String nodeId = findNodeIdByChannel(ctx.channel());
            if (nodeId != null && authenticatedNodes.contains(nodeId)) {
                relayFromMaster(ctx.channel(), trimmedMessage);
                messageConsumer.accept(trimmedMessage);
            }
        }

        /**
         * 处理认证消息
         * 验证从节点发送的用户名和密码
         *
         * @param ctx 通道处理器上下文
         * @param message 认证消息，格式：AUTH:username:password
         */
        private void handleAuthMessage(ChannelHandlerContext ctx, String message) {
            // 解析用户名和密码
            String[] parts = message.substring(AUTH_PREFIX.length()).split(":", 2);
            if (parts.length != 2) {
                sendResponse(ctx, AUTH_FAIL);
                return;
            }
            
            String username = parts[0];
            String password = parts[1];
            
            // 验证用户名密码
            if (authenticateUser(username, password)) {
                sendResponse(ctx, AUTH_OK);
                log.info("集群节点认证成功，来源: {}", ctx.channel().remoteAddress());
            } else {
                sendResponse(ctx, AUTH_FAIL);
                log.warn("集群节点认证失败，来源: {}", ctx.channel().remoteAddress());
                ctx.channel().close();
            }
        }

        /**
         * 处理注册消息
         * 从节点认证成功后发送注册信息，主节点记录该节点并发送配置同步
         *
         * @param ctx 通道处理器上下文
         * @param message 注册消息，格式：REGISTER:nodeId
         */
        private void handleRegisterMessage(ChannelHandlerContext ctx, String message) {
            // 解析节点ID
            String nodeId = message.substring(REGISTER_PREFIX.length()).trim();
            if (!StringUtils.hasText(nodeId)) {
                return;
            }
            
            // 记录已连接通道和已认证节点
            channels.add(ctx.channel());
            authenticatedNodes.add(nodeId);
            nodeIdsByChannel.put(ctx.channel().id(), nodeId);
            log.info("集群节点已注册: {}, 远程地址: {}", nodeId, ctx.channel().remoteAddress());
            
            // 向新注册的从节点发送配置同步消息
            sendConfigSync(ctx);
        }

        /**
         * 发送配置同步消息到从节点
         * 将主节点的配置序列化后发送给新注册的从节点
         *
         * @param ctx 通道处理器上下文
         */
        private void sendConfigSync(ChannelHandlerContext ctx) {
            MqttClusterConfigSyncService configSyncService = configSyncServiceProvider.getIfAvailable();
            if (configSyncService != null) {
                String configMessage = configSyncService.buildConfigSyncMessage();
                ctx.channel().writeAndFlush(Unpooled.wrappedBuffer((configMessage + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                log.info("已向从节点发送配置同步消息");
            }
        }

        private void handleHeartbeatMessage(ChannelHandlerContext ctx, String message) {
            String nodeId = findNodeIdByChannel(ctx.channel());
            if (nodeId != null) {
                relayFromMaster(ctx.channel(), message);
                messageConsumer.accept(message);
            }
        }

        private boolean authenticateUser(String username, String password) {
            if (!StringUtils.hasText(properties.getUsername()) || !StringUtils.hasText(properties.getPassword())) {
                return true;
            }
            return properties.getUsername().equals(username) && properties.getPassword().equals(password);
        }

        private void sendResponse(ChannelHandlerContext ctx, String response) {
            ctx.channel().writeAndFlush(Unpooled.wrappedBuffer((response + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }

        private String findNodeIdByChannel(io.netty.channel.Channel channel) {
            return channel == null ? null : nodeIdsByChannel.get(channel.id());
        }
    }

    /**
     * 子节点消息处理器 - 处理主节点响应
     */
    private class ClientAuthHandler extends SimpleChannelInboundHandler<String> {

        private final ConnectionTarget target;
        private boolean authenticated = false;

        public ClientAuthHandler(ConnectionTarget target) {
            this.target = target;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            // 连接成功后发送认证消息
            String authMessage = AUTH_PREFIX + (target.username() != null ? target.username() : "") + ":" + (target.password() != null ? target.password() : "");
            ctx.channel().writeAndFlush(Unpooled.wrappedBuffer((authMessage + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            log.debug("已向主节点发送认证消息");
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            channels.remove(ctx.channel());
            targetsByChannel.remove(ctx.channel().id());
            authenticated = false;
            scheduleReconnect(target);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("MQTT Broker集群客户端通道异常，远程地址={}", ctx.channel().remoteAddress(), cause);
            ctx.close();
        }

        /**
         * 处理来自主节点的响应消息
         * 处理认证响应、配置同步和其他业务消息
         *
         * @param ctx 通道处理器上下文
         * @param message 接收到的消息内容
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String message) {
            if (message == null) {
                return;
            }
            
            String trimmedMessage = message.trim();
            
            // 处理认证成功响应
            if (AUTH_OK.equals(trimmedMessage)) {
                authenticated = true;
                channels.add(ctx.channel());
                // 发送注册消息到主节点
                String registerMessage = REGISTER_PREFIX + properties.getNodeId();
                ctx.channel().writeAndFlush(Unpooled.wrappedBuffer((registerMessage + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                log.info("已通过主节点认证，注册为: {}", properties.getNodeId());
                return;
            }
            
            // 处理认证失败响应
            if (AUTH_FAIL.equals(trimmedMessage)) {
                log.error("主节点认证失败");
                ctx.channel().close();
                return;
            }
            
            // 处理配置同步消息：应用主节点的配置
            if (trimmedMessage.startsWith(CONFIG_SYNC_PREFIX)) {
                handleConfigSync(trimmedMessage);
                return;
            }
            
            // 处理配置请求消息：从节点不处理配置请求
            if (CONFIG_REQUEST.equals(trimmedMessage)) {
                return;
            }
            
            // 转发其他业务消息给消息消费者
            if (authenticated) {
                messageConsumer.accept(trimmedMessage);
            }
        }

        /**
         * 处理配置同步消息
         * 解析并应用来自主节点的配置
         *
         * @param message 配置同步消息
         */
        private void handleConfigSync(String message) {
            MqttClusterConfigSyncService configSyncService = configSyncServiceProvider.getIfAvailable();
            if (configSyncService != null) {
                // 解析配置同步消息
                MqttClusterConfigSyncService.ClusterConfig config = configSyncService.parseConfigSyncMessage(message);
                if (config != null) {
                    // 应用同步的配置到本地
                    configSyncService.applySyncedConfig(config);
                    log.info("已应用来自主节点的同步配置");
                }
            }
        }
    }

    private record ConnectionTarget(String host, int port, String username, String password) {

        private ConnectionTarget {
            host = host == null ? null : host.trim();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ConnectionTarget target)) {
                return false;
            }
            return port == target.port && Objects.equals(host, target.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port);
        }
    }

}