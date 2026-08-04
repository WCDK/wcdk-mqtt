package com.wcdk.mqtt.core.core;

import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MQTT TCP 透传代理处理器。
 * @auther WCDK
 * @date 2026/8/3
 * @version 1.0
 **/
public class MqttTcpProxyHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(MqttTcpProxyHandler.class);

    private final String targetNodeId;
    private final String targetHost;
    private final int targetPort;

    private volatile Channel outboundChannel;

    public MqttTcpProxyHandler(String targetNodeId, String targetHost, int targetPort) {
        this.targetNodeId = targetNodeId;
        this.targetHost = targetHost;
        this.targetPort = targetPort;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        Channel inboundChannel = ctx.channel();
        inboundChannel.config().setAutoRead(false);
        Bootstrap bootstrap = new Bootstrap()
                .group(inboundChannel.eventLoop())
                .channel(NioSocketChannel.class)
                .option(ChannelOption.AUTO_READ, false)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        channel.pipeline().addLast(new BackendHandler(inboundChannel));
                    }
                });
        bootstrap.connect(new InetSocketAddress(targetHost, targetPort)).addListener(future -> {
            if (future.isSuccess()) {
                Channel connectedChannel = ((ChannelFuture) future).channel();
                outboundChannel = connectedChannel;
                log.info("MQTT proxy connection established: nodeId={}, host={}, port={}, inbound={}, outbound={}",
                        targetNodeId, targetHost, targetPort, inboundChannel.localAddress(), connectedChannel.remoteAddress());
                inboundChannel.read();
                log.info("MQTT连接已代理到集群节点，目标节点ID={}, 目标={}:{}",
                        targetNodeId, targetHost, targetPort);
            } else {
                log.warn("MQTT proxy connection failed: nodeId={}, host={}, port={}, cause={}",
                        targetNodeId, targetHost, targetPort, future.cause());
                closeOnFlush(inboundChannel);
            }
        });
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Channel outbound = outboundChannel;
        if (outbound == null || !outbound.isActive()) {
            closeOnFlush(ctx.channel());
            return;
        }
        outbound.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                ctx.channel().read();
            } else {
                closeOnFlush(future.channel());
            }
        });
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        closeOnFlush(outboundChannel);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("MQTT代理通道异常，目标节点ID={}", targetNodeId, cause);
        closeOnFlush(ctx.channel());
    }

    private static void closeOnFlush(Channel channel) {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static class BackendHandler extends ChannelInboundHandlerAdapter {

        private final Channel inboundChannel;

        private BackendHandler(Channel inboundChannel) {
            this.inboundChannel = inboundChannel;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            ctx.read();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            inboundChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    ctx.channel().read();
                } else {
                    closeOnFlush(future.channel());
                }
            });
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            closeOnFlush(inboundChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            closeOnFlush(ctx.channel());
        }
    }
}