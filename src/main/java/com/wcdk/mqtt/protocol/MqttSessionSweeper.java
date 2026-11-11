package com.wcdk.mqtt.protocol;

import java.util.concurrent.TimeUnit;

import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.core.MqttSession;
import com.wcdk.mqtt.core.MqttSessionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/***
* 会话定时巡检器：断开 Keep Alive 超时连接并重投未确认的出站消息。
* @author wcdk
***/
@Component
public class MqttSessionSweeper {

    private static final Logger log = LoggerFactory.getLogger(MqttSessionSweeper.class);

    private final MqttSessionRegistry registry;
    private final MqttProperties properties;

    public MqttSessionSweeper(MqttSessionRegistry registry, MqttProperties properties) {
        this.registry = registry;
        this.properties = properties;
    }

    /***
    * 周期巡检，按配置的扫描间隔执行。
    * @author wcdk
    ***/
    @Scheduled(fixedDelayString = "${wcdk.mqtt.broker.sweep-interval-seconds:5}",
            timeUnit = TimeUnit.SECONDS)
    public void sweep() {
        try {
            // Keep Alive 超时的连接按规范断开，异常断开会触发遗嘱发布
            for (MqttSession session : registry.sweepIdleSessions()) {
                log.warn("客户端 Keep Alive 超时，断开连接，客户端ID：{}，KeepAlive：{}秒",
                        session.clientId(), session.keepAliveSeconds());
                session.closeConnection();
            }
            // 出站未确认消息重投，超过最大次数后放弃
            for (MqttSession session : registry.sessions()) {
                session.retransmitExpired(
                        properties.getBroker().getRetransmitTimeoutMillis(),
                        properties.getBroker().getMaxRetransmitAttempts());
            }
            // MQTT5 会话超过过期时间后从注册表移除，并通知集群同步清理
            for (MqttSession session : registry.sweepExpiredSessions()) {
                log.info("MQTT5 会话已过期并移除，客户端ID：{}，会话过期：{}秒",
                        session.clientId(), session.sessionExpirySeconds());
            }
        } catch (RuntimeException ex) {
            log.warn("会话巡检执行异常，异常信息：{}", ex.getMessage());
        }
    }
}
