# 多阶段构建：Maven 离线/在线打包 + JRE 运行时
FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /build

# 先拷贝 pom 以便利用 Docker 层缓存依赖
COPY pom.xml .
RUN mvn -B -q dependency:go-offline || true

COPY src ./src
RUN mvn -B -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app

# 中文日志统一 UTF-8
ENV LANG=C.UTF-8 \
    TZ=Asia/Shanghai \
    JAVA_OPTS=""

COPY --from=builder /build/target/*.jar /app/app.jar

# 38082 HTTP 管理接口与网页控制台
EXPOSE 38082
# 1883 MQTT 接入端口
EXPOSE 1883
# 28883 集群对等通道端口
EXPOSE 28883

HEALTHCHECK --interval=15s --timeout=3s --start-period=40s --retries=3 \
  CMD timeout 3 bash -c 'exec 3<>/dev/tcp/127.0.0.1/1883' || exit 1

ENTRYPOINT ["sh", "-c", "exec java ${JAVA_OPTS} -jar /app/app.jar"]
