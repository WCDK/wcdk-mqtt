package com.wcdk.mqtt.controller;

import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.reactive.resource.NoResourceFoundException;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

/***
* 管理接口统一异常处理，全部返回中文提示。
* @author wcdk
***/
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /***
    * 资源不存在异常映射为 404。
    * @author wcdk
    * @param ex 异常
    * @return 404 响应
    ***/
    @ExceptionHandler(NoSuchElementException.class)
    public Mono<ResponseEntity<ApiResponse<Void>>> handleNotFound(NoSuchElementException ex) {
        return Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(ApiResponse.fail(404, ex.getMessage() == null ? "资源不存在" : ex.getMessage())));
    }

    /***
    * 静态资源或接口路径不存在映射为 404，避免被包装成 500 误导排查。
    * @author wcdk
    * @param ex 异常
    * @return 404 响应
    ***/
    @ExceptionHandler(NoResourceFoundException.class)
    public Mono<ResponseEntity<ApiResponse<Void>>> handleNoResource(NoResourceFoundException ex) {
        String resource = resourcePath(ex);
        return Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(ApiResponse.fail(404, resource.isEmpty() ? "请求的资源不存在" : "请求的资源不存在：" + resource)));
    }

    /***
    * 框架状态码异常保持原状态码返回，不再统一降级为 500。
    * @author wcdk
    * @param ex 异常
    * @return 原状态码响应
    ***/
    @ExceptionHandler(ResponseStatusException.class)
    public Mono<ResponseEntity<ApiResponse<Void>>> handleResponseStatus(ResponseStatusException ex) {
        HttpStatusCode statusCode = ex.getStatusCode();
        String reason = StringUtils.hasText(ex.getReason()) ? ex.getReason() : "请求处理失败";
        return Mono.just(ResponseEntity.status(statusCode)
                .body(ApiResponse.fail(statusCode.value(), reason)));
    }

    /***
    * 参数异常映射为 400。
    * @author wcdk
    * @param ex 异常
    * @return 400 响应
    ***/
    @ExceptionHandler(IllegalArgumentException.class)
    public Mono<ResponseEntity<ApiResponse<Void>>> handleBadRequest(IllegalArgumentException ex) {
        return Mono.just(ResponseEntity.badRequest()
                .body(ApiResponse.fail(400, ex.getMessage() == null ? "请求参数不合法" : ex.getMessage())));
    }

    /***
    * 其余运行时异常映射为 500，记录日志便于排查。
    * @author wcdk
    * @param ex 异常
    * @return 500 响应
    ***/
    @ExceptionHandler(RuntimeException.class)
    public Mono<ResponseEntity<ApiResponse<Void>>> handleRuntime(RuntimeException ex) {
        log.error("管理接口处理异常：{}", ex.getMessage(), ex);
        return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.fail(500, "服务内部错误：" + ex.getMessage())));
    }

    /***
    * 从 NoResourceFoundException 的提示中解析缺失的资源路径。
    * @author wcdk
    * @param ex 异常
    * @return 资源路径，无法解析时返回空串
    ***/
    private String resourcePath(NoResourceFoundException ex) {
        String message = StringUtils.hasText(ex.getReason()) ? ex.getReason() : String.valueOf(ex.getMessage());
        String prefix = "No static resource ";
        String path = message.startsWith(prefix) ? message.substring(prefix.length()) : message;
        return path.endsWith(".") ? path.substring(0, path.length() - 1) : path;
    }
}
