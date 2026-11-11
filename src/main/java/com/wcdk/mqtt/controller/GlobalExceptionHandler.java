package com.wcdk.mqtt.controller;

import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
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
}
