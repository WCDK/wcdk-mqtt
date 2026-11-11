package com.wcdk.mqtt.controller;

/***
* 统一管理接口响应包装。
* @author wcdk
* @param code 结果码，0 表示成功
* @param message 提示信息
* @param data 业务数据
***/
public record ApiResponse<T>(int code, String message, T data) {

    /***
    * 构造成功响应。
    * @author wcdk
    * @param data 业务数据
    * @param <T> 数据类型
    * @return 成功响应
    ***/
    public static <T> ApiResponse<T> ok(T data) {
        return new ApiResponse<>(0, "操作成功", data);
    }

    /***
    * 构造成功响应（无数据）。
    * @author wcdk
    * @return 成功响应
    ***/
    public static ApiResponse<Void> ok() {
        return new ApiResponse<>(0, "操作成功", null);
    }

    /***
    * 构造失败响应。
    * @author wcdk
    * @param code 结果码
    * @param message 提示信息
    * @param <T> 数据类型
    * @return 失败响应
    ***/
    public static <T> ApiResponse<T> fail(int code, String message) {
        return new ApiResponse<>(code, message, null);
    }
}
