package com.wcdk.mqtt.core.json;


import java.math.BigDecimal;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * JSON 对象封装类，底层基于 Map 存储。
 * 继承 {@link JSON} 并实现 {@link Map} 接口，可直接作为 Map 使用，也可参与 Jackson 序列化。
 *
 * @auther WCDK
 * @date 2026/7/31
 * @version 1.0
 **/
public class JsonObject extends JSON implements Map<String, Object> {

    private final Map<String, Object> map;

    public JsonObject() {
        this.map = new LinkedHashMap<>();
    }

    public JsonObject(Map<String, Object> map) {
        this.map = map == null ? new LinkedHashMap<>() : new LinkedHashMap<>(map);
    }

    public JsonObject(String json) {
        this(JSON.parseObject(json).map);
    }

    /***
     * 设置键值，返回自身，支持链式调用。
     * 实现 {@link Map#put(Object, Object)} 的协变返回，直接作为 Map 使用时同样生效。
     ***/
    @Override
    public JsonObject put(String key, Object value) {
        map.put(key, value);
        return this;
    }

    @Override
    public Object get(Object key) {
        return map.get(key);
    }

    public String getString(String key) {
        return JSON.castToString(map.get(key));
    }

    public Integer getInteger(String key) {
        return JSON.castToInt(map.get(key));
    }

    public int getIntValue(String key) {
        Integer value = getInteger(key);
        return value == null ? 0 : value;
    }

    public Long getLong(String key) {
        return JSON.castToLong(map.get(key));
    }

    public long getLongValue(String key) {
        Long value = getLong(key);
        return value == null ? 0L : value;
    }

    public Double getDouble(String key) {
        return JSON.castToDouble(map.get(key));
    }

    public double getDoubleValue(String key) {
        Double value = getDouble(key);
        return value == null ? 0.0d : value;
    }

    public BigDecimal getBigDecimal(String key) {
        return JSON.castToBigDecimal(map.get(key));
    }

    public Boolean getBoolean(String key) {
        return JSON.castToBoolean(map.get(key));
    }

    public boolean getBooleanValue(String key) {
        Boolean value = getBoolean(key);
        return value != null && value;
    }

    /***
     * 获取嵌套 JsonObject，缺省返回 null
     ***/
    public JsonObject getJsonObject(String key) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        return JSON.toJSON(value);
    }

    /***
     * 获取嵌套 JsonObject
     ***/
    public JsonObject getJSONObject(String key) {
        return getJsonObject(key);
    }

    /***
     * 获取嵌套 JsonArray，缺省返回 null
     ***/
    public JsonArray getJsonArray(String key) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        return JSON.toJSONArray(value);
    }

    /***
     * 获取嵌套 JsonArray
     ***/
    public JsonArray getJSONArray(String key) {
        return getJsonArray(key);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public Object remove(Object key) {
        return map.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        map.putAll(m);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set<String> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<Object> values() {
        return map.values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return map.entrySet();
    }

    /***
     * 返回底层存储的 Map（直接引用）
     ***/
    public Map<String, Object> getMap() {
        return map;
    }

    /***
     * 返回底层存储 Map 的副本
     ***/
    public Map<String, Object> toMap() {
        return new LinkedHashMap<>(map);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Map)) {
            return false;
        }
        return map.equals(o);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    public String toJSONString() {
        return JSON.toJSONString(map);
    }

    @Override
    public String toString() {
        return toJSONString();
    }
}
