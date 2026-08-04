package com.wcdk.mqtt.core.json;

import java.math.BigDecimal;
import java.util.*;

/**
 * JSON 数组封装类，底层基于 List 存储。
 * 继承 {@link JSON} 并实现 {@link List} 接口，可直接作为 List 使用，也可参与 Jackson 序列化。
 *
 * @auther WCDK
 * @date 2026/7/31
 * @version 1.0
 **/
public class JsonArray extends JSON implements List<Object> {

    private final List<Object> list;

    public JsonArray() {
        this.list = new ArrayList<>();
    }

    public JsonArray(List<Object> list) {
        this.list = list == null ? new ArrayList<>() : new ArrayList<>(list);
    }

    public JsonArray(String json) {
        this(JSON.parseArray(json).list);
    }

    public String getString(int index) {
        return JSON.castToString(list.get(index));
    }

    public Integer getInteger(int index) {
        return JSON.castToInt(list.get(index));
    }

    public int getIntValue(int index) {
        Integer value = getInteger(index);
        return value == null ? 0 : value;
    }

    public Long getLong(int index) {
        return JSON.castToLong(list.get(index));
    }

    public long getLongValue(int index) {
        Long value = getLong(index);
        return value == null ? 0L : value;
    }

    public Double getDouble(int index) {
        return JSON.castToDouble(list.get(index));
    }

    public double getDoubleValue(int index) {
        Double value = getDouble(index);
        return value == null ? 0.0d : value;
    }

    public BigDecimal getBigDecimal(int index) {
        return JSON.castToBigDecimal(list.get(index));
    }

    public Boolean getBoolean(int index) {
        return JSON.castToBoolean(list.get(index));
    }

    public boolean getBooleanValue(int index) {
        Boolean value = getBoolean(index);
        return value != null && value;
    }

    /***
     * 获取嵌套 JsonObject，缺省返回 null
     ***/
    public JsonObject getJsonObject(int index) {
        Object value = list.get(index);
        if (value == null) {
            return null;
        }
        return JSON.toJSON(value);
    }

    /***
     * 获取嵌套 JsonObject
     ***/
    public JsonObject getJSONObject(int index) {
        return getJsonObject(index);
    }

    /***
     * 获取嵌套 JsonArray，缺省返回 null
     ***/
    public JsonArray getJsonArray(int index) {
        Object value = list.get(index);
        if (value == null) {
            return null;
        }
        return JSON.toJSONArray(value);
    }

    /***
     * 获取嵌套 JsonArray
     ***/
    public JsonArray getJSONArray(int index) {
        return getJsonArray(index);
    }

    /***
     * 返回底层存储的 List（直接引用）
     ***/
    public List<Object> getList() {
        return list;
    }

    /***
     * 返回底层存储 List 的副本
     ***/
    public List<Object> toList() {
        return new ArrayList<>(list);
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return list.contains(o);
    }

    @Override
    public Iterator<Object> iterator() {
        return list.iterator();
    }

    @Override
    public Object[] toArray() {
        return list.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return list.toArray(a);
    }

    @Override
    public boolean add(Object o) {
        return list.add(o);
    }

    @Override
    public boolean remove(Object o) {
        return list.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return list.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<?> c) {
        return list.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<?> c) {
        return list.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return list.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return list.retainAll(c);
    }

    @Override
    public void clear() {
        list.clear();
    }

    @Override
    public Object get(int index) {
        return list.get(index);
    }

    @Override
    public Object set(int index, Object element) {
        return list.set(index, element);
    }

    @Override
    public void add(int index, Object element) {
        list.add(index, element);
    }

    @Override
    public Object remove(int index) {
        return list.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return list.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return list.lastIndexOf(o);
    }

    @Override
    public ListIterator<Object> listIterator() {
        return list.listIterator();
    }

    @Override
    public ListIterator<Object> listIterator(int index) {
        return list.listIterator(index);
    }

    @Override
    public List<Object> subList(int fromIndex, int toIndex) {
        return list.subList(fromIndex, toIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof List)) {
            return false;
        }
        return list.equals(o);
    }

    @Override
    public int hashCode() {
        return list.hashCode();
    }

    public String toJSONString() {
        return JSON.toJSONString(list);
    }

    @Override
    public String toString() {
        return toJSONString();
    }
}
