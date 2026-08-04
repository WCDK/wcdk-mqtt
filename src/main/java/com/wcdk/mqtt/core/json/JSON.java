package com.wcdk.mqtt.core.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Jackson JSON 工具基类，统一提供 JSON 序列化与反序列化能力。
 * JsonObject 与 JsonArray 均继承本类，可共享全部静态工具方法。
 *
 * @auther WCDK
 * @date 2026/7/31
 * @version 1.0
 **/
public class JSON {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.registerModule(new JavaTimeModule());
        MAPPER.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    protected JSON() {
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    /***
     * 序列化为 JSON 字符串
     ***/
    public static String toJSONString(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("序列化 JSON 失败", e);
        }
    }

    /***
     * 序列化为 JSON 字节数组
     ***/
    public static byte[] toJSONBytes(Object obj) {
        try {
            return MAPPER.writeValueAsBytes(obj);
        } catch (Exception e) {
            throw new RuntimeException("序列化 JSON 失败", e);
        }
    }

    /***
     * 序列化为 JsonObject
     ***/
    public static JsonObject toJSON(Object obj) {
        if (obj == null) {
            return new JsonObject();
        }
        if (obj instanceof JsonObject) {
            return (JsonObject) obj;
        }
        if (obj instanceof Map) {
            return new JsonObject((Map<String, Object>) obj);
        }
        Map<String, Object> map = MAPPER.convertValue(obj, new TypeReference<Map<String, Object>>() {
        });
        return new JsonObject(map);
    }

    /***
     * 序列化为 JsonArray
     ***/
    public static JsonArray toJSONArray(Object obj) {
        if (obj == null) {
            return new JsonArray();
        }
        if (obj instanceof JsonArray) {
            return (JsonArray) obj;
        }
        if (obj instanceof List) {
            return new JsonArray((List<Object>) obj);
        }
        List<Object> list = MAPPER.convertValue(obj, new TypeReference<List<Object>>() {
        });
        return new JsonArray(list);
    }

    /***
     * 反序列化为 JsonObject
     ***/
    public static JsonObject parseObject(String json) {
        if (json == null || json.isBlank()) {
            return new JsonObject();
        }
        try {
            Map<String, Object> map = MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {
            });
            return new JsonObject(map);
        } catch (Exception e) {
            throw new RuntimeException("解析 JSON 失败", e);
        }
    }

    /***
     * 反序列化为 JsonObject（parseObject 的快捷入口）
     ***/
    public static JsonObject parse(String json) {
        return parseObject(json);
    }

    /***
     * 反序列化为指定类型对象
     ***/
    public static <T> T parseObject(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException("解析 JSON 失败", e);
        }
    }

    /***
     * 反序列化为指定泛型类型对象
     ***/
    public static <T> T parseObject(String json, TypeReference<T> typeReference) {
        try {
            return MAPPER.readValue(json, typeReference);
        } catch (Exception e) {
            throw new RuntimeException("解析 JSON 失败", e);
        }
    }

    /***
     * 反序列化为 JsonArray
     ***/
    public static JsonArray parseArray(String json) {
        if (json == null || json.isBlank()) {
            return new JsonArray();
        }
        try {
            List<Object> list = MAPPER.readValue(json, new TypeReference<List<Object>>() {
            });
            return new JsonArray(list);
        } catch (Exception e) {
            throw new RuntimeException("解析 JSON 失败", e);
        }
    }

    /***
     * 反序列化为指定类型的 List
     ***/
    public static <T> List<T> parseArray(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, MAPPER.getTypeFactory().constructCollectionType(List.class, clazz));
        } catch (Exception e) {
            throw new RuntimeException("解析 JSON 失败", e);
        }
    }

    /***
     * 判断字符串是否为合法 JSON
     ***/
    public static boolean isValidJson(String json) {
        if (json == null || json.isBlank()) {
            return false;
        }
        try {
            MAPPER.readTree(json);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /***
     * 将任意值转换为字符串，复合结构按 JSON 序列化输出
     ***/
    protected static String castToString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof Map || value instanceof List || value instanceof JsonObject || value instanceof JsonArray
                || value.getClass().isArray()) {
            return toJSONString(value);
        }
        return String.valueOf(value);
    }

    /***
     * 将任意值转换为 Integer，兼容数字、字符串与布尔类型
     ***/
    protected static Integer castToInt(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        }
        if (value instanceof String) {
            String s = ((String) value).trim();
            if (s.isEmpty()) {
                return null;
            }
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException e) {
                throw new NumberFormatException("无法将字符串转换为 Integer: " + s);
            }
        }
        throw new ClassCastException("无法将类型 [" + value.getClass().getName() + "] 转换为 Integer");
    }

    /***
     * 将任意值转换为 Long，兼容数字、字符串与布尔类型
     ***/
    protected static Long castToLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1L : 0L;
        }
        if (value instanceof String) {
            String s = ((String) value).trim();
            if (s.isEmpty()) {
                return null;
            }
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                throw new NumberFormatException("无法将字符串转换为 Long: " + s);
            }
        }
        throw new ClassCastException("无法将类型 [" + value.getClass().getName() + "] 转换为 Long");
    }

    /***
     * 将任意值转换为 Double，兼容数字、字符串与布尔类型
     ***/
    protected static Double castToDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Double) {
            return (Double) value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1.0d : 0.0d;
        }
        if (value instanceof String) {
            String s = ((String) value).trim();
            if (s.isEmpty()) {
                return null;
            }
            try {
                return Double.parseDouble(s);
            } catch (NumberFormatException e) {
                throw new NumberFormatException("无法将字符串转换为 Double: " + s);
            }
        }
        throw new ClassCastException("无法将类型 [" + value.getClass().getName() + "] 转换为 Double");
    }

    /***
     * 将任意值转换为 BigDecimal，兼容数字与字符串类型
     ***/
    protected static BigDecimal castToBigDecimal(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return new BigDecimal(value.toString());
        }
        if (value instanceof String) {
            String s = ((String) value).trim();
            if (s.isEmpty()) {
                return null;
            }
            try {
                return new BigDecimal(s);
            } catch (NumberFormatException e) {
                throw new NumberFormatException("无法将字符串转换为 BigDecimal: " + s);
            }
        }
        throw new ClassCastException("无法将类型 [" + value.getClass().getName() + "] 转换为 BigDecimal");
    }

    /***
     * 将任意值转换为 Boolean，兼容布尔、数字与字符串类型
     ***/
    protected static Boolean castToBoolean(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0;
        }
        if (value instanceof String) {
            String s = ((String) value).trim();
            if ("true".equalsIgnoreCase(s) || "1".equals(s)) {
                return Boolean.TRUE;
            }
            if ("false".equalsIgnoreCase(s) || "0".equals(s)) {
                return Boolean.FALSE;
            }
            throw new ClassCastException("无法将字符串转换为 Boolean: " + s);
        }
        throw new ClassCastException("无法将类型 [" + value.getClass().getName() + "] 转换为 Boolean");
    }
}
