package com.wcdk.mqtt.core;

/***
* MQTT 主题名称与主题过滤器的校验和匹配工具。
* @author wcdk
***/
public final class MqttTopicMatcher {

    private MqttTopicMatcher() {
    }

    /***
    * 校验主题名称是否合法，主题名称不允许包含通配符。
    * @author wcdk
    ***/
    public static boolean isValidTopicName(String topic) {
        if (topic == null || topic.isBlank()) {
            return false;
        }
        return topic.indexOf('#') < 0 && topic.indexOf('+') < 0;
    }

    /***
    * 校验主题过滤器是否合法，# 必须独占最后一层，+ 必须独占一层。
    * @author wcdk
    ***/
    public static boolean isValidTopicFilter(String filter) {
        if (filter == null || filter.isBlank()) {
            return false;
        }
        String[] levels = filter.split("/", -1);
        for (int i = 0; i < levels.length; i++) {
            String level = levels[i];
            if (level.indexOf('#') >= 0 && (!"#".equals(level) || i != levels.length - 1)) {
                return false;
            }
            if (level.indexOf('+') >= 0 && !"+".equals(level)) {
                return false;
            }
        }
        return true;
    }

    /***
    * 判断主题是否命中过滤器，支持 + 单层通配与 # 多层通配。
    * @author wcdk
    ***/
    public static boolean matches(String filter, String topic) {
        if (!isValidTopicFilter(filter) || !isValidTopicName(topic)) {
            return false;
        }
        // $ 开头的系统主题只能被同样以 $ 开头的过滤器命中
        if (topic.charAt(0) == '$' && filter.charAt(0) != '$') {
            return false;
        }
        String[] filterLevels = filter.split("/", -1);
        String[] topicLevels = topic.split("/", -1);
        int topicIndex = 0;
        for (int filterIndex = 0; filterIndex < filterLevels.length; filterIndex++) {
            String filterLevel = filterLevels[filterIndex];
            if ("#".equals(filterLevel)) {
                return filterIndex == filterLevels.length - 1;
            }
            if (topicIndex >= topicLevels.length) {
                return false;
            }
            if (!"+".equals(filterLevel) && !filterLevel.equals(topicLevels[topicIndex])) {
                return false;
            }
            topicIndex++;
        }
        return topicIndex == topicLevels.length;
    }

    /***
    * 首层快速预判：过滤器与主题首层不一致时直接排除，用于热路径减少完整匹配的开销。
    * 判定为"可能命中"时需再调用 {@link #matches} 做完整校验，判定为 false 时必定不命中。
    * @author wcdk
    * @param filter 主题过滤器
    * @param topic 主题名称
    * @return 可能命中返回 true
    ***/
    public static boolean maybeMatches(String filter, String topic) {
        if (filter == null || topic == null || filter.isEmpty() || topic.isEmpty()) {
            return false;
        }
        // $ 开头的系统主题只能被同样以 $ 开头的过滤器命中
        if (topic.charAt(0) == '$' && filter.charAt(0) != '$') {
            return false;
        }
        String filterFirst = firstLevel(filter);
        if ("+".equals(filterFirst) || "#".equals(filterFirst)) {
            return true;
        }
        return filterFirst.equals(firstLevel(topic));
    }

    static String firstLevel(String value) {
        int slash = value.indexOf('/');
        return slash < 0 ? value : value.substring(0, slash);
    }

    /***
    * 计算两个 QoS 的较低者，用于按订阅 QoS 降级投递。
    * @author wcdk
    ***/
    public static int minQos(int publishQos, int subscriptionQos) {
        return Math.max(0, Math.min(publishQos, subscriptionQos));
    }
}
