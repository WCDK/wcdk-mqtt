package com.wcdk.mqtt.core.core;

import org.springframework.util.StringUtils;
/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
public final class MqttTopicFilter {

    private MqttTopicFilter() {
    }

    public static boolean isValidTopicName(String topic) {
        return StringUtils.hasText(topic) && topic.indexOf('#') < 0 && topic.indexOf('+') < 0;
    }

    public static boolean isValidSubscriptionFilter(String filter) {
        if (!StringUtils.hasText(filter)) {
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

    public static boolean matches(String filter, String topic) {
        if (!isValidSubscriptionFilter(filter) || !isValidTopicName(topic)) {
            return false;
        }
        if (topic.startsWith("$") && !filter.startsWith("$")) {
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
}
