package com.wcdk.mqtt.core.core;

import java.util.List;
import java.util.regex.Pattern;

import com.wcdk.mqtt.core.config.MqttBrokerProperties;
import org.springframework.util.StringUtils;

/**
 * @auther WCDK
 * @date 2026/7/23
 * @version 1.0
 **/
public class MqttBrokerAcl {

    private final MqttBrokerProperties properties;

    public MqttBrokerAcl(MqttBrokerProperties properties) {
        this.properties = properties;
    }

    public boolean canPublish(String username, String clientId, String topic) {
        return allowed(username, clientId, MqttBrokerProperties.Action.PUBLISH, topic);
    }

    public boolean canSubscribe(String username, String clientId, String topic) {
        return allowed(username, clientId, MqttBrokerProperties.Action.SUBSCRIBE, topic);
    }

    private boolean allowed(String username, String clientId, MqttBrokerProperties.Action action, String topic) {
        MqttBrokerProperties.Acl acl = properties.getAcl();
        if (acl == null || !acl.isEnabled()) {
            return true;
        }
        for (MqttBrokerProperties.Rule rule : acl.getRules()) {
            if (matches(rule, username, clientId, action, topic)) {
                return rule.getPolicy() == MqttBrokerProperties.Policy.ALLOW;
            }
        }
        return acl.getDefaultPolicy() == MqttBrokerProperties.Policy.ALLOW;
    }

    private boolean matches(MqttBrokerProperties.Rule rule,
                            String username,
                            String clientId,
                            MqttBrokerProperties.Action action,
                            String topic) {
        if (rule == null || !rule.isEnabled()) {
            return false;
        }
        if (rule.getAction() != MqttBrokerProperties.Action.ALL && rule.getAction() != action) {
            return false;
        }
        if (!matchesText(rule.getUsernames(), username)) {
            return false;
        }
        if (!matchesText(rule.getClientIds(), clientId)) {
            return false;
        }
        return matchesTopic(rule.getTopicFilters(), topic);
    }

    private boolean matchesText(List<String> patterns, String value) {
        List<String> activePatterns = activePatterns(patterns);
        if (activePatterns.isEmpty()) {
            return true;
        }
        String candidate = value == null ? "" : value;
        for (String pattern : activePatterns) {
            if ("*".equals(pattern) || candidate.equals(pattern) || globMatches(pattern, candidate)) {
                return true;
            }
        }
        return false;
    }

    private boolean matchesTopic(List<String> topicFilters, String topic) {
        List<String> activeFilters = activePatterns(topicFilters);
        if (activeFilters.isEmpty()) {
            return true;
        }
        for (String topicFilter : activeFilters) {
            if (matchesTopicOrFilter(topicFilter, topic)) {
                return true;
            }
        }
        return false;
    }

    private boolean matchesTopicOrFilter(String aclFilter, String topicOrFilter) {
        if (MqttTopicFilter.isValidTopicName(topicOrFilter)) {
            return MqttTopicFilter.matches(aclFilter, topicOrFilter);
        }
        return MqttTopicFilter.isValidSubscriptionFilter(topicOrFilter) && coversSubscriptionFilter(aclFilter, topicOrFilter);
    }

    private boolean coversSubscriptionFilter(String aclFilter, String requestedFilter) {
        if (!MqttTopicFilter.isValidSubscriptionFilter(aclFilter)
                || !MqttTopicFilter.isValidSubscriptionFilter(requestedFilter)) {
            return false;
        }
        if (aclFilter.equals(requestedFilter) || "#".equals(aclFilter)) {
            return true;
        }
        String[] aclLevels = aclFilter.split("/", -1);
        String[] requestedLevels = requestedFilter.split("/", -1);
        for (int i = 0; i < requestedLevels.length; i++) {
            if (i >= aclLevels.length) {
                return false;
            }
            String aclLevel = aclLevels[i];
            String requestedLevel = requestedLevels[i];
            if ("#".equals(aclLevel)) {
                return i == aclLevels.length - 1;
            }
            if ("#".equals(requestedLevel)) {
                return false;
            }
            if ("+".equals(aclLevel)) {
                continue;
            }
            if ("+".equals(requestedLevel) || !aclLevel.equals(requestedLevel)) {
                return false;
            }
        }
        return aclLevels.length == requestedLevels.length
                || (aclLevels.length == requestedLevels.length + 1 && "#".equals(aclLevels[requestedLevels.length]));
    }

    private List<String> activePatterns(List<String> patterns) {
        if (patterns == null || patterns.isEmpty()) {
            return List.of();
        }
        return patterns.stream()
                .filter(StringUtils::hasText)
                .map(String::trim)
                .toList();
    }

    private boolean globMatches(String pattern, String value) {
        if (pattern.indexOf('*') < 0) {
            return false;
        }
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < pattern.length(); i++) {
            char current = pattern.charAt(i);
            if (current == '*') {
                regex.append(".*");
            } else {
                regex.append(Pattern.quote(String.valueOf(current)));
            }
        }
        regex.append('$');
        return Pattern.matches(regex.toString(), value);
    }
}
