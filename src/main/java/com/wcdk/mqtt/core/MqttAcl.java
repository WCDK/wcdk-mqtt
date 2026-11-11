package com.wcdk.mqtt.core;

import java.util.List;
import java.util.regex.Pattern;

import com.wcdk.mqtt.config.MqttProperties;
import org.springframework.util.StringUtils;

/***
* MQTT 访问控制列表，按用户名、客户端 ID 与主题过滤器判定发布订阅权限。
* @author wcdk
***/
public class MqttAcl {

    private final MqttProperties properties;

    public MqttAcl(MqttProperties properties) {
        this.properties = properties;
    }

    /***
    * 判断是否允许发布到指定主题。
    * @author wcdk
    * @param username 用户名
    * @param clientId 客户端 ID
    * @param topic 主题名称
    * @return 允许返回 true
    ***/
    public boolean canPublish(String username, String clientId, String topic) {
        return allowed(username, clientId, MqttProperties.Action.PUBLISH, topic);
    }

    /***
    * 判断是否允许订阅指定主题过滤器。
    * @author wcdk
    * @param username 用户名
    * @param clientId 客户端 ID
    * @param topicFilter 主题过滤器
    * @return 允许返回 true
    ***/
    public boolean canSubscribe(String username, String clientId, String topicFilter) {
        return allowed(username, clientId, MqttProperties.Action.SUBSCRIBE, topicFilter);
    }

    private boolean allowed(String username, String clientId, MqttProperties.Action action, String topic) {
        MqttProperties.Acl acl = properties.getAcl();
        if (acl == null || !acl.isEnabled()) {
            return true;
        }
        List<MqttProperties.AclRule> rules = acl.getRules();
        if (rules == null) {
            return acl.getDefaultPolicy() == MqttProperties.Policy.ALLOW;
        }
        for (MqttProperties.AclRule rule : rules) {
            if (matches(rule, username, clientId, action, topic)) {
                return rule.getPolicy() == MqttProperties.Policy.ALLOW;
            }
        }
        return acl.getDefaultPolicy() == MqttProperties.Policy.ALLOW;
    }

    private boolean matches(MqttProperties.AclRule rule,
                            String username,
                            String clientId,
                            MqttProperties.Action action,
                            String topic) {
        if (rule == null || !rule.isEnabled()) {
            return false;
        }
        if (rule.getAction() != MqttProperties.Action.ALL && rule.getAction() != action) {
            return false;
        }
        if (!matchesAny(rule.getUsernames(), username)) {
            return false;
        }
        if (!matchesAny(rule.getClientIds(), clientId)) {
            return false;
        }
        return matchesTopic(rule.getTopicFilters(), topic, action);
    }

    /***
    * 按通配模式匹配文本，空列表表示不限制。
    * @author wcdk
    ***/
    private boolean matchesAny(List<String> patterns, String value) {
        List<String> active = active(patterns);
        if (active.isEmpty()) {
            return true;
        }
        String candidate = value == null ? "" : value;
        for (String pattern : active) {
            if ("*".equals(pattern) || candidate.equals(pattern) || globMatches(pattern, candidate)) {
                return true;
            }
        }
        return false;
    }

    /***
    * 主题匹配：发布场景按主题名称匹配，订阅场景按过滤器覆盖关系匹配。
    * @author wcdk
    ***/
    private boolean matchesTopic(List<String> topicFilters, String topic, MqttProperties.Action action) {
        List<String> active = active(topicFilters);
        if (active.isEmpty()) {
            return true;
        }
        for (String aclFilter : active) {
            if (action == MqttProperties.Action.SUBSCRIBE) {
                if (MqttTopicMatcher.isValidTopicFilter(topic) && covers(aclFilter, topic)) {
                    return true;
                }
            } else if (MqttTopicMatcher.matches(aclFilter, topic)) {
                return true;
            }
        }
        return false;
    }

    /***
    * 判断 ACL 过滤器是否完全覆盖请求的订阅过滤器。
    * @author wcdk
    ***/
    private boolean covers(String aclFilter, String requestedFilter) {
        if (!MqttTopicMatcher.isValidTopicFilter(aclFilter) || !MqttTopicMatcher.isValidTopicFilter(requestedFilter)) {
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

    private List<String> active(List<String> patterns) {
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
