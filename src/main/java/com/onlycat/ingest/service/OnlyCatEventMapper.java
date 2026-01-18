package com.onlycat.ingest.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlycat.ingest.model.OnlyCatEvent;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OnlyCatEventMapper {
    private static final Logger log = LoggerFactory.getLogger(OnlyCatEventMapper.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final int RAW_JSON_LIMIT = 45000; // documented in README

    public OnlyCatEvent map(String eventName, Object[] args) {
        Instant ingestedAt = Instant.now();
        Object payload = firstPayload(args);
        Map<String, Object> payloadMap = asMap(payload);
        Map<String, Object> bodyMap = asMap(payloadMap.get("body"));
        Map<String, Object> lookupMap = new LinkedHashMap<>(payloadMap);
        bodyMap.forEach(lookupMap::putIfAbsent);

        Instant eventTime = extractInstant(lookupMap);
        String direction = firstString(lookupMap, List.of("direction", "flow"));
        String catName = findNestedString(lookupMap, List.of("catName", "cat_name"), List.of("cat", "name"));
        String catId = findNestedString(lookupMap, List.of("catId", "cat_id"), List.of("cat", "id"));
        String deviceName = findNestedString(lookupMap, List.of("deviceName", "device_name"), List.of("device", "name"));
        String deviceId = findNestedString(lookupMap, List.of("deviceId", "device_id"), List.of("device", "id"));
        String outcome = firstString(lookupMap, List.of("outcome", "result", "status", "access"));
        Boolean preyDetected = firstBoolean(lookupMap, List.of("preyDetected", "prey_detected", "prey"));

        String resolvedEventType = Optional.ofNullable(firstString(lookupMap, List.of("eventType", "type", "event", "eventClassification", "eventTriggerSource")))
                .orElse(eventName);

        String rawJson = sanitizeRawJson(args);

        return new OnlyCatEvent(
                ingestedAt,
                eventTime,
                resolvedEventType,
                direction == null ? "unknown" : direction,
                catName,
                catId,
                deviceName,
                deviceId,
                outcome == null ? "unknown" : outcome,
                preyDetected,
                rawJson
        );
    }

    private Object firstPayload(Object[] args) {
        if (args == null || args.length == 0) {
            return null;
        }
        for (Object arg : args) {
            if (arg != null) {
                return arg;
            }
        }
        return null;
    }

    private Map<String, Object> asMap(Object payload) {
        if (payload == null) {
            return Map.of();
        }
        if (payload instanceof Map<?, ?> map) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            map.forEach((k, v) -> normalized.put(String.valueOf(k), v));
            return normalized;
        }
        if (payload instanceof JSONObject jsonObject) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (String key : jsonObject.keySet()) {
                normalized.put(key, jsonObject.get(key));
            }
            return normalized;
        }
        try {
            return OBJECT_MAPPER.convertValue(payload, Map.class);
        } catch (IllegalArgumentException ex) {
            log.debug("Unable to convert payload to map: {}", ex.getMessage());
            return Map.of();
        }
    }

    private Instant extractInstant(Map<String, Object> map) {
        for (String key : List.of("eventTime", "event_time", "timestamp", "time", "ts", "createdAt")) {
            Object value = map.get(key);
            Instant instant = toInstant(value);
            if (instant != null) {
                return instant;
            }
        }
        return null;
    }

    private Instant toInstant(Object value) {
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof Number number) {
                long epoch = number.longValue();
                // Heuristic: assume milliseconds when long looks like ms.
                if (epoch > 1_000_000_000_000L) {
                    return Instant.ofEpochMilli(epoch);
                }
                return Instant.ofEpochSecond(epoch);
            }
            if (value instanceof String str && !str.isBlank()) {
                if (str.matches("\\d+")) {
                    return toInstant(Long.parseLong(str));
                }
                return Instant.parse(str);
            }
        } catch (DateTimeParseException | NumberFormatException ex) {
            log.debug("Cannot parse timestamp {}: {}", value, ex.getMessage());
        }
        return null;
    }

    private String firstString(Map<String, Object> map, List<String> keys) {
        for (String key : keys) {
            Object value = map.get(key);
            if (value instanceof String str && !str.isBlank()) {
                return str;
            }
            if (value != null && !(value instanceof Map)) {
                return String.valueOf(value);
            }
        }
        return null;
    }

    private Boolean firstBoolean(Map<String, Object> map, List<String> keys) {
        for (String key : keys) {
            Object value = map.get(key);
            if (value instanceof Boolean b) {
                return b;
            }
            if (value instanceof String str) {
                if ("true".equalsIgnoreCase(str) || "yes".equalsIgnoreCase(str)) {
                    return true;
                }
                if ("false".equalsIgnoreCase(str) || "no".equalsIgnoreCase(str)) {
                    return false;
                }
            }
        }
        return null;
    }

    private String findNestedString(Map<String, Object> map, List<String> directKeys, List<String> nestedPath) {
        String direct = firstString(map, directKeys);
        if (direct != null) {
            return direct;
        }
        Object nested = map.get(nestedPath.get(0));
        if (nested instanceof Map<?, ?> nestedMap) {
            Object value = nestedMap.get(nestedPath.get(1));
            if (value != null) {
                return String.valueOf(value);
            }
        }
        return null;
    }

    private String sanitizeRawJson(Object[] args) {
        Object payload = args != null && args.length == 1 ? args[0] : args;
        try {
            String json = OBJECT_MAPPER.writeValueAsString(payload);
            if (json.length() > RAW_JSON_LIMIT) {
                return json.substring(0, RAW_JSON_LIMIT) + "...(truncated)";
            }
            return json;
        } catch (JsonProcessingException e) {
            String fallback = payload == null ? "" : payload.toString();
            if (fallback.length() > RAW_JSON_LIMIT) {
                return fallback.substring(0, RAW_JSON_LIMIT) + "...(truncated)";
            }
            return fallback;
        }
    }
}
