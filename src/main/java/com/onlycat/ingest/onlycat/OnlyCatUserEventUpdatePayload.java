package com.onlycat.ingest.onlycat;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
record OnlyCatUserEventUpdatePayload(
        @JsonProperty("eventId") Integer eventId,
        @JsonProperty("type") String type,
        @JsonProperty("eventTriggerSource") Integer eventTriggerSource,
        @JsonProperty("eventClassification") Integer eventClassification,
        @JsonProperty("globalId") Long globalId,
        @JsonProperty("accessToken") String accessToken,
        @JsonProperty("deviceId") String deviceId,
        @JsonProperty("timestamp") String timestamp,
        @JsonProperty("body") OnlyCatUserEventBody body
) {
    Integer effectiveEventId() {
        return eventId != null ? eventId : (body == null ? null : body.eventId());
    }

    Integer effectiveEventTriggerSource() {
        return eventTriggerSource != null ? eventTriggerSource : (body == null ? null : body.eventTriggerSource());
    }

    Integer effectiveEventClassification() {
        return eventClassification != null ? eventClassification : (body == null ? null : body.eventClassification());
    }

    String effectiveDeviceId() {
        return hasText(deviceId) ? deviceId : (body == null ? null : body.deviceId());
    }

    String effectiveTimestamp() {
        return hasText(timestamp) ? timestamp : (body == null ? null : body.timestamp());
    }

    Long effectiveGlobalId() {
        return globalId != null ? globalId : (body == null ? null : body.globalId());
    }

    String effectiveAccessToken() {
        return hasText(accessToken) ? accessToken : (body == null ? null : body.accessToken());
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
