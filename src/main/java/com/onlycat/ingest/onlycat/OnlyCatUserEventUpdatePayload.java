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
}
