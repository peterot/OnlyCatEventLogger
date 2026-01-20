package com.onlycat.ingest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OnlyCatRfidEvent(
        @JsonProperty("eventId") Integer eventId,
        @JsonProperty("rfidCode") String rfidCode,
        @JsonProperty("deviceId") String deviceId,
        @JsonProperty("timestamp") Instant timestamp
) {
}
