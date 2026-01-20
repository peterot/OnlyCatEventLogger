package com.onlycat.ingest.onlycat;

import com.fasterxml.jackson.annotation.JsonProperty;

record OnlyCatDeviceRfidRequest(
        @JsonProperty("deviceId") String deviceId,
        @JsonProperty("limit") int limit
) {
}
