package com.onlycat.ingest.onlycat;

import com.fasterxml.jackson.annotation.JsonProperty;

record OnlyCatDeviceSubscribeRequest(
        @JsonProperty("deviceId") String deviceId,
        @JsonProperty("subscribe") boolean subscribe
) {
}
