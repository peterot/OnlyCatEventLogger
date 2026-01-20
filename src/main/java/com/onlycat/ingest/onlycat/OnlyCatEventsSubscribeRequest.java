package com.onlycat.ingest.onlycat;

import com.fasterxml.jackson.annotation.JsonProperty;

record OnlyCatEventsSubscribeRequest(
        @JsonProperty("subscribe") boolean subscribe,
        @JsonProperty("limit") int limit
) {
}
