package com.onlycat.ingest.onlycat;

import com.fasterxml.jackson.annotation.JsonProperty;

record OnlyCatSubscribeRequest(
        @JsonProperty("subscribe") boolean subscribe
) {
}
