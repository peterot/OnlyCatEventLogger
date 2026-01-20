package com.onlycat.ingest.onlycat;

import com.fasterxml.jackson.annotation.JsonProperty;

record OnlyCatRfidProfileRequest(
        @JsonProperty("rfidCode") String rfidCode
) {
}
