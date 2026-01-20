package com.onlycat.ingest.model;

import java.time.Instant;

public record OnlyCatRfidEvent(
        Integer eventId,
        String rfidCode,
        String deviceId,
        Instant timestamp
) {
}
