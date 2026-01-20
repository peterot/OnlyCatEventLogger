package com.onlycat.ingest.model;

import java.time.Instant;

public record OnlyCatRfidProfile(
        String label,
        Integer userId,
        Instant createdAt,
        Instant updatedAt
) {
}
