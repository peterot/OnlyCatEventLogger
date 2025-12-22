package com.onlycat.ingest.model;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;

public record OnlyCatEvent(
        Instant ingestedAtUtc,
        Instant eventTimeUtc,
        String eventType,
        String direction,
        String catName,
        String catId,
        String deviceName,
        String deviceId,
        String outcome,
        Boolean preyDetected,
        String rawJson
) {

    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_INSTANT;

    public List<Object> toRow() {
        return List.of(
                ingestedAtUtc != null ? ISO.format(ingestedAtUtc) : "",
                eventTimeUtc != null ? ISO.format(eventTimeUtc) : "",
                nullToEmpty(eventType),
                nullToEmpty(direction),
                nullToEmpty(catName),
                nullToEmpty(catId),
                nullToEmpty(deviceName),
                nullToEmpty(deviceId),
                nullToEmpty(outcome),
                preyDetected == null ? "" : String.valueOf(preyDetected),
                nullToEmpty(rawJson)
        );
    }

    private static String nullToEmpty(String value) {
        return Objects.requireNonNullElse(value, "");
    }
}
