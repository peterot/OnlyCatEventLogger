package com.onlycat.ingest.model;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;

public record OnlyCatEvent(
        Instant ingestedAtUtc,
        Instant eventTimeUtc,
        String eventName,
        String eventType,
        Integer eventId,
        String eventTriggerSource,
        String eventClassification,
        Long globalId,
        String deviceId,
        String rfidCode,
        List<String> catLabels
) {

    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_INSTANT;

    public List<Object> toRow(String mappedCatLabel) {
        return List.of(
                ingestedAtUtc != null ? ISO.format(ingestedAtUtc) : "",
                eventTimeUtc != null ? ISO.format(eventTimeUtc) : "",
                nullToEmpty(eventName),
                nullToEmpty(eventType),
                eventId == null ? "" : String.valueOf(eventId),
                nullToEmpty(eventTriggerSource),
                nullToEmpty(eventClassification),
                globalId == null ? "" : String.valueOf(globalId),
                nullToEmpty(deviceId),
                nullToEmpty(rfidCode),
                nullToEmpty(mappedCatLabel)
        );
    }

    private static String nullToEmpty(String value) {
        return Objects.requireNonNullElse(value, "");
    }
}
