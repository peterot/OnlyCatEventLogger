package com.onlycat.ingest.model;

public record OnlyCatInboundEvent(
        String eventName,
        Integer eventId,
        String eventType,
        Integer eventTriggerSource,
        Integer eventClassification,
        String deviceId,
        String timestamp,
        Long globalId,
        String accessToken,
        Object[] args
) {
}
