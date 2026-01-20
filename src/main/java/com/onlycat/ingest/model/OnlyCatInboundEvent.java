package com.onlycat.ingest.model;

public record OnlyCatInboundEvent(
        String eventName,
        Integer eventId,
        String eventType,
        OnlyCatEventTriggerSource eventTriggerSource,
        OnlyCatEventClassification eventClassification,
        String deviceId,
        String timestamp,
        Long globalId,
        String accessToken,
        Object[] args
) {
}
