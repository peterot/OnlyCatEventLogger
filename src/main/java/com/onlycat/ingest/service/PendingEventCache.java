package com.onlycat.ingest.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.onlycat.ingest.model.OnlyCatEvent;
import com.onlycat.ingest.model.OnlyCatEventClassification;
import com.onlycat.ingest.model.OnlyCatInboundEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.Optional;

public class PendingEventCache {
    private static final Logger log = LoggerFactory.getLogger(PendingEventCache.class);

    private final Cache<String, OnlyCatEvent> pendingEvents;

    public PendingEventCache(Duration expiry, int maxSize) {
        this.pendingEvents = Caffeine.newBuilder()
                .expireAfterWrite(expiry)
                .maximumSize(maxSize)
                .removalListener((String key, OnlyCatEvent value, RemovalCause cause) -> {
                    if (cause == RemovalCause.EXPIRED && value != null) {
                        log.warn("Pending event expired without final classification eventId={} deviceId={}",
                                value.eventId(), value.deviceId());
                    }
                })
                .build();
    }

    public Optional<OnlyCatEvent> apply(OnlyCatInboundEvent inbound, OnlyCatEvent event) {
        String eventKey = eventKey(inbound);
        if (eventKey == null) {
            return Optional.of(event);
        }
        if (!isFinalClassification(inbound)) {
            pendingEvents.put(eventKey, event);
            log.debug("Queued pending eventId={} deviceId={} awaiting classification update",
                    inbound.eventId(), inbound.deviceId());
            return Optional.empty();
        }
        OnlyCatEvent pending = pendingEvents.getIfPresent(eventKey);
        if (pending != null) {
            pendingEvents.invalidate(eventKey);
            return Optional.of(mergePending(pending, event));
        }
        return Optional.of(event);
    }

    private boolean isFinalClassification(OnlyCatInboundEvent inbound) {
        OnlyCatEventClassification classification = inbound.eventClassification();
        return classification != null
                && classification != OnlyCatEventClassification.UNKNOWN_CLASS
                && classification != OnlyCatEventClassification.UNKNOWN;
    }

    private OnlyCatEvent mergePending(OnlyCatEvent pending, OnlyCatEvent updated) {
        return new OnlyCatEvent(
                updated.ingestedAtUtc(),
                updated.eventTimeUtc() != null ? updated.eventTimeUtc() : pending.eventTimeUtc(),
                pending.eventName(),
                pending.eventType(),
                pending.eventId(),
                updated.eventTriggerSource() != null ? updated.eventTriggerSource() : pending.eventTriggerSource(),
                updated.eventClassification() != null ? updated.eventClassification() : pending.eventClassification(),
                updated.globalId() != null ? updated.globalId() : pending.globalId(),
                updated.deviceId() != null ? updated.deviceId() : pending.deviceId(),
                pending.rfidCode(),
                pending.catLabels()
        );
    }

    private String eventKey(OnlyCatInboundEvent inbound) {
        if (inbound.eventId() == null || !StringUtils.hasText(inbound.deviceId())) {
            return null;
        }
        return inbound.eventId() + ":" + inbound.deviceId();
    }
}
