package com.onlycat.ingest.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.onlycat.ingest.model.OnlyCatRfidEvent;
import com.onlycat.ingest.onlycat.OnlyCatEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Component
public class LastSeenRfidEventsCache {
    private static final Logger log = LoggerFactory.getLogger(LastSeenRfidEventsCache.class);
    private static final int MAX_RFID_RETRIES = 4;
    private static final long[] RFID_RETRY_DELAYS_MS = {250, 500, 1000, 1500};

    private final OnlyCatEmitter emitter;
    private final Cache<String, List<OnlyCatRfidEvent>> cache;

    public LastSeenRfidEventsCache(OnlyCatEmitter emitter) {
        this.emitter = emitter;
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(15))
                .maximumSize(256)
                .build();
    }

    public List<String> resolveRfidCodes(String deviceId, int eventId) {
        if (!StringUtils.hasText(deviceId)) {
            return List.of();
        }
        for (int attempt = 0; attempt <= MAX_RFID_RETRIES; attempt++) {
            List<String> rfidCodes = lookupRfidCodes(deviceId, eventId, attempt == 0);
            if (!rfidCodes.isEmpty()) {
                return rfidCodes;
            }
            if (attempt < MAX_RFID_RETRIES) {
                long delayMs = RFID_RETRY_DELAYS_MS[Math.min(attempt, RFID_RETRY_DELAYS_MS.length - 1)];
                log.info("Enrichment: retry {} in {}ms for eventId={} deviceId={}", attempt + 1, delayMs, eventId, deviceId);
                sleep(delayMs);
            }
        }
        log.info("Enrichment: no rfidCode found for eventId={} deviceId={}", eventId, deviceId);
        return List.of();
    }

    private List<String> lookupRfidCodes(String deviceId, int eventId, boolean allowCache) {
        List<OnlyCatRfidEvent> events = allowCache ? cache.getIfPresent(deviceId) : null;
        if (events == null || events.isEmpty()) {
            events = fetchEvents(deviceId);
        }
        if (events == null || events.isEmpty()) {
            return List.of();
        }
        Set<String> matches = new LinkedHashSet<>();
        for (OnlyCatRfidEvent event : events) {
            if (event == null || event.eventId() == null) {
                continue;
            }
            if (event.eventId() == eventId && StringUtils.hasText(event.rfidCode())) {
                matches.add(event.rfidCode());
            }
        }
        if (matches.isEmpty()) {
            return List.of();
        }
        return new ArrayList<>(matches);
    }

    private List<OnlyCatRfidEvent> fetchEvents(String deviceId) {
        List<OnlyCatRfidEvent> events = emitter.requestLastSeenRfidCodesByDevice(deviceId, 20);
        if (events == null) {
            events = Collections.emptyList();
        }
        cache.put(deviceId, events);
        return events;
    }

    private void sleep(long delayMs) {
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
