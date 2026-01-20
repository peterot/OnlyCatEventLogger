package com.onlycat.ingest.service;

import com.onlycat.ingest.onlycat.OnlyCatEmitter;
import com.onlycat.ingest.model.OnlyCatEvent;
import com.onlycat.ingest.model.OnlyCatInboundEvent;
import com.onlycat.ingest.model.OnlyCatRfidEvent;
import com.onlycat.ingest.model.OnlyCatRfidProfile;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Service
public class CatEventEnrichmentService {
    private static final Logger log = LoggerFactory.getLogger(CatEventEnrichmentService.class);
    private static final int MAX_RFID_RETRIES = 4;
    private static final long[] RFID_RETRY_DELAYS_MS = {250, 500, 1000, 1500};

    private final CatEventRepository eventRepository;
    private final OnlyCatEmitter emitter;
    private final DedupeCache dedupeCache;

    // Enrichment: RFID -> cat label (name) cache and pending user events awaiting enrichment.
    private final Map<String, String> rfidLabelCache = new ConcurrentHashMap<>();
    private final Map<Integer, PendingUserEvent> pendingUserEvents = new ConcurrentHashMap<>();

    private final ScheduledExecutorService enrichmentScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "onlycat-enrichment");
        t.setDaemon(true);
        return t;
    });

    public CatEventEnrichmentService(CatEventRepository eventRepository, OnlyCatEmitter emitter) {
        this.eventRepository = eventRepository;
        this.emitter = emitter;
        this.dedupeCache = new DedupeCache(512);
    }

    @EventListener
    public void onInbound(OnlyCatInboundEvent inbound) {
        handleAnyEvent(inbound);
    }

    private void handleAnyEvent(OnlyCatInboundEvent inbound) {
        String event = inbound.eventName();
        // Special-case: enrich userEventUpdate(create) with cat identity (RFID profile label).
        if ("userEventUpdate".equals(event)) {
            String type = inbound.eventType();
            Integer eventId = inbound.eventId();
            String deviceId = inbound.deviceId();
            try {
                // Only enrich create events with a known deviceId/eventId.
                if (eventId != null && StringUtils.hasText(deviceId) && "create".equalsIgnoreCase(type)) {
                    PendingUserEvent pending = new PendingUserEvent(eventId, deviceId, inbound);
                    PendingUserEvent existing = pendingUserEvents.putIfAbsent(eventId, pending);
                    if (existing == null) {
                        // Fallback: if enrichment doesn't complete quickly, ingest raw event anyway.
                        ScheduledFuture<?> fallback = enrichmentScheduler.schedule(() -> {
                            PendingUserEvent p = pendingUserEvents.remove(eventId);
                            if (p != null && !p.ingested) {
                                if (p.retry != null) {
                                    p.retry.cancel(false);
                                }
                                log.info("Enrichment timeout for eventId={} deviceId={} - ingesting raw event", eventId, deviceId);
                                ingest(p.original, null, null);
                                p.ingested = true;
                            }
                        }, 3, TimeUnit.SECONDS);
                        pending.fallback = fallback;

                        // Kick off enrichment asynchronously via ACK callbacks.
                        resolveCatForUserEvent(pending);
                        return; // do not ingest yet; enrichment will ingest (or fallback will)
                    }
                }
            } catch (Exception e) {
                log.debug("UserEvent enrichment pre-check failed; ingesting raw event", e);
            }
        }

        ingest(inbound, null, null);
    }

    private void resolveCatForUserEvent(PendingUserEvent pending) {
        List<OnlyCatRfidEvent> events = emitter.requestLastSeenRfidCodesByDevice(pending.deviceId, 20);
        String rfidCode = extractRfidForEventId(events, pending.eventId);
        if (!StringUtils.hasText(rfidCode)) {
            log.info("Enrichment: no rfidCode found for eventId={} deviceId={}", pending.eventId, pending.deviceId);
            scheduleRfidRetry(pending);
            return;
        }

        // Cache hit
        String label = rfidLabelCache.get(rfidCode);
        if (StringUtils.hasText(label)) {
            ingestEnrichedUserEvent(pending, rfidCode, label);
            return;
        }

        // Fetch profile for name/label
        resolveRfidProfileAndIngest(pending, rfidCode);
    }

    private String extractRfidForEventId(List<OnlyCatRfidEvent> events, int eventId) {
        if (events == null || events.isEmpty()) {
            return "";
        }
        for (OnlyCatRfidEvent event : events) {
            if (event == null) {
                continue;
            }
            if (event.eventId() != null && event.eventId() == eventId) {
                String code = event.rfidCode();
                if (StringUtils.hasText(code)) {
                    return code;
                }
            }
        }
        return "";
    }

    private void resolveRfidProfileAndIngest(PendingUserEvent pending, String rfidCode) {
        Optional<OnlyCatRfidProfile> profile = emitter.requestRfidProfile(rfidCode);
        if (profile.isPresent() && StringUtils.hasText(profile.get().label())) {
            String label = profile.get().label();
            rfidLabelCache.put(rfidCode, label);
            ingestEnrichedUserEvent(pending, rfidCode, label);
        } else {
            log.info("Enrichment: getRfidProfile returned no label for rfidCode={}", rfidCode);
        }
    }

    private void ingestEnrichedUserEvent(PendingUserEvent pending, String rfidCode, String label) {
        PendingUserEvent removed = pendingUserEvents.remove(pending.eventId);
        if (removed == null) {
            // Already ingested by fallback
            return;
        }
        if (removed.fallback != null) {
            removed.fallback.cancel(false);
        }
        if (removed.retry != null) {
            removed.retry.cancel(false);
        }
        if (removed.ingested) {
            return;
        }

        log.info("Enrichment complete for eventId={} deviceId={} catLabel={}", removed.eventId, removed.deviceId, label);
        ingest(removed.original, rfidCode, label);
        removed.ingested = true;
    }

    private void ingest(OnlyCatInboundEvent inbound, String rfidCode, String catLabel) {
        OnlyCatEvent event = new OnlyCatEvent(
                java.time.Instant.now(),
                parseInstant(inbound.timestamp()),
                inbound.eventType() == null ? inbound.eventName() : inbound.eventType(),
                "unknown",
                catLabel,
                rfidCode,
                null,
                inbound.deviceId(),
                "unknown",
                null,
                buildRawSummary(inbound, rfidCode, catLabel)
        );
        String key = hash(inbound.eventName() + ":" + event.rawJson() + ":" + (event.eventTimeUtc() != null ? event.eventTimeUtc().toEpochMilli() : ""));
        if (dedupeCache.seen(key)) {
            log.debug("Duplicate event suppressed for key {}", key);
            return;
        }
        eventRepository.append(event);
        log.info("Appended event type={} cat={} device={} time={}", event.eventType(), event.catId(), event.deviceId(), event.eventTimeUtc());
    }

    private String hash(String value) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(bytes);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private java.time.Instant parseInstant(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        try {
            return java.time.Instant.parse(value);
        } catch (Exception ignore) {
            return null;
        }
    }

    private String buildRawSummary(OnlyCatInboundEvent inbound, String rfidCode, String catLabel) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("event=").append(inbound.eventName());
        if (StringUtils.hasText(inbound.deviceId())) {
            sb.append(" deviceId=").append(inbound.deviceId());
        }
        if (inbound.eventId() != null) {
            sb.append(" eventId=").append(inbound.eventId());
        }
        if (StringUtils.hasText(inbound.timestamp())) {
            sb.append(" timestamp=").append(inbound.timestamp());
        }
        if (StringUtils.hasText(rfidCode)) {
            sb.append(" rfidCode=").append(rfidCode);
        }
        if (StringUtils.hasText(catLabel)) {
            sb.append(" catLabel=").append(catLabel);
        }
        return sb.toString();
    }

    private static class PendingUserEvent {
        final int eventId;
        final String deviceId;
        final OnlyCatInboundEvent original;
        volatile boolean ingested = false;
        volatile ScheduledFuture<?> fallback;
        volatile ScheduledFuture<?> retry;
        volatile int retryCount = 0;

        PendingUserEvent(int eventId, String deviceId, OnlyCatInboundEvent original) {
            this.eventId = eventId;
            this.deviceId = deviceId;
            this.original = original;
        }
    }

    private void scheduleRfidRetry(PendingUserEvent pending) {
        PendingUserEvent current = pendingUserEvents.get(pending.eventId);
        if (current == null || current.ingested) {
            return;
        }
        if (current.retryCount >= MAX_RFID_RETRIES) {
            log.info("Enrichment: giving up on RFID lookup for eventId={} after {} retries",
                    current.eventId, current.retryCount);
            return;
        }
        long delayMs = RFID_RETRY_DELAYS_MS[Math.min(current.retryCount, RFID_RETRY_DELAYS_MS.length - 1)];
        current.retryCount++;
        if (current.retry != null) {
            current.retry.cancel(false);
        }
        current.retry = enrichmentScheduler.schedule(() -> resolveCatForUserEvent(current), delayMs, TimeUnit.MILLISECONDS);
        log.info("Enrichment: retry {} scheduled in {}ms for eventId={}",
                current.retryCount, delayMs, current.eventId);
    }


    @PreDestroy
    public void shutdown() {
        enrichmentScheduler.shutdownNow();
    }
}
