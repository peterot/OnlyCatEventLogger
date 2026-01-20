package com.onlycat.ingest.service;

import com.onlycat.ingest.model.OnlyCatEvent;
import com.onlycat.ingest.model.OnlyCatInboundEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class CatEventEnrichmentService {
    private static final Logger log = LoggerFactory.getLogger(CatEventEnrichmentService.class);

    private final CatEventRepository eventRepository;
    private final DedupeCache dedupeCache;
    private final RfidLabelCache rfidLabelCache;
    private final LastSeenRfidEventsCache lastSeenRfidEventsCache;
    private final ExecutorService enrichmentExecutor;

    public CatEventEnrichmentService(CatEventRepository eventRepository,
                                     RfidLabelCache rfidLabelCache,
                                     LastSeenRfidEventsCache lastSeenRfidEventsCache) {
        this.eventRepository = eventRepository;
        this.dedupeCache = new DedupeCache(512);
        this.rfidLabelCache = rfidLabelCache;
        this.lastSeenRfidEventsCache = lastSeenRfidEventsCache;
        this.enrichmentExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "onlycat-enrichment");
            thread.setDaemon(true);
            return thread;
        });
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
                    enrichmentExecutor.execute(() -> {
                        try {
                            Optional<String> rfidCode = lastSeenRfidEventsCache.resolveRfidCode(deviceId, eventId);
                            Optional<String> label = rfidCode.flatMap(rfidLabelCache::getLabel);
                            ingest(inbound, rfidCode.orElse(null), label.orElse(null));
                        } catch (Exception ex) {
                            log.debug("UserEvent enrichment failed; ingesting raw event", ex);
                            ingest(inbound, null, null);
                        }
                    });
                    return;
                }
            } catch (Exception e) {
                log.debug("UserEvent enrichment pre-check failed; ingesting raw event", e);
            }
        }

        ingest(inbound, null, null);
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

    @jakarta.annotation.PreDestroy
    public void shutdown() {
        enrichmentExecutor.shutdownNow();
    }
}
