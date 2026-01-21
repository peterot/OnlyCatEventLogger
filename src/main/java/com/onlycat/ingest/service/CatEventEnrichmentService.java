package com.onlycat.ingest.service;

import com.onlycat.ingest.model.OnlyCatEvent;
import com.onlycat.ingest.model.OnlyCatInboundEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
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
    private final PendingEventCache pendingEventCache;

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
        this.pendingEventCache = new PendingEventCache(java.time.Duration.ofMinutes(10), 2048);
    }

    @EventListener
    public void onInbound(OnlyCatInboundEvent inbound) {
        handleAnyEvent(inbound);
    }

    private void handleAnyEvent(OnlyCatInboundEvent inbound) {
        if (inbound.eventId() == null) {
            log.debug("Skipping non-cat-flap event without eventId: {}", inbound.eventName());
            return;
        }
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
                            java.util.List<String> rfidCodes = lastSeenRfidEventsCache.resolveRfidCodes(deviceId, eventId);
                            java.util.List<String> labels = resolveLabels(rfidCodes);
                            ingest(inbound, firstNonBlank(rfidCodes), labels);
                        } catch (Exception ex) {
                            log.debug("UserEvent enrichment failed; ingesting raw event", ex);
                            ingest(inbound, null, java.util.List.of());
                        }
                    });
                    return;
                }
            } catch (Exception e) {
                log.debug("UserEvent enrichment pre-check failed; ingesting raw event", e);
            }
        }

        ingest(inbound, null, java.util.List.of());
    }

    private void ingest(OnlyCatInboundEvent inbound, String rfidCode, java.util.List<String> catLabels) {
        String triggerSource = inbound.eventTriggerSource() == null ? null : inbound.eventTriggerSource().prettyLabel();
        String classification = inbound.eventClassification() == null ? null : inbound.eventClassification().prettyLabel();
        OnlyCatEvent event = new OnlyCatEvent(
                java.time.Instant.now(),
                parseInstant(inbound.timestamp()),
                inbound.eventName(),
                inbound.eventType() == null ? inbound.eventName() : inbound.eventType(),
                inbound.eventId(),
                triggerSource,
                classification,
                inbound.globalId(),
                inbound.deviceId(),
                rfidCode,
                catLabels
        );
        String key = hash(inbound.eventName() + ":" + inbound.eventId() + ":" + inbound.deviceId() + ":" +
                (event.eventTimeUtc() != null ? event.eventTimeUtc().toEpochMilli() : ""));
        java.util.Optional<OnlyCatEvent> finalEvent = pendingEventCache.apply(inbound, event);
        if (finalEvent.isEmpty()) {
            return;
        }
        event = finalEvent.get();
        if (dedupeCache.seen(key)) {
            log.debug("Duplicate event suppressed for key {}", key);
            return;
        }
        eventRepository.append(event);
        log.info("Appended event type={} cats={} device={} time={}", event.eventType(), catLabelsSummary(event.catLabels()), event.deviceId(), event.eventTimeUtc());
    }

    private java.util.List<String> resolveLabels(java.util.List<String> rfidCodes) {
        if (rfidCodes == null || rfidCodes.isEmpty()) {
            return java.util.List.of();
        }
        java.util.List<String> labels = new java.util.ArrayList<>();
        for (String rfidCode : rfidCodes) {
            rfidLabelCache.getLabel(rfidCode).ifPresent(labels::add);
        }
        return labels;
    }

    private String firstNonBlank(java.util.List<String> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        for (String value : values) {
            if (StringUtils.hasText(value)) {
                return value;
            }
        }
        return null;
    }

    private String catLabelsSummary(java.util.List<String> labels) {
        if (labels == null || labels.isEmpty()) {
            return null;
        }
        return String.join("|", labels);
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

    @jakarta.annotation.PreDestroy
    public void shutdown() {
        enrichmentExecutor.shutdownNow();
    }
}
