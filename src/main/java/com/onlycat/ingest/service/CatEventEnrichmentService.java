package com.onlycat.ingest.service;

import com.onlycat.ingest.model.OnlyCatEvent;
import com.onlycat.ingest.model.OnlyCatEventClassification;
import com.onlycat.ingest.model.OnlyCatInboundEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class CatEventEnrichmentService {
    private static final Logger log = LoggerFactory.getLogger(CatEventEnrichmentService.class);

    private final CatEventRepository eventRepository;
    private final DedupeCache dedupeCache;
    private final RfidLabelCache rfidLabelCache;
    private final LastSeenRfidEventsCache lastSeenRfidEventsCache;
    private final BackfillCheckpointStore checkpointStore;
    private final ExecutorService enrichmentExecutor;
    private final ScheduledExecutorService pendingFallbackExecutor;
    private final PendingEventCache pendingEventCache;
    private final long pendingFallbackDelayMs;

    public CatEventEnrichmentService(CatEventRepository eventRepository,
                                     RfidLabelCache rfidLabelCache,
                                     LastSeenRfidEventsCache lastSeenRfidEventsCache,
                                     BackfillCheckpointStore checkpointStore) {
        this.eventRepository = eventRepository;
        this.dedupeCache = new DedupeCache(512);
        this.rfidLabelCache = rfidLabelCache;
        this.lastSeenRfidEventsCache = lastSeenRfidEventsCache;
        this.checkpointStore = checkpointStore;
        this.enrichmentExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "onlycat-enrichment");
            thread.setDaemon(true);
            return thread;
        });
        this.pendingFallbackExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "onlycat-pending-fallback");
            thread.setDaemon(true);
            return thread;
        });
        this.pendingEventCache = new PendingEventCache(Duration.ofMinutes(10), 2048);
        this.pendingFallbackDelayMs = Duration.ofSeconds(45).toMillis();
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
                    String eventKey = eventKey(eventId, deviceId);
                    if (eventKey != null && !isFinalClassification(inbound)) {
                        OnlyCatEvent pendingShell = buildEvent(inbound, null, null);
                        pendingEventCache.put(eventKey, pendingShell);
                        schedulePendingFallback(eventKey, 1);
                    }
                    enrichmentExecutor.execute(() -> {
                        try {
                            List<String> rfidCodes = lastSeenRfidEventsCache.resolveRfidCodes(deviceId, eventId);
                            List<String> labels = resolveLabels(rfidCodes);
                            ingest(inbound, firstNonBlank(rfidCodes), labels);
                        } catch (Exception ex) {
                            log.debug("UserEvent enrichment failed; ingesting raw event", ex);
                            ingest(inbound, null, List.of());
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

    private void ingest(OnlyCatInboundEvent inbound, String rfidCode, List<String> catLabels) {
        OnlyCatEvent event = buildEvent(inbound, rfidCode, catLabels);
        String key = dedupeKey(inbound, event);
        String eventKey = eventKey(inbound.eventId(), inbound.deviceId());
        if (!isUserEventCreate(inbound)) {
            if (eventKey == null || !pendingEventCache.hasPending(eventKey)) {
                log.info("Skipping non-create event without pending match eventId={} deviceId={} eventName={}",
                        inbound.eventId(), inbound.deviceId(), inbound.eventName());
                return;
            }
            if (!isFinalClassification(inbound)) {
                log.info("Skipping non-create event without final classification eventId={} deviceId={} eventName={}",
                        inbound.eventId(), inbound.deviceId(), inbound.eventName());
                return;
            }
        }
        Optional<OnlyCatEvent> finalEvent = pendingEventCache.apply(inbound, event);
        if (finalEvent.isEmpty()) {
            return;
        }
        event = finalEvent.get();
        appendIfNotSeen(event, key);
    }

    private List<String> resolveLabels(List<String> rfidCodes) {
        if (rfidCodes == null || rfidCodes.isEmpty()) {
            return List.of();
        }
        List<String> labels = new ArrayList<>();
        for (String rfidCode : rfidCodes) {
            rfidLabelCache.getLabel(rfidCode).ifPresent(labels::add);
        }
        return labels;
    }

    private String firstNonBlank(List<String> values) {
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

    private String catLabelsSummary(List<String> labels) {
        if (labels == null || labels.isEmpty()) {
            return null;
        }
        return String.join("|", labels);
    }

    private OnlyCatEvent buildEvent(OnlyCatInboundEvent inbound, String rfidCode, List<String> catLabels) {
        String triggerSource = inbound.eventTriggerSource() == null ? null : inbound.eventTriggerSource().prettyLabel();
        String classification = inbound.eventClassification() == null ? null : inbound.eventClassification().prettyLabel();
        return new OnlyCatEvent(
                Instant.now(),
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
    }

    private boolean isFinalClassification(OnlyCatInboundEvent inbound) {
        OnlyCatEventClassification classification = inbound.eventClassification();
        return classification != null
                && classification != OnlyCatEventClassification.UNKNOWN_CLASS
                && classification != OnlyCatEventClassification.UNKNOWN;
    }

    private boolean isUserEventCreate(OnlyCatInboundEvent inbound) {
        return inbound != null
                && "userEventUpdate".equals(inbound.eventName())
                && "create".equalsIgnoreCase(inbound.eventType());
    }

    private String eventKey(Integer eventId, String deviceId) {
        if (eventId == null || !StringUtils.hasText(deviceId)) {
            return null;
        }
        return eventId + ":" + deviceId;
    }

    private String hash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private Instant parseInstant(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (Exception ignore) {
            return null;
        }
    }

    @PreDestroy
    public void shutdown() {
        enrichmentExecutor.shutdownNow();
        pendingFallbackExecutor.shutdownNow();
    }

    private void schedulePendingFallback(String eventKey, int attempt) {
        pendingFallbackExecutor.schedule(() -> {
            Optional<OnlyCatEvent> pending = pendingEventCache.pop(eventKey);
            if (pending.isEmpty()) {
                return;
            }
            OnlyCatEvent event = tryEnrichPending(pending.get());
            if (!isEnrichedEvent(event) && attempt < 2) {
                pendingEventCache.put(eventKey, event);
                schedulePendingFallback(eventKey, attempt + 1);
                return;
            }
            String dedupeKey = dedupeKey(event.eventId(), event.deviceId(), event.eventTimeUtc());
            log.info("Appending pending event after timeout eventId={} deviceId={}", event.eventId(), event.deviceId());
            appendIfNotSeen(event, dedupeKey);
        }, pendingFallbackDelayMs, TimeUnit.MILLISECONDS);
    }

    private OnlyCatEvent tryEnrichPending(OnlyCatEvent event) {
        if (event == null || isEnrichedEvent(event)) {
            return event;
        }
        try {
            List<String> rfidCodes = lastSeenRfidEventsCache.resolveRfidCodes(event.deviceId(), event.eventId());
            List<String> labels = resolveLabels(rfidCodes);
            String rfidCode = firstNonBlank(rfidCodes);
            if (StringUtils.hasText(rfidCode) || (labels != null && !labels.isEmpty())) {
                return new OnlyCatEvent(
                        event.ingestedAtUtc(),
                        event.eventTimeUtc(),
                        event.eventName(),
                        event.eventType(),
                        event.eventId(),
                        event.eventTriggerSource(),
                        event.eventClassification(),
                        event.globalId(),
                        event.deviceId(),
                        StringUtils.hasText(rfidCode) ? rfidCode : event.rfidCode(),
                        (labels != null && !labels.isEmpty()) ? labels : event.catLabels()
                );
            }
        } catch (Exception e) {
            log.debug("Pending fallback enrichment failed; keeping pending event", e);
        }
        return event;
    }

    private boolean isEnrichedEvent(OnlyCatEvent event) {
        return event != null
                && (StringUtils.hasText(event.rfidCode())
                || (event.catLabels() != null && !event.catLabels().isEmpty()));
    }

    private void appendIfNotSeen(OnlyCatEvent event, String key) {
        if (dedupeCache.seen(key)) {
            log.debug("Duplicate event suppressed for key {}", key);
            return;
        }
        eventRepository.append(event);
        checkpointStore.record(event);
        log.info("Appended event type={} cats={} device={} time={}", event.eventType(), catLabelsSummary(event.catLabels()), event.deviceId(), event.eventTimeUtc());
    }

    private String dedupeKey(OnlyCatInboundEvent inbound, OnlyCatEvent event) {
        return dedupeKey(inbound.eventId(), inbound.deviceId(), event.eventTimeUtc());
    }

    private String dedupeKey(Integer eventId, String deviceId, Instant eventTimeUtc) {
        if (eventId != null && StringUtils.hasText(deviceId)) {
            return hash(eventId + ":" + deviceId);
        }
        return hash((eventId == null ? "" : eventId) + ":" + deviceId + ":" +
                (eventTimeUtc != null ? eventTimeUtc.toEpochMilli() : ""));
    }
}
