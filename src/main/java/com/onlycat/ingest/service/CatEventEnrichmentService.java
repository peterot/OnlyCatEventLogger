package com.onlycat.ingest.service;

import com.onlycat.ingest.onlycat.OnlyCatEmitter;
import com.onlycat.ingest.model.OnlyCatEventClassification;
import com.onlycat.ingest.model.OnlyCatEventTriggerSource;
import com.onlycat.ingest.model.OnlyCatInboundEvent;
import com.onlycat.ingest.model.OnlyCatRfidEvent;
import com.onlycat.ingest.model.OnlyCatRfidProfile;
import jakarta.annotation.PreDestroy;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.Arrays;
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

    private final EventIngestService ingestService;
    private final OnlyCatEmitter emitter;

    // Enrichment: RFID -> cat label (name) cache and pending user events awaiting enrichment.
    private final Map<String, String> rfidLabelCache = new ConcurrentHashMap<>();
    private final Map<Integer, PendingUserEvent> pendingUserEvents = new ConcurrentHashMap<>();

    private final ScheduledExecutorService enrichmentScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "onlycat-enrichment");
        t.setDaemon(true);
        return t;
    });

    public CatEventEnrichmentService(EventIngestService ingestService, OnlyCatEmitter emitter) {
        this.ingestService = ingestService;
        this.emitter = emitter;
    }

    @EventListener
    public void onInbound(OnlyCatInboundEvent inbound) {
        handleAnyEvent(inbound);
    }

    private void handleAnyEvent(OnlyCatInboundEvent inbound) {
        String event = inbound.eventName();
        Object[] args = inbound.args();
        // Special-case: enrich userEventUpdate(create) with cat identity (RFID profile label).
        if ("userEventUpdate".equals(event)) {
            String type = inbound.eventType();
            Integer eventId = inbound.eventId();
            String deviceId = inbound.deviceId();
            JSONObject obj = coerceToJsonObject(firstPayload(args));
            if (obj != null) {
                try {
                    // Only enrich create events with a known deviceId/eventId.
                    if (eventId != null && StringUtils.hasText(deviceId) && "create".equalsIgnoreCase(type)) {
                        PendingUserEvent pending = new PendingUserEvent(eventId, deviceId, obj);
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
                                    JSONObject decorated = decorateUserEventUpdate(new JSONObject(p.original.toString()));
                                    ingestService.handleInbound(event, new Object[]{decorated});
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
        }

        // For userEventUpdate, decorate with friendly classification/trigger strings even if we can't RFID-enrich.
        if ("userEventUpdate".equals(event)) {
            JSONObject obj = coerceToJsonObject(firstPayload(args));
            if (obj != null) {
                JSONObject decorated = decorateUserEventUpdate(new JSONObject(obj.toString()));
                Object[] newArgs = Arrays.copyOf(args, args.length);
                newArgs[0] = decorated;
                ingestService.handleInbound(event, newArgs);
                return;
            }
        }

        ingestService.handleInbound(event, args);
    }

    private JSONObject coerceToJsonObject(Object o) {
        if (o == null) return null;
        if (o instanceof JSONObject jo) return jo;
        if (o instanceof Map<?, ?> map) return new JSONObject(map);
        if (o instanceof String s) {
            String t = s.trim();
            if (t.startsWith("{")) {
                try {
                    return new JSONObject(t);
                } catch (Exception ignore) {
                    return null;
                }
            }
        }
        return null;
    }

    private Object firstPayload(Object[] args) {
        if (args == null || args.length == 0) {
            return null;
        }
        for (Object arg : args) {
            if (arg != null) {
                return arg;
            }
        }
        return null;
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

        // Attach enrichment fields to the event JSON.
        JSONObject enriched = decorateUserEventUpdate(new JSONObject(removed.original.toString()));
        enriched.put("rfidCode", rfidCode);
        enriched.put("catLabel", label);
        JSONObject cat = enriched.optJSONObject("cat");
        if (cat == null) {
            cat = new JSONObject();
        }
        cat.put("rfidCode", rfidCode);
        cat.put("label", label);
        enriched.put("cat", cat);

        log.info("Enrichment complete for eventId={} deviceId={} catLabel={}", removed.eventId, removed.deviceId, label);
        ingestService.handleInbound("userEventUpdate", new Object[]{enriched});
        removed.ingested = true;
    }

    private static class PendingUserEvent {
        final int eventId;
        final String deviceId;
        final JSONObject original;
        volatile boolean ingested = false;
        volatile ScheduledFuture<?> fallback;
        volatile ScheduledFuture<?> retry;
        volatile int retryCount = 0;

        PendingUserEvent(int eventId, String deviceId, JSONObject original) {
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

    /**
     * Add friendly fields for eventClassification/eventTriggerSource.
     * Based on OnlyCat's published Door Policy schema enums.
     */
    private JSONObject decorateUserEventUpdate(JSONObject userEventUpdate) {
        if (userEventUpdate == null) return null;

        JSONObject body = userEventUpdate.optJSONObject("body");
        if (body == null) {
            body = new JSONObject();
            userEventUpdate.put("body", body);
        }

        // Values may live top-level or under body; prefer body if present.
        Integer trigger = readInt(body, "eventTriggerSource");
        if (trigger == null) trigger = readInt(userEventUpdate, "eventTriggerSource");

        Integer classification = readInt(body, "eventClassification");
        if (classification == null) classification = readInt(userEventUpdate, "eventClassification");

        if (trigger != null) {
            String triggerCode = OnlyCatEventTriggerSource.formatCode(trigger);
            String triggerPretty = OnlyCatEventTriggerSource.formatLabel(trigger);
            userEventUpdate.put("eventTriggerSourceCode", triggerCode);
            userEventUpdate.put("eventTriggerSourceLabel", triggerPretty);
            body.put("eventTriggerSourceCode", triggerCode);
            body.put("eventTriggerSourceLabel", triggerPretty);
        }

        if (classification != null) {
            String classCode = OnlyCatEventClassification.formatCode(classification);
            String classPretty = OnlyCatEventClassification.formatLabel(classification);
            userEventUpdate.put("eventClassificationCode", classCode);
            userEventUpdate.put("eventClassificationLabel", classPretty);
            body.put("eventClassificationCode", classCode);
            body.put("eventClassificationLabel", classPretty);

            // Convenience booleans for spreadsheets.
            userEventUpdate.put("isEntry", classification == 2);
            userEventUpdate.put("isExit", classification == 3);
            userEventUpdate.put("isPrey", classification == 4);
            body.put("isEntry", classification == 2);
            body.put("isExit", classification == 3);
            body.put("isPrey", classification == 4);
        }

        return userEventUpdate;
    }

    private Integer readInt(JSONObject obj, String key) {
        if (obj == null || key == null) return null;
        Object v = obj.opt(key);
        if (v == null) return null;
        if (v instanceof Number n) return n.intValue();
        if (v instanceof String s) {
            try {
                return Integer.parseInt(s);
            } catch (Exception ignore) {
                return null;
            }
        }
        return null;
    }


    @PreDestroy
    public void shutdown() {
        enrichmentScheduler.shutdownNow();
    }
}
