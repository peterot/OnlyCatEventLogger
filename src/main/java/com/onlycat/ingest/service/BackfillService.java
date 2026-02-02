package com.onlycat.ingest.service;

import com.onlycat.ingest.config.BackfillProperties;
import com.onlycat.ingest.model.OnlyCatInboundEvent;
import com.onlycat.ingest.onlycat.OnlyCatClient;
import com.onlycat.ingest.onlycat.OnlyCatEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Component
public class BackfillService {
    private static final Logger log = LoggerFactory.getLogger(BackfillService.class);

    private final BackfillProperties properties;
    private final BackfillCheckpointStore checkpointStore;
    private final OnlyCatEmitter emitter;
    private final OnlyCatClient client;
    private final ApplicationEventPublisher publisher;

    public BackfillService(BackfillProperties properties,
                           BackfillCheckpointStore checkpointStore,
                           OnlyCatEmitter emitter,
                           OnlyCatClient client,
                           ApplicationEventPublisher publisher) {
        this.properties = properties;
        this.checkpointStore = checkpointStore;
        this.emitter = emitter;
        this.client = client;
        this.publisher = publisher;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        if (!properties.isEnabled()) {
            return;
        }
        Thread worker = new Thread(this::runBackfill, "onlycat-backfill");
        worker.setDaemon(true);
        worker.start();
    }

    private void runBackfill() {
        List<String> deviceIds = emitter.requestDeviceIds();
        if (deviceIds == null || deviceIds.isEmpty()) {
            log.warn("Backfill enabled but no deviceIds available; skipping");
            return;
        }
        waitForConnection();
        for (String deviceId : deviceIds) {
            if (!StringUtils.hasText(deviceId)) {
                continue;
            }
            Optional<BackfillCheckpointStore.CheckpointEntry> entry = checkpointStore.getEntry(deviceId);
            Optional<Instant> lastEventTime = entry.flatMap(e -> parseInstant(e.eventTimeUtc));
            Integer lastEventId = entry.map(e -> e.eventId).orElse(null);
            List<OnlyCatInboundEvent> events = emitter.requestDeviceEvents(deviceId, null, null, properties.getLimit());
            if (events.isEmpty()) {
                log.info("Backfill: no events returned for deviceId={}", deviceId);
                continue;
            }
            List<OnlyCatInboundEvent> filtered = events.stream()
                    .filter(event -> shouldInclude(event, lastEventTime, lastEventId))
                    .sorted(Comparator.comparing(this::eventTimeOrMin)
                            .thenComparing(event -> event.eventId() == null ? 0 : event.eventId()))
                    .toList();
            if (filtered.isEmpty()) {
                log.info("Backfill: no new events after checkpoint for deviceId={}", deviceId);
                continue;
            }
            log.info("Backfill: publishing {} event(s) for deviceId={}", filtered.size(), deviceId);
            for (OnlyCatInboundEvent event : filtered) {
                publisher.publishEvent(event);
            }
        }
    }

    private void waitForConnection() {
        for (int i = 0; i < 20; i++) {
            if (client.isConnected()) {
                return;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        log.warn("Backfill proceeding without confirmed socket connection");
    }

    private boolean shouldInclude(OnlyCatInboundEvent event,
                                  Optional<Instant> lastEventTime,
                                  Integer lastEventId) {
        if (event == null) {
            return false;
        }
        if (lastEventTime.isEmpty() && lastEventId == null) {
            return true;
        }
        Instant eventTime = eventTime(event).orElse(null);
        if (eventTime != null && lastEventTime.isPresent()) {
            int compare = eventTime.compareTo(lastEventTime.get());
            if (compare > 0) {
                return true;
            }
            if (compare < 0) {
                return false;
            }
            if (lastEventId == null || event.eventId() == null) {
                return false;
            }
            return event.eventId() > lastEventId;
        }
        if (lastEventId != null && event.eventId() != null) {
            return event.eventId() > lastEventId;
        }
        return false;
    }

    private Optional<Instant> eventTime(OnlyCatInboundEvent event) {
        if (event == null || !StringUtils.hasText(event.timestamp())) {
            return Optional.empty();
        }
        return parseInstant(event.timestamp());
    }

    private Instant eventTimeOrMin(OnlyCatInboundEvent event) {
        return eventTime(event).orElse(Instant.EPOCH);
    }

    private Optional<Instant> parseInstant(String value) {
        if (!StringUtils.hasText(value)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Instant.parse(value));
        } catch (Exception ignore) {
            return Optional.empty();
        }
    }
}
