package com.onlycat.ingest.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlycat.ingest.config.BackfillProperties;
import com.onlycat.ingest.model.OnlyCatEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class BackfillCheckpointStore {
    private static final Logger log = LoggerFactory.getLogger(BackfillCheckpointStore.class);

    private final BackfillProperties properties;
    private final ObjectMapper objectMapper;
    private final Object lock = new Object();
    private final Path checkpointPath;
    private CheckpointData data;

    public BackfillCheckpointStore(BackfillProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.checkpointPath = Path.of(properties.getCheckpointPath()).toAbsolutePath();
        this.data = load();
        log.info("Backfill checkpoint path={}", checkpointPath);
    }

    public Optional<CheckpointEntry> getEntry(String deviceId) {
        if (!StringUtils.hasText(deviceId)) {
            return Optional.empty();
        }
        synchronized (lock) {
            if (data.devices == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(data.devices.get(deviceId));
        }
    }

    public Optional<Instant> getLastEventTime(String deviceId) {
        return getEntry(deviceId).flatMap(entry -> parseInstant(entry.eventTimeUtc));
    }

    public void record(OnlyCatEvent event) {
        if (!properties.isEnabled()) {
            return;
        }
        if (event == null || !StringUtils.hasText(event.deviceId())) {
            return;
        }
        Instant eventTime = event.eventTimeUtc();
        if (eventTime == null) {
            return;
        }
        synchronized (lock) {
            if (data.devices == null) {
                data.devices = new HashMap<>();
            }
            CheckpointEntry existing = data.devices.get(event.deviceId());
            if (!shouldUpdate(existing, eventTime, event.eventId())) {
                return;
            }
            CheckpointEntry updated = new CheckpointEntry();
            updated.eventTimeUtc = eventTime.toString();
            updated.eventId = event.eventId();
            data.devices.put(event.deviceId(), updated);
            data.updatedAtUtc = Instant.now().toString();
            persist();
        }
    }

    private boolean shouldUpdate(CheckpointEntry existing, Instant eventTime, Integer eventId) {
        if (existing == null) {
            return true;
        }
        Optional<Instant> existingTime = parseInstant(existing.eventTimeUtc);
        if (existingTime.isEmpty()) {
            return true;
        }
        int compare = eventTime.compareTo(existingTime.get());
        if (compare > 0) {
            return true;
        }
        if (compare < 0) {
            return false;
        }
        if (existing.eventId == null || eventId == null) {
            return false;
        }
        return eventId > existing.eventId;
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

    private CheckpointData load() {
        if (!Files.exists(checkpointPath)) {
            return new CheckpointData();
        }
        try {
            return objectMapper.readValue(checkpointPath.toFile(), CheckpointData.class);
        } catch (IOException ex) {
            log.warn("Failed to read checkpoint file {}; starting fresh", checkpointPath, ex);
            return new CheckpointData();
        }
    }

    private void persist() {
        try {
            Path parent = checkpointPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = checkpointPath.resolveSibling(checkpointPath.getFileName() + ".tmp");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), data);
            Files.move(tmp, checkpointPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {
            log.warn("Failed to persist checkpoint file {}", checkpointPath, ex);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class CheckpointData {
        public String updatedAtUtc;
        public Map<String, CheckpointEntry> devices = new HashMap<>();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class CheckpointEntry {
        public String eventTimeUtc;
        public Integer eventId;
    }
}
