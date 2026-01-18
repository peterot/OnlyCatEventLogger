package com.onlycat.ingest.service;

import com.onlycat.ingest.model.OnlyCatEvent;
import com.onlycat.ingest.sheets.GoogleSheetsAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

@Service
public class EventIngestService {
    private static final Logger log = LoggerFactory.getLogger(EventIngestService.class);
    private final GoogleSheetsAppender sheetsAppender;
    private final DedupeCache dedupeCache;
    private final OnlyCatEventMapper mapper;

    public EventIngestService(GoogleSheetsAppender sheetsAppender) {
        this.sheetsAppender = sheetsAppender;
        this.dedupeCache = new DedupeCache(512);
        this.mapper = new OnlyCatEventMapper();
    }

    public void handleInbound(String eventName, Object[] args) {
        OnlyCatEvent event = mapper.map(eventName, args);
        String key = hash(eventName + ":" + event.rawJson() + ":" + (event.eventTimeUtc() != null ? event.eventTimeUtc().toEpochMilli() : ""));
        if (dedupeCache.seen(key)) {
            log.debug("Duplicate event suppressed for key {}", key);
            return;
        }
        sheetsAppender.append(event);
        log.info("Appended event type={} cat={} device={} time={}", event.eventType(), event.catId(), event.deviceId(), event.eventTimeUtc());
    }

    public OnlyCatEventMapper getMapper() {
        return mapper;
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
}
