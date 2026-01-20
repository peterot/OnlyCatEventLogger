package com.onlycat.ingest.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.onlycat.ingest.onlycat.OnlyCatEmitter;
import com.onlycat.ingest.model.OnlyCatRfidProfile;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.Optional;

@Component
public class RfidLabelCache {
    private final OnlyCatEmitter emitter;
    private final Cache<String, Optional<String>> cache;

    public RfidLabelCache(OnlyCatEmitter emitter) {
        this.emitter = emitter;
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofHours(6))
                .maximumSize(1024)
                .build();
    }

    public Optional<String> getLabel(String rfidCode) {
        if (!StringUtils.hasText(rfidCode)) {
            return Optional.empty();
        }
        return cache.get(rfidCode, this::fetchLabel);
    }

    private Optional<String> fetchLabel(String rfidCode) {
        Optional<OnlyCatRfidProfile> profile = emitter.requestRfidProfile(rfidCode);
        if (profile.isPresent() && StringUtils.hasText(profile.get().label())) {
            return Optional.of(profile.get().label());
        }
        return Optional.empty();
    }
}
