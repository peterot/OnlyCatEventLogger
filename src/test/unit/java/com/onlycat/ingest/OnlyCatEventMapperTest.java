package com.onlycat.ingest;

import com.onlycat.ingest.model.OnlyCatEvent;
import com.onlycat.ingest.service.OnlyCatEventMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OnlyCatEventMapperTest {

    private final OnlyCatEventMapper mapper = new OnlyCatEventMapper();

    @Test
    void mapsCommonFields() {
        Map<String, Object> cat = new HashMap<>();
        cat.put("name", "Mochi");
        cat.put("id", "cat-123");

        Map<String, Object> device = new HashMap<>();
        device.put("name", "Front Door");
        device.put("id", "dev-1");

        Map<String, Object> payload = new HashMap<>();
        payload.put("eventType", "cat_seen");
        payload.put("timestamp", Instant.parse("2024-01-01T10:15:30Z").toEpochMilli());
        payload.put("direction", "in");
        payload.put("cat", cat);
        payload.put("device", device);
        payload.put("outcome", "allowed");
        payload.put("preyDetected", false);

        OnlyCatEvent event = mapper.map("cat_seen", new Object[]{payload});

        assertThat(event.eventType()).isEqualTo("cat_seen");
        assertThat(event.direction()).isEqualTo("in");
        assertThat(event.catName()).isEqualTo("Mochi");
        assertThat(event.deviceName()).isEqualTo("Front Door");
        assertThat(event.outcome()).isEqualTo("allowed");
        assertThat(event.eventTimeUtc()).isEqualTo(Instant.parse("2024-01-01T10:15:30Z"));
        assertThat(event.rawJson()).contains("cat_seen");
    }

    @Test
    void truncatesLargeRawJson() {
        String large = "x".repeat(OnlyCatEventMapper.RAW_JSON_LIMIT + 10);
        OnlyCatEvent event = mapper.map("test", new Object[]{large});

        assertThat(event.rawJson()).hasSizeLessThanOrEqualTo(OnlyCatEventMapper.RAW_JSON_LIMIT + 20);
        assertThat(event.rawJson()).contains("truncated");
    }
}
