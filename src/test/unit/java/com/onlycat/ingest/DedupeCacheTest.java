package com.onlycat.ingest;

import com.onlycat.ingest.service.DedupeCache;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DedupeCacheTest {

    @Test
    void detectsDuplicates() {
        DedupeCache cache = new DedupeCache(5);
        assertThat(cache.seen("abc")).isFalse();
        assertThat(cache.seen("abc")).isTrue();
        assertThat(cache.seen("abc")).isTrue();
        assertThat(cache.size()).isEqualTo(1);
    }

    @Test
    void evictsOldEntries() {
        DedupeCache cache = new DedupeCache(2);
        cache.seen("first");
        cache.seen("second");
        cache.seen("third"); // should evict "first"

        assertThat(cache.seen("first")).isFalse();
        assertThat(cache.seen("second")).isTrue();
        assertThat(cache.seen("third")).isTrue();
    }
}
