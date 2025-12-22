package com.onlycat.ingest.service;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple LRU cache used to suppress duplicate events after reconnects.
 */
public class DedupeCache {
    private final Map<String, Boolean> cache;

    public DedupeCache(int maxEntries) {
        cache = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > maxEntries;
            }
        });
    }

    /**
     * @return true if the key has already been seen (and is left in the cache), false otherwise.
     */
    public boolean seen(String key) {
        if (key == null || key.isBlank()) {
            return false;
        }
        synchronized (cache) {
            if (cache.containsKey(key)) {
                return true;
            }
            cache.put(key, Boolean.TRUE);
            return false;
        }
    }

    int size() {
        synchronized (cache) {
            return cache.size();
        }
    }
}
