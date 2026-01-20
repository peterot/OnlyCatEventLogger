package com.onlycat.ingest.service;

import com.onlycat.ingest.model.OnlyCatEvent;

public interface CatEventRepository {
    void append(OnlyCatEvent event);
}
