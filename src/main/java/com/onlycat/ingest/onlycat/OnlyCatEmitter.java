package com.onlycat.ingest.onlycat;

import com.onlycat.ingest.model.OnlyCatRfidEvent;
import com.onlycat.ingest.model.OnlyCatRfidProfile;

import java.util.List;
import java.util.Optional;

public interface OnlyCatEmitter {
    List<OnlyCatRfidEvent> requestLastSeenRfidCodesByDevice(String deviceId, int limit);

    Optional<OnlyCatRfidProfile> requestRfidProfile(String rfidCode);
}
