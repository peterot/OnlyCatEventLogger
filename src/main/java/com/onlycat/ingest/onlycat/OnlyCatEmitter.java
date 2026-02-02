package com.onlycat.ingest.onlycat;

import com.onlycat.ingest.model.OnlyCatInboundEvent;
import com.onlycat.ingest.model.OnlyCatRfidEvent;
import com.onlycat.ingest.model.OnlyCatRfidProfile;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface OnlyCatEmitter {
    List<OnlyCatRfidEvent> requestLastSeenRfidCodesByDevice(String deviceId, int limit);

    List<OnlyCatInboundEvent> requestDeviceEvents(String deviceId, Instant from, Instant to, int limit);

    List<String> requestDeviceIds();

    Optional<OnlyCatRfidProfile> requestRfidProfile(String rfidCode);
}
