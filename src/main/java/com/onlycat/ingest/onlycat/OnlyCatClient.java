package com.onlycat.ingest.onlycat;

public interface OnlyCatClient {
    void connect();

    void disconnect();

    boolean isConnected();
}
