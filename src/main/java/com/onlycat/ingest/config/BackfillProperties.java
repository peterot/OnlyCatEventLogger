package com.onlycat.ingest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "onlycat.backfill")
public class BackfillProperties {
    private boolean enabled = false;
    private int limit = 500;
    private String checkpointPath = "onlycat-checkpoint.json";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public String getCheckpointPath() {
        return checkpointPath;
    }

    public void setCheckpointPath(String checkpointPath) {
        this.checkpointPath = checkpointPath;
    }

}
