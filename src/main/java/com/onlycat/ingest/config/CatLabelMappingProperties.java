package com.onlycat.ingest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "cat-label-mapping")
public class CatLabelMappingProperties {
    private Map<String, String> aliases = new LinkedHashMap<>();

    public Map<String, String> getAliases() {
        return aliases;
    }

    public void setAliases(Map<String, String> aliases) {
        if (aliases == null) {
            this.aliases = new LinkedHashMap<>();
            return;
        }
        this.aliases = new LinkedHashMap<>(aliases);
    }
}
