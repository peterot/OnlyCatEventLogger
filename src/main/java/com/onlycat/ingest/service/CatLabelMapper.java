package com.onlycat.ingest.service;

import com.onlycat.ingest.config.CatLabelMappingProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class CatLabelMapper {
    private static final Logger log = LoggerFactory.getLogger(CatLabelMapper.class);

    private final Map<String, String> aliases;

    public CatLabelMapper(CatLabelMappingProperties properties) {
        this.aliases = properties.getAliases();
    }

    public String mapFinalLabel(List<String> labels) {
        if (labels == null || labels.isEmpty()) {
            return null;
        }
        Set<String> mapped = new LinkedHashSet<>();
        for (String label : labels) {
            if (!StringUtils.hasText(label)) {
                continue;
            }
            String resolved = aliases.getOrDefault(label, label);
            if (StringUtils.hasText(resolved)) {
                mapped.add(resolved);
            }
        }
        if (mapped.isEmpty()) {
            return null;
        }
        if (mapped.size() > 1) {
            log.warn("Multiple mapped cat labels {} from raw labels {}", mapped, labels);
        }
        return mapped.iterator().next();
    }
}
