package com.onlycat.ingest.service;

import com.onlycat.ingest.config.CatLabelMappingProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.text.Normalizer;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class CatLabelMapper {
    private static final Logger log = LoggerFactory.getLogger(CatLabelMapper.class);

    private final Map<String, String> aliases;
    private final Map<String, String> aliasesCompact;

    public CatLabelMapper(CatLabelMappingProperties properties) {
        this.aliases = normalizeAliases(properties.getAliases());
        this.aliasesCompact = buildCompactAliases(this.aliases);
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
            String normalized = normalizeLabel(label);
            if (!StringUtils.hasText(normalized)) {
                continue;
            }
            String resolved = aliases.get(normalized);
            if (!StringUtils.hasText(resolved)) {
                String compact = compactKey(normalized);
                resolved = aliasesCompact.getOrDefault(compact, normalized);
            }
            if (StringUtils.hasText(resolved)) {
                mapped.add(normalizeLabel(resolved));
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

    private Map<String, String> normalizeAliases(Map<String, String> source) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }
        java.util.Map<String, String> normalized = new java.util.HashMap<>();
        for (Map.Entry<String, String> entry : source.entrySet()) {
            String key = normalizeLabel(entry.getKey());
            String value = normalizeLabel(entry.getValue());
            if (StringUtils.hasText(key) && StringUtils.hasText(value)) {
                normalized.put(key, value);
            }
        }
        return java.util.Collections.unmodifiableMap(normalized);
    }

    private Map<String, String> buildCompactAliases(Map<String, String> source) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }
        java.util.Map<String, String> compact = new java.util.HashMap<>();
        for (Map.Entry<String, String> entry : source.entrySet()) {
            String key = compactKey(entry.getKey());
            String value = normalizeLabel(entry.getValue());
            if (StringUtils.hasText(key) && StringUtils.hasText(value)) {
                compact.put(key, value);
            }
        }
        return java.util.Collections.unmodifiableMap(compact);
    }

    private String normalizeLabel(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        String normalized = Normalizer.normalize(value, Normalizer.Form.NFKC);
        StringBuilder cleaned = new StringBuilder(normalized.length());
        for (int i = 0; i < normalized.length(); i++) {
            char ch = normalized.charAt(i);
            if (Character.isWhitespace(ch)) {
                cleaned.append(' ');
            } else if (!Character.isISOControl(ch)) {
                cleaned.append(ch);
            }
        }
        normalized = cleaned.toString().replaceAll("\\s+", " ").trim();
        return StringUtils.hasText(normalized) ? normalized : null;
    }

    private String compactKey(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        String normalized = normalizeLabel(value);
        if (!StringUtils.hasText(normalized)) {
            return null;
        }
        String compact = normalized.replaceAll("\\s+", "").toLowerCase();
        return StringUtils.hasText(compact) ? compact : null;
    }
}
