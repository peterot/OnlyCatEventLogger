package com.onlycat.ingest.config;

import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.springframework.core.env.MapPropertySource;

public class YamlPropertySourceFactory implements PropertySourceFactory {
    @Override
    public PropertySource<?> createPropertySource(String name, EncodedResource resource) throws IOException {
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
        String sourceName = name != null ? name : resource.getResource().getFilename();
        List<PropertySource<?>> sources = loader.load(sourceName, resource.getResource());
        return sources.isEmpty() ? new MapPropertySource(sourceName, Map.of()) : sources.get(0);
    }
}
