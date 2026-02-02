package com.onlycat.ingest.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(value = "classpath:cat-label-mapping.yml", factory = YamlPropertySourceFactory.class)
public class CatLabelMappingConfig {
}
