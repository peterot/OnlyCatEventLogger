package com.onlycat.ingest;

import com.onlycat.ingest.config.BackfillProperties;
import com.onlycat.ingest.config.CatLabelMappingProperties;
import com.onlycat.ingest.config.OnlyCatProperties;
import com.onlycat.ingest.config.SheetsProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({OnlyCatProperties.class, SheetsProperties.class, CatLabelMappingProperties.class, BackfillProperties.class})
public class OnlyCatIngestApplication {

    public static void main(String[] args) {
        SpringApplication.run(OnlyCatIngestApplication.class, args);
    }
}
