package com.onlycat.ingest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "output.file")
public class FileOutputProperties {

    private String path = "onlycat-events.csv";
    private String llmPath = "onlycat-events-llm.csv";
    private String timezone = "Europe/London";

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getLlmPath() {
        return llmPath;
    }

    public void setLlmPath(String llmPath) {
        this.llmPath = llmPath;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }
}
