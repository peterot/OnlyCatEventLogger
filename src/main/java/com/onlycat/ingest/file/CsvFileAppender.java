package com.onlycat.ingest.file;

import com.onlycat.ingest.config.FileOutputProperties;
import com.onlycat.ingest.model.OnlyCatEvent;
import com.onlycat.ingest.service.CatEventRepository;
import com.onlycat.ingest.service.CatLabelMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@ConditionalOnProperty(name = "output.mode", havingValue = "file")
public class CsvFileAppender implements CatEventRepository {

    private static final Logger log = LoggerFactory.getLogger(CsvFileAppender.class);

    private static final List<String> FULL_HEADER = List.of(
            "ingested_at_utc", "event_time_utc", "event_name", "event_type", "event_id",
            "event_trigger_source", "event_classification", "global_id", "device_id",
            "rfid_code", "cat_label"
    );
    private static final List<String> LLM_HEADER = List.of(
            "event_time_utc", "event_time_local", "direction", "event_classification", "cat_label"
    );
    private static final Map<String, String> DIRECTION_MAP = Map.of(
            "Exit Allowed", "out",
            "Entry Allowed", "in",
            "Remote", "remote",
            "Manual", "manual"
    );
    private static final DateTimeFormatter LOCAL_TIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Path fullPath;
    private final Path llmPath;
    private final CatLabelMapper catLabelMapper;
    private final ZoneId zone;
    private boolean fullHeaderWritten;
    private boolean llmHeaderWritten;

    public CsvFileAppender(FileOutputProperties properties, CatLabelMapper catLabelMapper) {
        this.fullPath = Path.of(properties.getPath());
        this.llmPath = Path.of(properties.getLlmPath());
        this.catLabelMapper = catLabelMapper;
        this.zone = ZoneId.of(properties.getTimezone());
        this.fullHeaderWritten = hasContent(fullPath);
        this.llmHeaderWritten = hasContent(llmPath);
        log.info("File output mode enabled — full: {}, llm: {}",
                fullPath.toAbsolutePath(), llmPath.toAbsolutePath());
    }

    @Override
    public synchronized void append(OnlyCatEvent event) {
        String mappedLabel = catLabelMapper.mapFinalLabel(event.catLabels());
        appendFull(event, mappedLabel);
        appendLlm(event, mappedLabel);
    }

    private void appendFull(OnlyCatEvent event, String mappedLabel) {
        try (var writer = Files.newBufferedWriter(fullPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            if (!fullHeaderWritten) {
                writer.write(toCsvRow(FULL_HEADER));
                writer.newLine();
                fullHeaderWritten = true;
            }
            writer.write(toCsvRow(event.toRow(mappedLabel)));
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException("Failed to append event to full CSV file", e);
        }
    }

    private void appendLlm(OnlyCatEvent event, String mappedLabel) {
        try (var writer = Files.newBufferedWriter(llmPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            if (!llmHeaderWritten) {
                writer.write(toCsvRow(LLM_HEADER));
                writer.newLine();
                llmHeaderWritten = true;
            }
            writer.write(toCsvRow(toLlmRow(event, mappedLabel)));
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException("Failed to append event to LLM CSV file", e);
        }
    }

    private List<String> toLlmRow(OnlyCatEvent event, String mappedLabel) {
        String eventTimeUtc = event.eventTimeUtc() != null
                ? DateTimeFormatter.ISO_INSTANT.format(event.eventTimeUtc()) : "";
        String eventTimeLocal = event.eventTimeUtc() != null
                ? LOCAL_TIME_FMT.format(event.eventTimeUtc().atZone(zone)) : "";
        String direction = DIRECTION_MAP.getOrDefault(event.eventTriggerSource(), "unknown");
        return List.of(eventTimeUtc, eventTimeLocal, direction,
                nullToEmpty(event.eventClassification()), nullToEmpty(mappedLabel));
    }

    private String toCsvRow(List<?> fields) {
        return fields.stream()
                .map(f -> escapeCsvField(f == null ? "" : f.toString()))
                .collect(Collectors.joining(","));
    }

    private String escapeCsvField(String value) {
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private static String nullToEmpty(String value) {
        return value != null ? value : "";
    }

    private static boolean hasContent(Path path) {
        try {
            return Files.exists(path) && Files.size(path) > 0;
        } catch (IOException e) {
            return false;
        }
    }
}
