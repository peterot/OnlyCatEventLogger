package com.onlycat.ingest.sheets;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.onlycat.ingest.config.SheetsProperties;
import com.onlycat.ingest.model.OnlyCatEvent;
import com.onlycat.ingest.service.CatLabelMapper;
import com.onlycat.ingest.service.CatEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

@Component
public class GoogleSheetsAppender implements CatEventRepository {
    private static final Logger log = LoggerFactory.getLogger(GoogleSheetsAppender.class);
    private static final String VALUE_INPUT_OPTION = "RAW"; // Keep payload unmodified
    private static final List<Object> HEADER = List.of(
            "ingested_at_utc",
            "event_time_utc",
            "event_name",
            "event_type",
            "event_id",
            "event_trigger_source",
            "event_classification",
            "global_id",
            "device_id",
            "rfid_code",
            "cat_label"
    );
    private final Sheets sheets;
    private final SheetsProperties properties;
    private final CatLabelMapper catLabelMapper;
    private volatile boolean headerEnsured = false;

    public GoogleSheetsAppender(SheetsProperties properties, CatLabelMapper catLabelMapper) {
        this.properties = properties;
        this.catLabelMapper = catLabelMapper;
        this.sheets = buildSheetsClient(properties);
    }

    @Override
    public synchronized void append(OnlyCatEvent event) {
        ensureHeader();
        String mappedLabel = catLabelMapper.mapFinalLabel(event.catLabels());
        ValueRange body = new ValueRange().setValues(List.of(event.toRow(mappedLabel)));
        try {
            sheets.spreadsheets().values()
                    .append(properties.getSpreadsheetId(), properties.getAppendRange(), body)
                    .setValueInputOption(VALUE_INPUT_OPTION)
                    .setInsertDataOption("INSERT_ROWS")
                    .execute();
        } catch (IOException e) {
            throw new RuntimeException("Failed to append row to Sheets", e);
        }
    }

    private void ensureHeader() {
        if (headerEnsured) {
            return;
        }
        try {
            ValueRange existing = sheets.spreadsheets().values()
                    .get(properties.getSpreadsheetId(), properties.getAppendRange())
                    .setMajorDimension("ROWS")
                    .execute();
            if (existing.getValues() == null || existing.getValues().isEmpty()) {
                ValueRange headerRow = new ValueRange().setValues(List.of(HEADER));
                sheets.spreadsheets().values()
                        .append(properties.getSpreadsheetId(), properties.getAppendRange(), headerRow)
                        .setValueInputOption(VALUE_INPUT_OPTION)
                        .setInsertDataOption("INSERT_ROWS")
                        .execute();
                log.info("Wrote header row to sheet {}", properties.getSheetName());
            }
            headerEnsured = true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to ensure sheet header", e);
        }
    }

    private Sheets buildSheetsClient(SheetsProperties properties) {
        try {
            String credentialsPath = properties.getCredentialsPath();
            java.io.File credentialsFile = new java.io.File(credentialsPath);
            if (!credentialsFile.exists()) {
                throw new IllegalStateException("Google Sheets credentials file not found at " + credentialsPath);
            }

            HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
            GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsFile))
                    .createScoped(List.of(SheetsScopes.SPREADSHEETS));

            return new Sheets.Builder(transport, GsonFactory.getDefaultInstance(), new HttpCredentialsAdapter(credentials))
                    .setApplicationName("OnlyCatEventLogger")
                    .build();
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("Unable to initialize Google Sheets client", e);
        }
    }
}
