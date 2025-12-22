package com.onlycat.ingest.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "sheets")
public class SheetsProperties {

    @NotBlank
    private String credentialsPath;

    @NotBlank
    private String spreadsheetId;

    @NotBlank
    private String sheetName;

    /**
     * Append range like "Sheet1!A1". Defaults to sheetName + "!A1" when not provided.
     */
    private String appendRange;

    public String getCredentialsPath() {
        return credentialsPath;
    }

    public void setCredentialsPath(String credentialsPath) {
        this.credentialsPath = credentialsPath;
    }

    public String getSpreadsheetId() {
        return spreadsheetId;
    }

    public void setSpreadsheetId(String spreadsheetId) {
        this.spreadsheetId = spreadsheetId;
    }

    public String getSheetName() {
        return sheetName;
    }

    public void setSheetName(String sheetName) {
        this.sheetName = sheetName;
    }

    public String getAppendRange() {
        if (appendRange == null || appendRange.isBlank()) {
            return sheetName + "!A1";
        }
        return appendRange;
    }

    public void setAppendRange(String appendRange) {
        this.appendRange = appendRange;
    }
}
