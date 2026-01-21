package com.onlycat.ingest.model;

public enum OnlyCatEventTriggerSource {
    MANUAL(0, "MANUAL", "Manual"),
    REMOTE(1, "REMOTE", "Remote"),
    INDOOR_MOTION(2, "EXIT_ALLOWED", "Exit Allowed"),
    OUTDOOR_MOTION(3, "ENTRY_ALLOWED", "Entry Allowed"),
    UNKNOWN(-1, "UNKNOWN", "Unknown");

    private final int code;
    private final String codeLabel;
    private final String prettyLabel;

    OnlyCatEventTriggerSource(int code, String codeLabel, String prettyLabel) {
        this.code = code;
        this.codeLabel = codeLabel;
        this.prettyLabel = prettyLabel;
    }

    public int code() {
        return code;
    }

    public String codeLabel() {
        return codeLabel;
    }

    public String prettyLabel() {
        return prettyLabel;
    }

    public static OnlyCatEventTriggerSource fromCode(Integer code) {
        if (code == null) {
            return null;
        }
        for (OnlyCatEventTriggerSource value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return UNKNOWN;
    }

    public static String formatCode(Integer code) {
        if (code == null) {
            return null;
        }
        OnlyCatEventTriggerSource value = fromCode(code);
        if (value == UNKNOWN) {
            return "UNKNOWN(" + code + ")";
        }
        return value.codeLabel;
    }

    public static String formatLabel(Integer code) {
        if (code == null) {
            return null;
        }
        OnlyCatEventTriggerSource value = fromCode(code);
        if (value == UNKNOWN) {
            return "Unknown (" + code + ")";
        }
        return value.prettyLabel;
    }
}
