package com.onlycat.ingest.model;

public enum OnlyCatEventClassification {
    NONE(0, "NONE", "None"),
    MOVEMENT(1, "MOVEMENT", "Movement"),
    ENTRY(2, "ENTRY", "Entry"),
    EXIT(3, "EXIT", "Exit"),
    PREY(4, "PREY", "Prey"),
    UNKNOWN(-1, "UNKNOWN", "Unknown");

    private final int code;
    private final String codeLabel;
    private final String prettyLabel;

    OnlyCatEventClassification(int code, String codeLabel, String prettyLabel) {
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

    public static OnlyCatEventClassification fromCode(Integer code) {
        if (code == null) {
            return null;
        }
        for (OnlyCatEventClassification value : values()) {
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
        OnlyCatEventClassification value = fromCode(code);
        if (value == UNKNOWN) {
            return "UNKNOWN(" + code + ")";
        }
        return value.codeLabel;
    }

    public static String formatLabel(Integer code) {
        if (code == null) {
            return null;
        }
        OnlyCatEventClassification value = fromCode(code);
        if (value == UNKNOWN) {
            return "Unknown (" + code + ")";
        }
        return value.prettyLabel;
    }
}
