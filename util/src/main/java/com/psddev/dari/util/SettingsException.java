package com.psddev.dari.util;

/**
 * Thrown when there is an error working with
 * {@linkplain Settings setting values}.
 */
@SuppressWarnings("serial")
public class SettingsException extends IllegalArgumentException {

    private final String key;

    public SettingsException(String key, String message) {
        super(message);
        this.key = key;
    }

    public SettingsException(String key, String message, Throwable cause) {
        super(message, cause);
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String getMessage() {
        return String.format("%s (check [%s] setting)", super.getMessage(), key);
    }
}
