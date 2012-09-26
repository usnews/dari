package com.psddev.dari.util;

import java.util.Map;

/**
 * Object that can be initialized by {@linkplain Settings setting values}.
 */
public interface SettingsBackedObject {

    /**
     * Initializes this object using the given {@code settings}.
     * @param settingsKey Key used to retrieve the given {@code settings}.
     */
    public void initialize(String settingsKey, Map<String, Object> settings);
}
