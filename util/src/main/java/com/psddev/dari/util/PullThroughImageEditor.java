package com.psddev.dari.util;

import java.util.Map;

/** An empty ImageEditor used when DIMS is unavailable */
public class PullThroughImageEditor extends AbstractImageEditor {

    /** Setting key for the base URL to the  installation. */
    public static final String BASE_URL_SETTING = "baseUrl";

    private String baseUrl;


    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {

        setBaseUrl(ObjectUtils.to(String.class, settings.get(BASE_URL_SETTING)));
    }

    @Override
    public StorageItem edit(
                StorageItem image,
                String command,
                Map<String, Object> options,
                Object... arguments) {

        return image;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }
}