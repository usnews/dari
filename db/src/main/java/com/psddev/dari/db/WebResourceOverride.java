package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.List;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;

@UpdateTrackable.Names(WebResourceOverride.UPDATE_TRACKING_NAME)
public class WebResourceOverride extends Record implements UpdateTrackable {

    public static final String UPDATE_TRACKING_NAME = "dari.webResourceOverride";

    @Indexed
    @Required
    private String path;

    private Integer statusCode;
    private List<Header> headers;
    private String content;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public void setStatus(Integer statusCode) {
        this.statusCode = statusCode;
    }

    public List<Header> getHeaders() {
        if (headers == null) {
            headers = new ArrayList<Header>();
        }
        return headers;
    }

    public void setHeaders(List<Header> headers) {
        this.headers = headers;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    protected void beforeSave() {
        String path = getPath();

        if (!ObjectUtils.isBlank(path)) {
            setPath(StringUtils.ensureStart(path, "/"));
        }
    }

    @Embedded
    public static class Header extends Record {

        @Required
        private String name;

        @Required
        private String value;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
