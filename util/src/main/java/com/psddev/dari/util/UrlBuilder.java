package com.psddev.dari.util;

import java.net.URI;
import java.net.URISyntaxException;

import javax.servlet.http.HttpServletRequest;

public class UrlBuilder {

    private final HttpServletRequest request;
    private String scheme;
    private String host;
    private String path;

    public UrlBuilder(HttpServletRequest request) {
        this.request = request;
    }

    public UrlBuilder() {
        this(null);
    }

    protected HttpServletRequest getRequest() {
        if (request == null) {
            throw new IllegalStateException("[request] required!");
        }

        return request;
    }

    public UrlBuilder absolutePath(String path) {
        this.path = path;

        return this;
    }

    public UrlBuilder path(String path) {
        if (path != null) {
            HttpServletRequest request = getRequest();

            if (path.startsWith("/")) {
                this.path = request.getContextPath() + path;

            } else {
                try {
                    this.path = new URI(request.getRequestURI()).resolve(path).toString();

                } catch (URISyntaxException error) {
                    this.path = path;
                }
            }
        }

        return this;
    }

    public UrlBuilder currentPath() {
        return absolutePath(getRequest().getRequestURI());
    }

    /**
     * @param key If {@code null}, does nothing.
     * @param value If {@code null}, removes all parameters associated with
     * the given {@code key}.
     */
    public UrlBuilder parameter(Object key, Object value) {
        if (key != null) {
            path = StringUtils.addQueryParameters(path, key, value);
        }

        return this;
    }

    public UrlBuilder currentParameters() {
        int questionAt = path.indexOf('?');

        if (questionAt > -1) {
            path = path.substring(0, questionAt);
        }

        String queryString = getRequest().getQueryString();

        if (queryString != null) {
            path += "?";
            path += queryString;
        }

        return this;
    }

    public UrlBuilder host(String host) {
        this.host = host;

        return this;
    }

    public UrlBuilder currentHost() {
        return host(JspUtils.getHost(getRequest()));
    }

    public UrlBuilder scheme(String scheme) {
        this.scheme = scheme;

        return this;
    }

    public UrlBuilder currentScheme() {
        return scheme(JspUtils.isSecure(getRequest()) ? "https" : "http");
    }

    @Override
    public String toString() {
        StringBuilder url = new StringBuilder();

        if (path != null) {
            if (path.startsWith("/") && host != null) {
                if (scheme != null) {
                    url.append(scheme);
                    url.append(':');
                }

                url.append("//");
                url.append(host);
            }

            url.append(path);
        }

        return url.toString();
    }
}
