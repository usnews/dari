package com.psddev.dari.util;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Forces {@link StringUtils#UTF_8} character encoding on all requests
 * and responses.
 */
public class Utf8Filter extends AbstractFilter {

    // --- AbstractFilter support ---

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        String encoding = StringUtils.UTF_8.name();
        request.setCharacterEncoding(encoding);
        response.setCharacterEncoding(encoding);
        chain.doFilter(request, response);
    }

    // --- Deprecated ---

    /** @deprecated Use {@link StringUtils#UTF_8} instead. */
    @Deprecated
    public static final String ENCODING = StringUtils.UTF_8.name();
}
