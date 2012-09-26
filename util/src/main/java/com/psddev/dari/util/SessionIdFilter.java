package com.psddev.dari.util;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import javax.servlet.http.HttpSession;

/** Suppresses session IDs in URLs for security. */
public class SessionIdFilter extends AbstractFilter {

    // --- AbstractFilter support ---

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        if (request.isRequestedSessionIdFromURL()) {
            HttpSession session = request.getSession();
            if (session != null) {
                session.invalidate();
            }
        }

        chain.doFilter(request, new HttpServletResponseWrapper(response) {

            @Override
            public String encodeRedirectUrl(String url) {
                return url;
            }

            @Override
            public String encodeRedirectURL(String url) {
                return url;
            }

            @Override
            public String encodeUrl(String url) {
                return url;
            }

            @Override
            public String encodeURL(String url) {
                return url;
            }
        });
    }
}
