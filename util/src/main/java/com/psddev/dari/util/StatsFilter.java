package com.psddev.dari.util;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

/**
 * Collects statistics around common servlet operations using
 * {@link Stats}.
 */
public class StatsFilter extends AbstractFilter {

    private static final Stats STATS_INCLUDES = new Stats("JSP Includes");
    private static final Stats STATS_RESPONSES = new Stats("HTTP Responses");

    // --- AbstractFilter support ---

    @Override
    protected void doInclude(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        Stats.Timer timer = STATS_INCLUDES.startTimer();

        try {
            chain.doFilter(request, response);

        } finally {
            timer.stop(JspUtils.getCurrentServletPath(request));
        }
    }

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        StatusPreservingResponse statusResponse = new StatusPreservingResponse(response);
        Stats.Timer timer = STATS_RESPONSES.startTimer();

        try {
            chain.doFilter(request, statusResponse);

        } finally {
            timer.stop(String.valueOf(statusResponse.getStatus()));
        }
    }

    private static class StatusPreservingResponse extends HttpServletResponseWrapper {

        private int status;

        public StatusPreservingResponse(HttpServletResponse response) {
            super(response);
        }

        public int getStatus() {
            return status;
        }

        // --- HttpServletResponseWrapper support ---

        @Override
        public void sendError(int status) throws IOException {
            this.status = status;
            super.sendError(status);
        }

        @Override
        public void sendError(int status, String message) throws IOException {
            this.status = status;
            super.sendError(status, message);
        }

        @Override
        public void setStatus(int status) {
            this.status = status;
            super.setStatus(status);
        }
    }
}
