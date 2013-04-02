package com.psddev.dari.util;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

/**
 * See {@link JspUtils#setHeaderResponse} for a detailed explanation of
 * when and why this filter should be used.
 */
public class HeaderResponseFilter extends AbstractFilter {

    // --- AbstractFilter support ---

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        // Try to find the real original response.
        while (response instanceof HttpServletResponseWrapper) {
            response = (HttpServletResponse) ((HttpServletResponseWrapper) response).getResponse();
        }

        response = new HeaderResponse(response);

        JspUtils.setHeaderResponse(request, response);
        chain.doFilter(request, response);
    }
}
