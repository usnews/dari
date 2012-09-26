package com.psddev.dari.util;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.servlet.ServletFileUpload;

/** Automatically enables {@link MultipartRequest}. */
public class MultipartRequestFilter extends AbstractFilter {

    // --- AbstractFilter support ---

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        if (!(request instanceof MultipartRequest) &&
                ServletFileUpload.isMultipartContent(request)) {
            request = new MultipartRequest(request);
        }

        chain.doFilter(request, response);
    }
}
