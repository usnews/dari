package com.psddev.dari.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.Map;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

/**
 * Filter that automatically captures the HTML output from the response
 * and formats it so that it's suitable for use as an API.
 *
 * @see HtmlMicrodata
 */
public class HtmlApiFilter extends AbstractFilter {

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        String format = request.getParameter("_format");

        if (ObjectUtils.isBlank(format)) {
            chain.doFilter(request, response);
            return;
        }

        if ("json".equals(format)) {
            CapturingResponse capturing = new CapturingResponse(response);
            Map<String, Object> jsonResponse = new CompactMap<String, Object>();

            try {
                chain.doFilter(request, capturing);
                jsonResponse.put("status", "ok");
                jsonResponse.put("result", HtmlMicrodata.Static.parseString(
                        new URL(JspUtils.getAbsoluteUrl(request, "")),
                        capturing.getOutput()));

            } catch (RuntimeException error) {
                jsonResponse.put("status", "error");
                jsonResponse.put("errorClass", error.getClass().getName());
                jsonResponse.put("errorMessage", error.getMessage());
            }

            response.setContentType("application/json");
            response.getWriter().write(ObjectUtils.toJson(jsonResponse));

        } else {
            throw new IllegalArgumentException(String.format(
                    "[%s] isn't a valid API response format!", format));
        }
    }

    private final static class CapturingResponse extends HttpServletResponseWrapper {

        private final StringWriter output;
        private final PrintWriter printWriter;

        public CapturingResponse(HttpServletResponse response) {
            super(response);

            this.output = new StringWriter();
            this.printWriter = new PrintWriter(output);
        }

        @Override
        public ServletOutputStream getOutputStream() {
            throw new IllegalStateException();
        }

        @Override
        public PrintWriter getWriter() {
            return printWriter;
        }

        public String getOutput() {
            return output.toString();
        }
    }
}
