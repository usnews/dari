package com.psddev.dari.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
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

        CapturingResponse capturing = new CapturingResponse(response);
        Object output;

        try {
            chain.doFilter(request, capturing);
            output = capturing.getOutput();

        } catch (RuntimeException error) {
            output = error;
        }

        Writer writer = response.getWriter();

        if ("json".equals(format)) {
            response.setContentType("application/json");
            writeJson(request, writer, output);

        } else if("jsonp".equals(format)) {
            String callback = request.getParameter("_callback");

            ErrorUtils.errorIfBlank(callback, "_callback");

            response.setContentType("application/javascript");
            writer.write(callback);
            writer.write("(");
            writeJson(request, writer, output);
            writer.write(");");

        } else {
            throw new IllegalArgumentException(String.format(
                    "[%s] isn't a valid API response format!", format));
        }
    }

    private static void writeJson(HttpServletRequest request, Writer writer, Object output) throws IOException {
        Map<String, Object> json = new CompactMap<String, Object>();

        if (output instanceof Throwable) {
            Throwable error = (Throwable) output;

            json.put("status", "error");
            json.put("errorClass", error.getClass().getName());
            json.put("errorMessage", error.getMessage());

        } else {
            if (!"html".equals(request.getParameter("_result"))) {
                output = HtmlMicrodata.Static.parseString(
                        new URL(JspUtils.getAbsoluteUrl(request, "")),
                        (String) output);
            }

            json.put("status", "ok");
            json.put("result", output);
        }

        writer.write(ObjectUtils.toJson(json));
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
