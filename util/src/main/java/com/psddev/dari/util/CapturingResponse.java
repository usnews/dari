package com.psddev.dari.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

/**
 * {@link HttpServletResponse} implementation that buffers and captures
 * the response output.
 */
public class CapturingResponse extends HttpServletResponseWrapper {

    private ServletOutputStream outputStream;
    private StringWriter capture;
    private PrintWriter writer;

    public CapturingResponse(HttpServletResponse response) {
        super(response);
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        if (writer != null) {
            throw new IllegalStateException();

        } else {
            if (outputStream == null) {
                outputStream = getResponse().getOutputStream();
            }

            return outputStream;
        }
    }

    @Override
    public PrintWriter getWriter() {
        if (outputStream != null) {
            throw new IllegalStateException();

        } else {
            if (writer == null) {
                capture = new StringWriter();
                writer = new PrintWriter(capture);
            }

            return writer;
        }
    }

    /**
     * Returns the output captured so far.
     *
     * @return Never {@code null}.
     */
    public String getOutput() {
        return capture != null ? capture.toString() : "";
    }
}
