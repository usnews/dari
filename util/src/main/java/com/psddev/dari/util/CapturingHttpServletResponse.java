package com.psddev.dari.util;

import java.io.PrintWriter;
import java.io.StringWriter;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

/**
 * {@link HttpServletResponse} implementation that buffers and captures
 * the response output.
 */
public class CapturingHttpServletResponse extends HttpServletResponseWrapper {

    private final StringWriter capture;
    private final PrintWriter writer;

    public CapturingHttpServletResponse(HttpServletResponse response) {
        super(response);

        this.capture = new StringWriter();
        this.writer = new PrintWriter(capture);
    }

    @Override
    public ServletOutputStream getOutputStream() {
        throw new IllegalStateException();
    }

    @Override
    public PrintWriter getWriter() {
        return writer;
    }

    /**
     * Returns the output captured so far.
     *
     * @return Never {@code null}.
     */
    public String getOutput() {
        return capture.toString();
    }
}
