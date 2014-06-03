package com.psddev.dari.util;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Processes a form.
 *
 * @see FormTag
 * @deprecated No replacement.
 */
@Deprecated
public interface FormProcessor {

    /**
     * Processes the form using the given {@code request} and optionally writes
     * to the given {@code response}. Optionally returns an object containing
     * the result of processing.
     *
     * @param request Can't be {@code null}.
     * @param response Can't be {@code null}.
     * @throws IOException
     */
    public Object process(HttpServletRequest request, HttpServletResponse response) throws IOException;
}
