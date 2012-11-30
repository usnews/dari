package com.psddev.dari.util;

import javax.servlet.http.HttpServletRequest;

/**
 * Processes a form.
 *
 * @see FormTag
 */
public interface FormProcessor {

    /**
     * Processes the form using the given {@code request}.
     *
     * @param request Can't be {@code null}.
     */
    public void processRequest(HttpServletRequest request);
}
