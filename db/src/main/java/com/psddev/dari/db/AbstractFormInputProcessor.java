package com.psddev.dari.db;

import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.TypeReference;

import java.io.IOException;
import java.io.StringWriter;

import javax.servlet.http.HttpServletRequest;

/**
 * Skeletal implementation of {@link FormInputProcessor}.
 *
 * <p>A subclass must implement:
 *
 * <ul>
 * <li>{@link #doDisplay}</li>
 * <li>{@link #update}</li>
 * </ul>
 */
public abstract class AbstractFormInputProcessor implements FormInputProcessor {

    @Override
    public final String display(String inputId, String inputName, ObjectField field, Object value) {
        StringWriter string = new StringWriter();
        HtmlWriter writer = new HtmlWriter(string);
        try {
            doDisplay(inputId, inputName, field, value, writer);
        } catch (IOException ex) {
        }
        return string.toString();
    }

    /**
     * Called by {@link #display} to construct the HTML string
     * for displaying an input.
     */
    protected abstract void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException;

    /**
     * Finds a parameter value associated with the given {@code name}
     * in the given {@code request}, converts it to an instance of
     * the given {@code returnClass}, and returns it.
     */
    protected <T> T param(Class<T> returnClass, HttpServletRequest request, String name) {
        String value = request.getParameter(name);
        value = ObjectUtils.isBlank(value) ? null : value.trim();
        return ObjectUtils.to(returnClass, value);
    }

    protected <T> T param(TypeReference<T> returnTypeReference, HttpServletRequest request, String name) {
        String value = request.getParameter(name);
        value = ObjectUtils.isBlank(value) ? null : value.trim();
        return ObjectUtils.to(returnTypeReference, value);
    }
}
