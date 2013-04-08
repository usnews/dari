package com.psddev.dari.db;

import java.io.IOException;
import java.io.StringWriter;

import javax.servlet.http.HttpServletRequest;

import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.TypeReference;

/**
 * Skeletal implementation of {@link FormInputProcessor2}.
 *
 * <p>A subclass must implement:
 *
 * <ul>
 * <li>{@link #doDisplay}</li>
 * <li>{@link FormInputProcessor2#update update}</li>
 * </ul>
 *
 */
@SuppressWarnings("deprecation")
public abstract class AbstractFormInputProcessor implements FormInputProcessor, FormInputProcessor2 {

    @Override
    public final String display(String inputId, String inputName, ObjectField field, State state) {
        StringWriter string = new StringWriter();
        HtmlWriter writer = new HtmlWriter(string);
        try {
            doDisplay(inputId, inputName, field, state, state.get(field.getInternalName()), writer);
        } catch (IOException ex) {
        }
        return string.toString();
    }

    /**
     * Called by {@link #display} to construct the HTML string
     * for displaying an input.
     */
    protected void doDisplay(String inputId, String inputName, ObjectField field, State state, Object value, HtmlWriter writer) throws IOException {
        doDisplay(inputId, inputName, field, state.get(field.getInternalName()), writer);
    }

    /**
     * @deprecated Sub-classes are expected to override
     * {@link #doDisplay(String, String, ObjectField, State, Object, HtmlWriter)} instead.
     */
    @Deprecated
    protected void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException {
    }

    /** @deprecated Use {@link #display(String, String, ObjectField, State)} instead. */
    @Override
    @Deprecated
    public final String display(String inputId, String inputName, ObjectField field, Object value) {
        StringWriter string = new StringWriter();
        HtmlWriter writer = new HtmlWriter(string);
        try {
            doDisplay(inputId, inputName, field, value, writer);
        } catch (IOException ex) {
        }
        return string.toString();
    }

    /** @deprecated Use {@link ObjectUtils#to(Class, Object)} instead. */
    @Deprecated
    protected <T> T param(Class<T> returnClass, HttpServletRequest request, String name) {
        String value = request.getParameter(name);
        value = ObjectUtils.isBlank(value) ? null : value.trim();
        return ObjectUtils.to(returnClass, value);
    }

    /** @deprecated Use {@link ObjectUtils#to(TypeReference, Object)} instead. */
    @Deprecated
    protected <T> T param(TypeReference<T> returnTypeReference, HttpServletRequest request, String name) {
        String value = request.getParameter(name);
        value = ObjectUtils.isBlank(value) ? null : value.trim();
        return ObjectUtils.to(returnTypeReference, value);
    }
}
