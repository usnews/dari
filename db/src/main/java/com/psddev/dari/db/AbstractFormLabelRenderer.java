package com.psddev.dari.db;

import java.io.IOException;
import java.io.StringWriter;

import com.psddev.dari.util.HtmlWriter;

/**
 * Skeletal implementation sof {@link FormLabelRenderer}.
 *
 * <p>A subclass must implement:
 *
 * <ul>
 * <li>{@link #doDisplay}</li>
 * </ul>
 *
 * @deprecated No replacement.
 */
@Deprecated
public abstract class AbstractFormLabelRenderer implements FormLabelRenderer {

    @Override
    public final String display(String inputId, String inputName, ObjectField field) {
        StringWriter string = new StringWriter();
        HtmlWriter html = new HtmlWriter(string);
        try {
            doDisplay(inputId, inputName, field, html);
        } catch (IOException error) {
            // This should never happen since StringWriter doesn't throw
            // IOException.
        }
        return string.toString();
    }

    /**
     * Called by {@link #display} to construct the HTML string
     * for display an input label.
     */
    protected abstract void doDisplay(String inputId, String inputName, ObjectField field, HtmlWriter writer) throws IOException;
}
