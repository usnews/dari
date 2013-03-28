package com.psddev.dari.db;

import java.io.IOException;
import java.io.StringWriter;

import com.psddev.dari.util.HtmlWriter;

public abstract class AbstractFormInputProcessor2 implements FormInputProcessor2 {

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
    protected abstract void doDisplay(String inputId, String inputName, ObjectField field, State state, Object value, HtmlWriter writer) throws IOException;
}
