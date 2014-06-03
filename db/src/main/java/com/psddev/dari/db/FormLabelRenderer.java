package com.psddev.dari.db;

import java.io.IOException;

import com.psddev.dari.util.HtmlWriter;

/**
 * Writes input labels for {@link FormWriter}.
 *
 * @deprecated No replacement.
 */
@Deprecated
public interface FormLabelRenderer {

    /** Returns an HTML string for displaying an input label. */
    public String display(String inputId, String inputName, ObjectField field);

    /**
     * Default {@link FormLabelRenderer}.
     *
     * @deprecated No replacement.
     */
    @Deprecated
    public static class Default extends AbstractFormLabelRenderer {

        @Override
        protected void doDisplay(String inputId, String inputName, ObjectField field, HtmlWriter writer) throws IOException {
            writer.writeStart("label", "for", inputId);
                writer.writeHtml(field.getDisplayName());
            writer.writeEnd();
        }
    }

    // --- Deprecated ---

    /** @deprecated Use {@link AbstractFormLabelRenderer} instead. */
    @Deprecated
    public abstract static class Abstract extends AbstractFormLabelRenderer {
    }
}
