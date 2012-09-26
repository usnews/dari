package com.psddev.dari.db;

import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.ObjectUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

/** Processes individual inputs written with {@link FormWriter}. */
public interface FormInputProcessor {

    /** Returns an HTML string for displaying an input. */
    public String display(String inputId, String inputName, ObjectField field, Object value);

    /** Returns an updated value after processing an input. */
    public Object update(String inputName, ObjectField field, HttpServletRequest request);

    /**
     * Default {@link FormInputProcessor} that uses JSON to handle unknown
     * content.
     */
    public static class Default extends AbstractFormInputProcessor {

        @Override
        protected void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException {
            writer.start("textarea",
                    "class", "json",
                    "id", inputId,
                    "name", inputName);
                writer.html(ObjectUtils.toJson(value, true));
            writer.end();
        }

        @Override
        public Object update(String inputName, ObjectField field, HttpServletRequest request) {
            return ObjectUtils.fromJson(param(String.class, request, inputName));
        }
    }

    /** {@link FormInputProcessor} for {@link ObjectField#BOOLEAN_TYPE}. */
    public static class ForBoolean extends AbstractFormInputProcessor {

        @Override
        protected void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException {
            writer.tag("input",
                    "type", "checkbox",
                    "id", inputId,
                    "name", inputName,
                    "value", "true",
                    "checked", Boolean.TRUE.equals(value) ? "checked" : null);
        }

        @Override
        public Object update(String inputName, ObjectField field, HttpServletRequest request) {
            return param(boolean.class, request, inputName);
        }
    }

    /** {@link FormInputProcessor} for {@link ObjectField#RECORD_TYPE}. */
    public static class ForRecord extends AbstractFormInputProcessor {

        @Override
        protected void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException {
            State valueState = State.getInstance(value);
            writer.tag("input",
                    "type", "text",
                    "class", "objectId",
                    "id", inputId,
                    "name", inputName,
                    "value", valueState != null ? valueState.getId() : null);
        }

        @Override
        public Object update(String inputName, ObjectField field, HttpServletRequest request) {
            return param(UUID.class, request, inputName);
        }
    }

    /** {@link FormInputProcessor} for {@link ObjectField#TEXT_TYPE}. */
    public static class ForText extends AbstractFormInputProcessor {

        protected String createPlaceholder(ObjectField field) {
            return field.isRequired() ? "(Required)" : null;
        }

        protected Map<String, String> createExtraAttributes(ObjectField field) {
            return null;
        }

        @Override
        protected void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException {
            String placeholder = createPlaceholder(field);
            Map<String, String> extraAttributes = createExtraAttributes(field);
            Set<ObjectField.Value> possibleValues = field.getValues();

            if (possibleValues == null || possibleValues.isEmpty()) {
                writer.tag("input",
                        "type", "text",
                        "id", inputId,
                        "name", inputName,
                        "value", value,
                        "placeholder", placeholder,
                        extraAttributes);

            } else {
                writer.start("select",
                        "id", inputId,
                        "name", inputName,
                        extraAttributes);

                    writer.start("option",
                            "value", "",
                            "class", "placeholder");
                        writer.html(placeholder);
                    writer.end();

                    for (ObjectField.Value v : possibleValues) {
                        String vv = v.getValue();
                        writer.start("option",
                                "value", vv,
                                "selected", ObjectUtils.equals(vv, value));
                            writer.html(v.getLabel());
                        writer.end();
                    }

                writer.end();
            }
        }

        @Override
        public Object update(String inputName, ObjectField field, HttpServletRequest request) {
            return param(String.class, request, inputName);
        }
    }

    // --- Deprecated ---

    /** @deprecated Use {@link AbstractFormInputProcessor} instead. */
    @Deprecated
    public static abstract class Abstract extends AbstractFormInputProcessor {
    }
}
