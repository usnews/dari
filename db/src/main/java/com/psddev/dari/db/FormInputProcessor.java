package com.psddev.dari.db;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.TypeReference;

/**
 * @deprecated Use {@link FormInputProcessor2} instead.
 */
@Deprecated
public interface FormInputProcessor {

    /** Returns an HTML string for displaying an input. */
    public String display(String inputId, String inputName, ObjectField field, Object value);

    /** Returns an updated value after processing an input. */
    public Object update(String inputName, ObjectField field, HttpServletRequest request);

    /** @deprecated Use {@link FormInputProcessor2.Default} instead. */
    @Deprecated
    public static class Default extends AbstractFormInputProcessor {

        @Override
        protected void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException {
            writer.writeStart("span", "class", "json");
                writer.writeStart("textarea", "id", inputId, "name", inputName);
                    writer.writeHtml(ObjectUtils.toJson(value, true));
                writer.writeEnd();
            writer.writeEnd();
        }

        @Override
        public Object update(String inputName, ObjectField field, HttpServletRequest request) {
            return ObjectUtils.fromJson(param(String.class, request, inputName));
        }
    }

    /** @deprecated Use {@link FormInputProcessor2.ForBoolean} instead. */
    @Deprecated
    public static class ForBoolean extends AbstractFormInputProcessor {

        @Override
        protected void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException {
            writer.writeTag("input",
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

    /** @deprecated Use {@link FormInputProcessor2.Date} instead. */
    @Deprecated
    public static class ForDate extends AbstractFormInputProcessor {

        @Override
        protected void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException {
            writer.writeTag("input",
                    "type", "text",
                    "class", "date",
                    "id", inputId,
                    "name", inputName,
                    "value", value);
        }

        @Override
        public Object update(String inputName, ObjectField field, HttpServletRequest request) {
            return param(Date.class, request, inputName);
        }
    }

    /** @deprecated Use {@link FormInputProcessor2.ForListRecord} instead. */
    @Deprecated
    public static class ForListRecord extends AbstractFormInputProcessor {

        private static final FormInputProcessor.ForRecord FOR_RECORD = new FormInputProcessor.ForRecord();
        private static final TypeReference<List<UUID>> LIST_UUID_TYPE = new TypeReference<List<UUID>>() { };

        @Override
        protected void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException {
            List<?> valueList = ObjectUtils.to(List.class, value);

            writer.writeStart("ol", "class", "repeatable");
                if (valueList != null) {
                    for (Object item : valueList) {
                        writer.writeStart("li", "class", "repeatable-item");
                            FOR_RECORD.doDisplay(null, inputName, field, item, writer);
                        writer.writeEnd();
                    }
                }

                writer.writeStart("li", "class", "repeatable-template");
                    FOR_RECORD.doDisplay(null, inputName, field, null, writer);
                writer.writeEnd();
            writer.writeEnd();
        }

        @Override
        public Object update(String inputName, ObjectField field, HttpServletRequest request) {
            return param(LIST_UUID_TYPE, request, inputName);
        }
    }

    /** @deprecated Use {@link FormInputProcessor2.ForListText} instead. */
    @Deprecated
    public static class ForListText extends AbstractFormInputProcessor {

        private static final FormInputProcessor.ForText FOR_TEXT = new FormInputProcessor.ForText();
        private static final TypeReference<List<String>> LIST_STRING_TYPE = new TypeReference<List<String>>() { };

        @Override
        protected void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException {
            List<String> valueList = ObjectUtils.to(LIST_STRING_TYPE, value);

            writer.writeStart("ol", "class", "repeatable");
                if (valueList != null) {
                    for (String item : valueList) {
                        writer.writeStart("li", "class", "repeatable-item");
                            FOR_TEXT.doDisplay(null, inputName, field, item, writer);
                        writer.writeEnd();
                    }
                }

                writer.writeStart("li", "class", "repeatable-template");
                    FOR_TEXT.doDisplay(null, inputName, field, null, writer);
                writer.writeEnd();
            writer.writeEnd();
        }

        @Override
        public Object update(String inputName, ObjectField field, HttpServletRequest request) {
            return param(LIST_STRING_TYPE, request, inputName);
        }
    }

    /** @deprecated Use {@link FormInputProcessor2.ForRecord} instead. */
    @Deprecated
    public static class ForRecord extends AbstractFormInputProcessor {

        @Override
        protected void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException {
            State valueState = State.getInstance(value);
            StringBuilder typeIdsBuilder = new StringBuilder();

            for (ObjectType type : field.getTypes()) {
                typeIdsBuilder.append(type.getId());
                break;
            }

            writer.writeStart("div", "class", "objectId-label");
                if (valueState != null) {
                    writer.writeHtml(valueState.getLabel());
                }
            writer.writeEnd();

            writer.writeTag("input",
                    "type", "text",
                    "class", "objectId",
                    "data-type-ids", typeIdsBuilder,
                    "id", inputId,
                    "name", inputName,
                    "value", valueState != null ? valueState.getId() : null);
        }

        @Override
        public Object update(String inputName, ObjectField field, HttpServletRequest request) {
            return param(UUID.class, request, inputName);
        }
    }

    /** @deprecated Use {@link FormInputProcessor2.ForSetRecord} instead. */
    @Deprecated
    public static class ForSetRecord extends AbstractFormInputProcessor {

        private static final FormInputProcessor.ForRecord FOR_RECORD = new FormInputProcessor.ForRecord();
        private static final TypeReference<Set<UUID>> SET_UUID_TYPE = new TypeReference<Set<UUID>>() { };

        @Override
        protected void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException {
            Set<?> valueSet = ObjectUtils.to(Set.class, value);

            writer.writeStart("ul", "class", "repeatable");
                if (valueSet != null) {
                    for (Object item : valueSet) {
                        writer.writeStart("li", "class", "repeatable-item");
                            FOR_RECORD.doDisplay(null, inputName, field, item, writer);
                        writer.writeEnd();
                    }
                }

                writer.writeStart("li", "class", "repeatable-template");
                    FOR_RECORD.doDisplay(null, inputName, field, null, writer);
                writer.writeEnd();
            writer.writeEnd();
        }

        @Override
        public Object update(String inputName, ObjectField field, HttpServletRequest request) {
            return param(SET_UUID_TYPE, request, inputName);
        }
    }

    /** @deprecated Use {@link FormInputProcessor2.ForSetText} instead. */
    @Deprecated
    public static class ForSetText extends AbstractFormInputProcessor {

        private static final FormInputProcessor.ForText FOR_TEXT = new FormInputProcessor.ForText();
        private static final TypeReference<Set<String>> SET_STRING_TYPE = new TypeReference<Set<String>>() { };

        @Override
        protected void doDisplay(String inputId, String inputName, ObjectField field, Object value, HtmlWriter writer) throws IOException {
            Set<String> valueSet = ObjectUtils.to(SET_STRING_TYPE, value);

            writer.writeStart("ul", "class", "repeatable");
                if (valueSet != null) {
                    for (String item : valueSet) {
                        writer.writeStart("li", "class", "repeatable-item");
                            FOR_TEXT.doDisplay(null, inputName, field, item, writer);
                        writer.writeEnd();
                    }
                }

                writer.writeStart("li", "class", "repeatable-template");
                    FOR_TEXT.doDisplay(null, inputName, field, null, writer);
                writer.writeEnd();
            writer.writeEnd();
        }

        @Override
        public Object update(String inputName, ObjectField field, HttpServletRequest request) {
            return param(SET_STRING_TYPE, request, inputName);
        }
    }

    /** @deprecated Use {@link FormInputProcessor2.Text} instead. */
    @Deprecated
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
                writer.writeTag("input",
                        "type", "text",
                        "id", inputId,
                        "name", inputName,
                        "value", value,
                        "placeholder", placeholder,
                        extraAttributes);

            } else {
                writer.writeStart("select",
                        "id", inputId,
                        "name", inputName,
                        extraAttributes);

                    writer.writeStart("option",
                            "value", "",
                            "class", "placeholder");
                        writer.writeHtml(placeholder);
                    writer.writeEnd();

                    for (ObjectField.Value v : possibleValues) {
                        String vv = v.getValue();
                        writer.writeStart("option",
                                "value", vv,
                                "selected", ObjectUtils.equals(vv, value));
                            writer.writeHtml(v.getLabel());
                        writer.writeEnd();
                    }

                writer.writeEnd();
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
