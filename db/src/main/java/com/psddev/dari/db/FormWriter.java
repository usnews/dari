package com.psddev.dari.db;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import com.psddev.dari.util.ErrorUtils;
import com.psddev.dari.util.HtmlWriter;

/** Writer that specializes in processing HTML form inputs. */
public class FormWriter extends HtmlWriter {

    private FormLabelRenderer labelRenderer;
    private FormInputProcessor2 defaultInputProcessor2;
    private Map<String, FormInputProcessor2> inputProcessors2;

    /** Creates an instance that writes to the given {@code writer}. */
    public FormWriter(Writer writer) {
        super(writer);
    }

    /**
     * Returns the label renderer. If {@code null}, it's set to the
     * {@linkplain FormLabelRenderer.Default default} before returning.
     *
     * @return Can't be {@code null}.
     */
    public FormLabelRenderer getLabelRenderer() {
        if (labelRenderer == null) {
            setLabelRenderer(new FormLabelRenderer.Default());
        }
        return labelRenderer;
    }

    /** Sets the label renderer. */
    public void setLabelRenderer(FormLabelRenderer labelRenderer) {
        this.labelRenderer = labelRenderer;
    }

    /**
     * Returns the default form input processor. If {@code null}, it's set to
     * the {@linkplain FormInputProcessor2.Default default} before returning.
     *
     * @return Can't be {@code null}.
     */
    public FormInputProcessor2 getDefaultInputProcessor2() {
        if (defaultInputProcessor2 == null) {
            setDefaultInputProcessor2(new FormInputProcessor2.Default());
        }
        return defaultInputProcessor2;
    }

    /** Sets the default form input processor. */
    public void setDefaultInputProcessor2(FormInputProcessor2 defaultInputProcessor2) {
        this.defaultInputProcessor2 = defaultInputProcessor2;
    }

    /**
     * Returns the map of all input processors.
     *
     * @return Can't be {@code null}.
     */
    public Map<String, FormInputProcessor2> getInputProcessors2() {
        if (inputProcessors2 == null) {
            setInputProcessors2(new HashMap<String, FormInputProcessor2>());
        }
        return inputProcessors2;
    }

    /** Sets the map of all input processors. */
    public void setInputProcessors2(Map<String, FormInputProcessor2> inputProcessors2) {
        this.inputProcessors2 = inputProcessors2;
    }

    /** Puts all standard input processors. */
    public void putAllStandardInputProcessors2() {
        Map<String, FormInputProcessor2> inputProcessors = getInputProcessors2();
        inputProcessors.put(ObjectField.BOOLEAN_TYPE, new FormInputProcessor2.ForBoolean());
        inputProcessors.put(ObjectField.DATE_TYPE, new FormInputProcessor2.ForDate());
        inputProcessors.put(ObjectField.LIST_TYPE + "/" + ObjectField.RECORD_TYPE, new FormInputProcessor2.ForListRecord(this));
        inputProcessors.put(ObjectField.LIST_TYPE + "/" + ObjectField.TEXT_TYPE, new FormInputProcessor2.ForListText());
        inputProcessors.put(ObjectField.NUMBER_TYPE, new FormInputProcessor2.ForText());
        inputProcessors.put(ObjectField.RECORD_TYPE, new FormInputProcessor2.ForRecord());
        inputProcessors.put(ObjectField.SET_TYPE + "/" + ObjectField.RECORD_TYPE, new FormInputProcessor2.ForSetRecord(this));
        inputProcessors.put(ObjectField.SET_TYPE + "/" + ObjectField.TEXT_TYPE, new FormInputProcessor2.ForSetText());
        inputProcessors.put(ObjectField.TEXT_TYPE, new FormInputProcessor2.ForText());
    }

    // Finds the type associated with the given state, throwing
    // errors if there isn't one.
    private ObjectType findType(State state) {
        ErrorUtils.errorIfNull(state, "state");

        ObjectType type = state.getType();
        if (type == null) {
            throw new IllegalStateException("Given state isn't typed!");
        }

        return type;
    }

    // Finds the field associated with the given fieldName in the
    // given type, throwing errors if there isn't one.
    private ObjectField findField(ObjectType type, String fieldName) {
        ObjectField field = type.getState().getDatabase().getEnvironment().getField(fieldName);

        if (field == null) {
            field = type.getField(fieldName);

            if (field == null) {
                throw new IllegalArgumentException(String.format(
                        "[%s] doesn't contain [%s]!", type.getLabel(), fieldName));
            }
        }

        return field;
    }

    // Finds the processor associated with the given field, returning
    // null if there isn't one.
    protected FormInputProcessor2 findInputProcessor2(ObjectField field) {
        String type = field.getInternalType();

        if (type != null) {
            Map<String, FormInputProcessor2> processors = getInputProcessors2();

            while (true) {
                FormInputProcessor2 processor = processors.get(type);
                if (processor != null) {
                    return processor;
                }

                int slashAt = type.lastIndexOf('/');
                if (slashAt < 0) {
                    break;
                } else {
                    type = type.substring(0, slashAt);
                }
            }
        }

        return null;
    }

    /**
     * Writes the given {@code field} in the given {@code state}. This
     * method is the underlying implementation for both {@link #inputs}
     * and {@link #allInputs}.
     */
    protected void writeField2(State state, ObjectField field, FormInputProcessor2 processor) throws IOException {
        String fieldName = field.getInternalName();
        String inputId = "i" + UUID.randomUUID().toString().replace("-", "");
        String inputName = state.getId() + "/" + fieldName;

        if (processor == null) {
            processor = getDefaultInputProcessor2();
        }

        write(getLabelRenderer().display(inputId, inputName, field));
        write(processor.display(inputId, inputName, field, state));
    }

    /**
     * Writes the inputs for the given {@code fieldNames} in the given
     * {@code state}.
     */
    public HtmlWriter inputs(State state, String... fieldNames) throws IOException {
        if (fieldNames != null) {
            boolean isUsingDeprecatedWrite = isUsingDeprecatedWriteField();

            ObjectType type = findType(state);
            for (String fieldName : fieldNames) {
                ObjectField field = findField(type, fieldName);

                if (isUsingDeprecatedWrite) {
                    writeField(state, field, findInputProcessor(field));
                } else {
                    writeField2(state, field, findInputProcessor2(field));
                }
            }
        }
        return this;
    }

    /**
     * Writes all inputs in the given {@code state}.
     */
    public HtmlWriter allInputs(State state) throws IOException {
        boolean isUsingDeprecatedWrite = isUsingDeprecatedWriteField();

        ObjectType type = findType(state);
        for (ObjectField field : type.getFields()) {

            if (isUsingDeprecatedWrite) {
                writeField(state, field, findInputProcessor(field));
            } else {
                writeField2(state, field, findInputProcessor2(field));
            }
        }
        return this;
    }

    @SuppressWarnings("deprecation")
    private boolean isUsingDeprecatedWriteField() {
        try {
            Method writeFieldMethod = getClass().getDeclaredMethod(
                    "writeField",
                    State.class, ObjectField.class, FormInputProcessor.class);

            if (!FormWriter.class.equals(writeFieldMethod.getDeclaringClass())) {
                return true;
            }
        } catch (SecurityException e) {
        } catch (NoSuchMethodException e) {
        }
        return false;
    }

    /**
     * Updates the given {@code field} in the given {@code state}
     * using the given {@code request}. This method is the underlying
     * implementation for both {@link #update} and {@link #updateAll}.
     */
    protected void updateField2(State state, HttpServletRequest request, ObjectField field, FormInputProcessor2 processor) {
        String fieldName = field.getInternalName();
        String inputName = state.getId() + "/" + fieldName;

        if (processor == null) {
            processor = getDefaultInputProcessor2();
        }

        state.put(fieldName, processor.update(inputName, field, request));
    }

    /**
     * Updates the given {@code fieldNames} in the given {@code state}
     * using the given {@code request}.
     */
    public void update(State state, HttpServletRequest request, String... fieldNames) {
        ErrorUtils.errorIfNull(request, "request");

        if (fieldNames != null) {
            boolean isUsingDeprecatedUpdate = isUsingDeprecatedUpdateField();

            ObjectType type = findType(state);
            for (String fieldName : fieldNames) {
                ObjectField field = findField(type, fieldName);

                if (isUsingDeprecatedUpdate) {
                    updateField(state, request, field, findInputProcessor(field));

                } else {
                    updateField2(state, request, field, findInputProcessor2(field));
                }
            }
        }
    }

    /**
     * Updates all fields in the given {@code state} using the given
     * {@code request}.
     */
    public void updateAll(State state, HttpServletRequest request) {
        ErrorUtils.errorIfNull(request, "request");

        boolean isUsingDeprecatedUpdate = isUsingDeprecatedUpdateField();

        ObjectType type = findType(state);
        for (ObjectField field : type.getFields()) {

            if (isUsingDeprecatedUpdate) {
                updateField(state, request, field, findInputProcessor(field));

            } else {
                updateField2(state, request, field, findInputProcessor2(field));
            }
        }
    }

    @SuppressWarnings("deprecation")
    private boolean isUsingDeprecatedUpdateField() {
        try {
            Method updateFieldMethod = getClass().getDeclaredMethod(
                    "updateField",
                    State.class, HttpServletRequest.class, ObjectField.class, FormInputProcessor.class);

            if (!FormWriter.class.equals(updateFieldMethod.getDeclaringClass())) {
                return true;
            }
        } catch (SecurityException e) {
        } catch (NoSuchMethodException e) {
        }
        return false;
    }

    // --- deprecations ---

    @Deprecated
    private FormInputProcessor defaultInputProcessor;
    @Deprecated
    private Map<String, FormInputProcessor> inputProcessors;

    /** @deprecated Use {@link #getDefaultInputProcessor2()} instead. */
    @Deprecated
    public FormInputProcessor getDefaultInputProcessor() {
        if (defaultInputProcessor == null) {
            setDefaultInputProcessor(new FormInputProcessor.Default());
        }
        return defaultInputProcessor;
    }

    /** @deprecated Use {@link #setDefaultInputProcessor2(FormInputProcessor2)} instead. */
    @Deprecated
    public void setDefaultInputProcessor(FormInputProcessor defaultInputProcessor) {
        this.defaultInputProcessor = defaultInputProcessor;
    }

    /** @deprecated Use {@link #getInputProcessors2()} instead. */
    @Deprecated
    public Map<String, FormInputProcessor> getInputProcessors() {
        if (inputProcessors == null) {
            setInputProcessors(new HashMap<String, FormInputProcessor>());
        }
        return inputProcessors;
    }

    /** @deprecated Use {@link #setInputProcessors2(Map)} instead. */
    @Deprecated
    public void setInputProcessors(Map<String, FormInputProcessor> inputProcessors) {
        this.inputProcessors = inputProcessors;
    }

    /** @deprecated Use {@link #putAllStandardInputProcessors2()} instead. */
    @Deprecated
    public void putAllStandardInputProcessors() {
        Map<String, FormInputProcessor> inputProcessors = getInputProcessors();
        inputProcessors.put(ObjectField.BOOLEAN_TYPE, new FormInputProcessor.ForBoolean());
        inputProcessors.put(ObjectField.DATE_TYPE, new FormInputProcessor.ForDate());
        inputProcessors.put(ObjectField.LIST_TYPE + "/" + ObjectField.RECORD_TYPE, new FormInputProcessor.ForListRecord());
        inputProcessors.put(ObjectField.LIST_TYPE + "/" + ObjectField.TEXT_TYPE, new FormInputProcessor.ForListText());
        inputProcessors.put(ObjectField.NUMBER_TYPE, new FormInputProcessor.ForText());
        inputProcessors.put(ObjectField.RECORD_TYPE, new FormInputProcessor.ForRecord());
        inputProcessors.put(ObjectField.SET_TYPE + "/" + ObjectField.RECORD_TYPE, new FormInputProcessor.ForSetRecord());
        inputProcessors.put(ObjectField.SET_TYPE + "/" + ObjectField.TEXT_TYPE, new FormInputProcessor.ForSetText());
        inputProcessors.put(ObjectField.TEXT_TYPE, new FormInputProcessor.ForText());
    }

    /** @deprecated Use {@link #findInputProcessor2(ObjectField)} instead. */
    @Deprecated
    protected FormInputProcessor findInputProcessor(ObjectField field) {
        String type = field.getInternalType();

        if (type != null) {
            Map<String, FormInputProcessor> processors = getInputProcessors();

            while (true) {
                FormInputProcessor processor = processors.get(type);
                if (processor != null) {
                    return processor;
                }

                int slashAt = type.lastIndexOf('/');
                if (slashAt < 0) {
                    break;
                } else {
                    type = type.substring(0, slashAt);
                }
            }
        }

        return null;
    }

    /** @deprecated Use {@link #writeField2(State, ObjectField, FormInputProcessor2)} instead. */
    @Deprecated
    protected void writeField(State state, ObjectField field, FormInputProcessor processor) throws IOException {
        String fieldName = field.getInternalName();
        String inputId = "i" + UUID.randomUUID().toString().replace("-", "");
        String inputName = state.getId() + "/" + fieldName;

        if (processor == null) {
            processor = getDefaultInputProcessor();
        }

        write(getLabelRenderer().display(inputId, inputName, field));
        write(processor.display(inputId, inputName, field, state.get(fieldName)));
    }

    /** @deprecated Use {@link #updateField2(State, HttpServletRequest, ObjectField, FormInputProcessor2)} instead. */
    @Deprecated
    protected void updateField(State state, HttpServletRequest request, ObjectField field, FormInputProcessor processor) {
        String fieldName = field.getInternalName();
        String inputName = state.getId() + "/" + fieldName;

        if (processor == null) {
            processor = getDefaultInputProcessor();
        }

        state.put(fieldName, processor.update(inputName, field, request));
    }
}
