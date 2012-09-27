package com.psddev.dari.db;

import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public interface Recordable {

    /**
     * Returns the state {@linkplain State#linkObject linked} to this
     * instance.
     */
    public State getState();

    /**
     * Sets the state {@linkplain State#linkObject linked} to this
     * instance. This method must also {@linkplain State#unlinkObject
     * unlink} the state previously set.
     */
    public void setState(State state);

    // --- Annotations ---

    /**
     * Specifies whether the target type is abstract and can't be used
     * to create a concrete instance.
     */
    @Documented
    @ObjectType.AnnotationProcessorClass(AbstractProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Abstract {
        boolean value() default true;
    }

    /** Specifies the maximum number of items allowed in the target field. */
    @Documented
    @ObjectField.AnnotationProcessorClass(CollectionMaximumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface CollectionMaximum {
        int value();
    }

    /** Specifies the minimum number of items required in the target field. */
    @Documented
    @ObjectField.AnnotationProcessorClass(CollectionMinimumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface CollectionMinimum {
        int value();
    }

    /**
     * Specifies whether the target field is always denormalized within
     * another instance.
     */
    @Documented
    @Inherited
    @ObjectField.AnnotationProcessorClass(DenormalizedProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Denormalized {
        boolean value() default true;
    }

    /** Specifies the target's display name. */
    @Documented
    @ObjectField.AnnotationProcessorClass(DisplayNameProcessor.class)
    @ObjectType.AnnotationProcessorClass(DisplayNameProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.TYPE })
    public @interface DisplayName {
        String value();
    }

    /**
     * Specifies whether the target data is always embedded within
     * another instance.
     */
    @Documented
    @Inherited
    @ObjectField.AnnotationProcessorClass(EmbeddedProcessor.class)
    @ObjectType.AnnotationProcessorClass(EmbeddedProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.TYPE })
    public @interface Embedded {
        boolean value() default true;
    }

    /**
     * Specifies the prefix for the internal names of all fields in the
     * target type.
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface FieldInternalNamePrefix {
        String value();
    }

    /** Specifies whether the target field is ignored. */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Ignored {
        boolean value() default true;
    }

    /** Specifies whether the target field value is indexed. */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Indexed {
        String[] extraFields() default { };
        boolean unique() default false;
        boolean caseSensitive() default false;

        /** @deprecated Use {@link #unique} instead. */
        @Deprecated
        boolean isUnique() default false;
    }

    /** Specifies the target's internal name. */
    @Documented
    @ObjectType.AnnotationProcessorClass(InternalNameProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.TYPE })
    public @interface InternalName {
        String value();
    }

    /**
     * Specifies the field names that are used to retrieve the
     * labels of the objects represented by the target type.
     */
    @Documented
    @Inherited
    @ObjectType.AnnotationProcessorClass(LabelFieldsProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface LabelFields {
        String[] value();
    }

    /**
     * Specifies either the maximum numeric value or string length of the
     * target field.
     */
    @Documented
    @ObjectField.AnnotationProcessorClass(MaximumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Maximum {
        double value();
    }

    /**
     * Specifies either the minimum numeric value or string length of the
     * target field.
     */
    @Documented
    @ObjectField.AnnotationProcessorClass(MinimumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Minimum {
        double value();
    }

    /**
     * Specifies the field name used to retrieve the previews of the
     * objects represented by the target type.
     */
    @Documented
    @Inherited
    @ObjectType.AnnotationProcessorClass(PreviewFieldProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface PreviewField {
        String value();
    }

    /**
     * Specifies the regular expression pattern that the target field value
     * must match.
     */
    @Documented
    @ObjectField.AnnotationProcessorClass(RegexProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Regex {
        String value();
    }

    /** Specifies whether the target field value is required. */
    @Documented
    @ObjectField.AnnotationProcessorClass(RequiredProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Required {
        boolean value() default true;
    }

    /**
     * Specifies the step between the minimum and the maximum that the
     * target field must match.
     */
    @Documented
    @ObjectField.AnnotationProcessorClass(StepProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Step {
        double value();
    }

    /** Specifies the valid types for the target field value. */
    @Documented
    @ObjectField.AnnotationProcessorClass(TypesProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Types {
        Class<?>[] value();
    }

    /** Specifies the valid values for the target field value. */
    @Documented
    @ObjectField.AnnotationProcessorClass(ValuesProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Values {
        String[] value();
    }

    @Documented
    @Inherited
    @ObjectField.AnnotationProcessorClass(WhereProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Where {
        String value();
    }

    // --- Deprecated ---
    //
    /** @deprecated Use {@link Denormalized} instead. */
    @Deprecated
    @Documented
    @Inherited
    @ObjectType.AnnotationProcessorClass(DenormalizedFieldsProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface DenormalizedFields {
        String[] value();
    }

    /** @deprecated Use {@link CollectionMaximum} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(CollectionMaximumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldCollectionMaximum {
        int value();
    }

    /** @deprecated Use {@link CollectionMinimum} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(CollectionMinimumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldCollectionMinimum {
        int value();
    }

    /** @deprecated Use {@link DisplayName} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(DisplayNameProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldDisplayName {
        String value();
    }

    /** @deprecated Use {@link Embedded} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(EmbeddedProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldEmbedded {
        boolean value() default true;
    }

    /** @deprecated Use {@link Modification} instead. */
    @Deprecated
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldGlobal {
    }

    /** @deprecated Use {@link Ignored} instead. */
    @Deprecated
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldIgnored {
    }

    /** @deprecated Use {@link Indexed} instead. */
    @Deprecated
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldIndexed {
        String[] extraFields() default { };
        boolean isUnique() default false;
    }

    /** @deprecated Use {@link InternalName} instead. */
    @Deprecated
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.METHOD })
    public @interface FieldInternalName {
        String value();
    }

    /** @deprecated Use {@link FieldTypes} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(TypesProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldItemTypes {
        Class<?>[] value();
    }

    /** @deprecated Use {@link Maximum} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(MaximumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldMaximum {
        double value();
    }

    /** @deprecated Use {@link Minimum} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(MinimumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldMinimum {
        double value();
    }

    /** @deprecated Use {@link Regex} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(RegexProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldPattern {
        String value();
    }

    /** @deprecated Use {@link Required} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(RequiredProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldRequired {
        boolean value() default true;
    }

    /** @deprecated Use {@link Step} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(StepProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldStep {
        double value();
    }

    /** @deprecated Use {@link Types} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(TypesProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldTypes {
        Class<?>[] value();
    }

    /** @deprecated Use {@link FieldIndexed} with {@code isUnique} instead. */
    @Deprecated
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldUnique {
    }

    /** @deprecated Use {@link Values} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(ValuesProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldValues {
        String[] value();
    }
}

// --- Internal ---

class AbstractProcessor implements ObjectType.AnnotationProcessor<Recordable.Abstract> {
    @Override
    public void process(ObjectType type, Recordable.Abstract annotation) {
        type.setAbstract(annotation.value());
    }
}

class CollectionMaximumProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        if (field.isInternalCollectionType()) {
            field.setCollectionMaximum(annotation instanceof Recordable.FieldCollectionMaximum ?
                    ((Recordable.FieldCollectionMaximum) annotation).value() :
                    ((Recordable.CollectionMaximum) annotation).value());
        } else {
            throw new IllegalArgumentException(String.format(
                    "[%s] annotation cannot be applied to a non-collection field!",
                    annotation.getClass().getName()));
        }
    }
}

class CollectionMinimumProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        if (field.isInternalCollectionType()) {
            field.setCollectionMinimum(annotation instanceof Recordable.FieldCollectionMinimum ?
                    ((Recordable.FieldCollectionMinimum) annotation).value() :
                    ((Recordable.CollectionMinimum) annotation).value());
        } else {
            throw new IllegalArgumentException(String.format(
                    "[%s] annotation cannot be applied to a non-collection field!",
                    annotation.getClass().getName()));
        }
    }
}

class DenormalizedProcessor implements ObjectField.AnnotationProcessor<Recordable.Denormalized> {
    @Override
    public void process(ObjectType type, ObjectField field, Recordable.Denormalized annotation) {
        field.setDenormalized(annotation.value());
    }
}

class DenormalizedFieldsProcessor implements ObjectType.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, Annotation annotation) {
        for (String fieldName : ((Recordable.DenormalizedFields) annotation).value()) {
            ObjectField field = type.getField(fieldName);
            if (field != null) {
                field.setDenormalized(true);
            }
        }
    }
}

class DisplayNameProcessor implements
        ObjectField.AnnotationProcessor<Annotation>,
        ObjectType.AnnotationProcessor<Recordable.DisplayName> {

    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setDisplayName(annotation instanceof Recordable.FieldDisplayName ?
                ((Recordable.FieldDisplayName) annotation).value() :
                ((Recordable.DisplayName) annotation).value());
    }

    @Override
    public void process(ObjectType type, Recordable.DisplayName annotation) {
        Class<?> objectClass = type.getObjectClass();
        if (objectClass != null) {
            Recordable.DisplayName displayName = objectClass.getAnnotation(Recordable.DisplayName.class);
            // Only sets the display name if the annotation came from the type being modified.
            if (displayName != null && displayName.value() != null && displayName.value().equals(annotation.value())) {
                type.setDisplayName(annotation.value());
            }
        }
    }
}

class EmbeddedProcessor implements
        ObjectField.AnnotationProcessor<Annotation>,
        ObjectType.AnnotationProcessor<Recordable.Embedded> {

    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setEmbedded(annotation instanceof Recordable.FieldEmbedded ?
                ((Recordable.FieldEmbedded) annotation).value() :
                ((Recordable.Embedded) annotation).value());
    }

    @Override
    public void process(ObjectType type, Recordable.Embedded annotation) {
        type.setEmbedded(annotation.value());
    }
}

class InternalNameProcessor implements ObjectType.AnnotationProcessor<Recordable.InternalName> {
    @Override
    public void process(ObjectType type, Recordable.InternalName annotation) {
        type.setInternalName(annotation.value());
    }
}

class LabelFieldsProcessor implements ObjectType.AnnotationProcessor<Recordable.LabelFields> {
    @Override
    public void process(ObjectType type, Recordable.LabelFields annotation) {
        List<String> labelFields = type.getLabelFields();
        for (String field : annotation.value()) {
            if (!labelFields.contains(field)) {
                labelFields.add(field);
            }
        }
    }
}

class MaximumProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setMaximum(annotation instanceof Recordable.FieldMaximum ?
                ((Recordable.FieldMaximum) annotation).value() :
                ((Recordable.Maximum) annotation).value());
    }
}

class MinimumProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setMinimum(annotation instanceof Recordable.FieldMinimum ?
                ((Recordable.FieldMinimum) annotation).value() :
                ((Recordable.Minimum) annotation).value());
    }
}

class PreviewFieldProcessor implements ObjectType.AnnotationProcessor<Recordable.PreviewField> {
    @Override
    public void process(ObjectType type, Recordable.PreviewField annotation) {
        String fieldName = annotation.value();
        ObjectField field = type.getField(fieldName);
        if (field != null && ObjectField.FILE_TYPE.equals(field.getInternalType())) {
            type.setPreviewField(fieldName);
        } else {
            throw new IllegalArgumentException(
                    "[%s] annotation cannot be applied to a non-file field!");
        }
    }
}

class RegexProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setPattern(annotation instanceof Recordable.FieldPattern ?
                ((Recordable.FieldPattern) annotation).value() :
                ((Recordable.Regex) annotation).value());
    }
}

class RequiredProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setRequired(annotation instanceof Recordable.FieldRequired ?
                ((Recordable.FieldRequired) annotation).value() :
                ((Recordable.Required) annotation).value());
    }
}

class StepProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setStep(annotation instanceof Recordable.FieldStep ?
                ((Recordable.FieldStep) annotation).value() :
                ((Recordable.Step) annotation).value());
    }
}

class TypesProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        Set<ObjectType> types = new LinkedHashSet<ObjectType>();
        DatabaseEnvironment environment = field.getParent().getEnvironment();
        for (Class<?> typeClass : annotation instanceof Recordable.FieldTypes ? ((Recordable.FieldTypes) annotation).value() :
                annotation instanceof Recordable.FieldItemTypes ? ((Recordable.FieldItemTypes) annotation).value() :
                ((Recordable.Types) annotation).value()) {
            types.add(environment.getTypeByClass(typeClass));
        }
        field.setTypes(types);
    }
}

class ValuesProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        Set<ObjectField.Value> values = new LinkedHashSet<ObjectField.Value>();
        for (String valueValue : annotation instanceof Recordable.FieldValues ?
                ((Recordable.FieldValues) annotation).value() :
                ((Recordable.Values) annotation).value()) {
            ObjectField.Value value = new ObjectField.Value();
            value.setValue(valueValue);
            values.add(value);
        }
        field.setValues(values);
    }
}

class WhereProcessor implements ObjectField.AnnotationProcessor<Recordable.Where> {
    @Override
    public void process(ObjectType type, ObjectField field, Recordable.Where annotation) {
        field.setPredicate(annotation.value());
    }
}