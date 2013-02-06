package com.psddev.dari.db;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.psddev.dari.util.TypeDefinition;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public interface Countable extends Recordable {

    /** Specifies whether the target field value is indexed in the CountRecord dimension tables. */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Dimension {
        Class<?>[] value() default {};
    }

    /** Specifies the */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface CountField {
    }

    public static class CountAction extends Modification<Countable> {

        //private static final Logger LOGGER = LoggerFactory.getLogger(CountAction.class);

        private transient Map<Class<?>, CountRecord> countRecords = new HashMap<Class<?>, CountRecord>();

        ObjectField getCountField(Class<? extends Modification<? extends Countable>> modificationClass) {
            //TypeDefinition<?> definition = TypeDefinition.getInstance(modificationClass);
            ObjectType modificationType = ObjectType.getInstance(modificationClass);
            for (ObjectField objectField : modificationType.getFields()) {
                Field javaField = objectField.getJavaField(modificationClass);
                boolean isCountField = javaField.isAnnotationPresent(CountField.class);
                if (isCountField) {
                    return objectField;
                }
            }
            throw new RuntimeException("One int field must be marked as @Countable.CountField");
        }

        public void incrementCount(Class<? extends Modification<? extends Countable>> modificationClass, int c) {
            try {
                getCountRecord(modificationClass).incrementCount(c);
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(modificationClass).getDatabase(), "Error in CountRecord.incrementCount() : " + e.getLocalizedMessage());
            }
        }

        public int getCount(Class<? extends Modification<? extends Countable>> modificationClass) {
            try {
                return getCountRecord(modificationClass).getCount();
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(modificationClass).getDatabase(), "Error in CountRecord.getCount() : " + e.getLocalizedMessage());
            }
        }

        public void syncCountSummary(Class<? extends Modification<? extends Countable>> modificationClass) {
            try {
                getCountRecord(modificationClass).syncCountSummary();
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(modificationClass).getDatabase(), "Error in CountRecord.getCount() : " + e.getLocalizedMessage());
            }
        }

        Map<String, Object> getDimensions(Class<? extends Modification<? extends Countable>> modificationClass) {
            // Checking each field for @Dimension annotation
            Map<String, Object> dimensions = new HashMap<String, Object>();
            dimensions.put("id", getState().getId()); // 1 Implicit dimension - the record ID

            Class<?>[] objectClasses = {modificationClass, getState().getType().getObjectClass()};
            for (Class<?> objectClass : objectClasses) {
                TypeDefinition<?> definition = TypeDefinition.getInstance(objectClass);
                Map<String, List<Field>> fields = definition.getAllSerializableFields();
                ObjectType objectType = ObjectType.getInstance(objectClass);
                for (ObjectField objectField : objectType.getFields()) {
                    List<Field> javaFields = fields.get(objectField.getJavaFieldName());
                    if (javaFields == null || javaFields.size() == 0) continue;
                    Field javaField = javaFields.get(javaFields.size()-1);
                    boolean isDimension = javaField.isAnnotationPresent(Dimension.class);
                    if (isDimension) {
                        Dimension dimensionAnnotation = javaField.getAnnotation(Dimension.class);
                        Object dimensionValue = getState().get(objectField.getInternalName());
                        if (dimensionValue != null) {
                            if (dimensionAnnotation.value().length == 0) {
                                dimensions.put(objectField.getInternalName(), dimensionValue);
                            } else {
                                for (Class<?> annotationValue : dimensionAnnotation.value()) {
                                    if (annotationValue == modificationClass) {
                                        dimensions.put(objectField.getInternalName(), dimensionValue);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // this shouldn't happen since we have the implicit "id" dimension
            if (dimensions.size() == 0) {
                throw new RuntimeException("Zero fields are marked as @Countable.Dimension");
            }
            return dimensions;
        }

        CountRecord getCountRecord(Class<? extends Modification<? extends Countable>> modificationClass) {
            if (! countRecords.containsKey(modificationClass)) {
                ObjectField countField = getCountField(modificationClass);
                CountRecord countRecord = new CountRecord(countField.getInternalName(), this.getDimensions(modificationClass));
                countRecord.setSummaryField(countField, getState().getId());
                countRecords.put(modificationClass, countRecord);
            }
            return countRecords.get(modificationClass);
        }

    }

}
