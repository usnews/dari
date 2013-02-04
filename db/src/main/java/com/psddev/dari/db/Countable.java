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

public interface Countable extends Recordable {

    /** Specifies whether the target field value is indexed. */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Dimension {
        Class<?>[] value() default {};
    }

    public static abstract class Action extends Modification<Countable> {
        private transient CountRecord countRecord;
        private transient Map<String, Object> dimensions;

        public abstract String getActionSymbol();

        public void setCount(int c) {
            try {
                getCountRecord().setCount(c);
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord().getDatabase(), "Error in CountRecord.setCount() : " + e.getLocalizedMessage());
            }
        }

        public void incrementCount(int c) {
            try {
                getCountRecord().incrementCount(c);
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord().getDatabase(), "Error in CountRecord.incrementCount() : " + e.getLocalizedMessage());
            }
        }

        public int getCount() {
            try {
                return getCountRecord().getCount();
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord().getDatabase(), "Error in CountRecord.getCount() : " + e.getLocalizedMessage());
            }
        }

        Map<String, Object> getDimensions() {
            if (dimensions == null) {
                // Checking each field for @Dimension annotation
                dimensions = new HashMap<String, Object>();
                dimensions.put("id", getState().getId()); // 1 Implicit dimension - the record ID
                TypeDefinition<?> definition = TypeDefinition.getInstance(getState().getType().getObjectClass());
                for (Map.Entry<String, List<Field>> entry : definition.getAllSerializableFields().entrySet()) {
                    Field javaField = entry.getValue().get(entry.getValue().size() - 1);
                    boolean isDimension = javaField.isAnnotationPresent(Dimension.class);
                    if (isDimension) {
                        Dimension dimensionAnnotation = javaField.getAnnotation(Dimension.class);
                        if (dimensionAnnotation.value().length == 0) {
                            dimensions.put(entry.getKey(), getState().get(entry.getKey()));
                        } else {
                            for (Class<?> annotationValue : dimensionAnnotation.value()) {
                                if (annotationValue.isInstance(this)) {
                                    dimensions.put(entry.getKey(), getState().get(entry.getKey()));
                                    break;
                                }
                            }
                        }
                    }
                }
                // this shouldn't happen since we have the implicit "id" dimension
                if (dimensions.size() == 0) {
                    throw new RuntimeException("Zero fields are marked as @Countable.Dimension");
                }
            }
            return dimensions;
        }

        CountRecord getCountRecord() {
            if (countRecord == null) {
                countRecord = new CountRecord(this.getActionSymbol(), this.getDimensions());
            }
            return countRecord;
        }

    }

}
