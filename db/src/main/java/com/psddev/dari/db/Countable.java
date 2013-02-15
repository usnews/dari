package com.psddev.dari.db;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;

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
    @ObjectField.AnnotationProcessorClass(DimensionProcessor.class)
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

        public static ObjectField getCountField(Class<? extends Modification<? extends Countable>> modificationClass) {
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
                if (getCountField(modificationClass) != null) {
                    // also increment the summary field so it can be immediately available, 
                    // even though CountRecord will (probably) do the actual update of the 
                    // summary field in the database.
                    String fieldName = getCountField(modificationClass).getInternalName();
                    int oldCountSummary = (Integer) getState().get(fieldName);
                    getState().put(fieldName, oldCountSummary + c);
                }
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

        Map<ObjectField, Object> getDimensions(Class<? extends Modification<? extends Countable>> modificationClass) {
            // Checking each field for @Dimension annotation
            Map<ObjectField, Object> dimensions = new HashMap<ObjectField, Object>();
            //dimensions.put("_id", getState().getId()); // 1 Implicit dimension - the record ID

            Class<?>[] objectClasses = {modificationClass, getState().getType().getObjectClass()};
            for (Class<?> objectClass : objectClasses) {
                ObjectType objectType = ObjectType.getInstance(objectClass);
                for (ObjectField objectField : objectType.getFields()) {
                    CountableFieldData countableFieldData = objectField.as(CountableFieldData.class);
                    if (countableFieldData.isDimension()) {
                        Object dimensionValue = getState().get(objectField.getInternalName());
                        if (dimensionValue == null) continue;
                        if (countableFieldData.getDimensionClasses() != null && countableFieldData.getDimensionClasses().size() > 0) {
                            if (countableFieldData.getDimensionClasses().contains(modificationClass.getName())) {
                                dimensions.put(objectField, dimensionValue);
                            }
                        } else {
                            dimensions.put(objectField, dimensionValue);
                        }
                    }
                }
            }

            return dimensions;
        }

        public CountRecord getCountRecord(Class<? extends Modification<? extends Countable>> modificationClass) {
            if (! countRecords.containsKey(modificationClass)) {
                ObjectField countField = getCountField(modificationClass);
                CountRecord countRecord = new CountRecord(this, countField.getUniqueName(), this.getDimensions(modificationClass));
                countRecord.setSummaryField(countField);
                countRecords.put(modificationClass, countRecord);
            }
            return countRecords.get(modificationClass);
        }

    }

    static class DimensionProcessor implements ObjectField.AnnotationProcessor<Dimension> {

        @Override
        public void process(ObjectType type, ObjectField field, Dimension annotation) {
            //field.getParent().setIndexes();
            SqlDatabase.FieldData fieldData = field.as(SqlDatabase.FieldData.class);
            fieldData.setIndexTable(CountRecord.Static.getIndexTable(field));
            fieldData.setIndexTableSameColumnNames(false);
            fieldData.setIndexTableSource(true);
            fieldData.setIndexTableReadOnly(true);

            CountableFieldData countableFieldData = field.as(CountableFieldData.class);
            Set<String> dimensionClasses = new HashSet<String>();
            for (Class<?> cls : annotation.value()) {
                dimensionClasses.add(cls.getName());
            }
            countableFieldData.setDimensionClasses(dimensionClasses);
            countableFieldData.setDimension(true);
        }
    }

    @FieldInternalNamePrefix("countable.")
    public static class CountableFieldData extends Modification<ObjectField> {

        private boolean dimension;
        private Set<String> dimensionClasses;

        public boolean isDimension() {
            return dimension;
        }

        public void setDimension(boolean dimension) {
            this.dimension = dimension;
        }

        public Set<String> getDimensionClasses() {
            return dimensionClasses;
        }

        public void setDimensionClasses(Set<String> dimensionClasses) {
            this.dimensionClasses = dimensionClasses;
        }

    }

}
