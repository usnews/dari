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

public interface Countable extends Recordable {

    /** Specifies whether the target field value is indexed in the CountRecord dimension tables. 
     * This field's value will not be loaded or saved into the state. */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @ObjectField.AnnotationProcessorClass(DimensionProcessor.class)
    @Target(ElementType.FIELD)
    public @interface Dimension {
    }

    /** Specifies the field the count is recorded in */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @ObjectField.AnnotationProcessorClass(CountFieldProcessor.class)
    @Target(ElementType.FIELD)
    public @interface CountField {
    }

    /** Specifies that the target field virtually represents the EventDate field and optionally an SQL date format, and can be queried against. 
     * This field's value will not be loaded or saved into the state. */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @ObjectField.AnnotationProcessorClass(EventDateProcessor.class)
    @Target(ElementType.FIELD)
    public @interface EventDate {
        CountRecord.EventDatePrecision value() default CountRecord.EventDatePrecision.HOUR;
    }

    public static class CountAction extends Modification<Countable> {

        private transient Map<Class<?>, CountRecord> countRecords = new HashMap<Class<?>, CountRecord>();

        public static ObjectField getCountField(Class<? extends Modification<? extends Countable>> modificationClass) {
            ObjectType modificationType = ObjectType.getInstance(modificationClass);
            for (ObjectField objectField : modificationType.getFields()) {
                if (objectField.as(CountableFieldData.class).isCountField()) {
                    return objectField;
                }
            }
            throw new RuntimeException("One int field must be marked as @Countable.CountField");
        }

        public static ObjectField getEventDateField(Class<? extends Modification<? extends Countable>> modificationClass) {
            ObjectType modificationType = ObjectType.getInstance(modificationClass);
            for (ObjectField objectField : modificationType.getFields()) {
                if (objectField.as(CountableFieldData.class).isEventDateField()) {
                    return objectField;
                }
            }
            return null;
        }

        public void incrementCount(Class<? extends Modification<? extends Countable>> modificationClass, int c) {
            try {
                getCountRecord(modificationClass).incrementCount(c);
                if (getCountField(modificationClass) != null) {
                    // also increment the summary field in the state so it can
                    // be immediately available, even though CountRecord will
                    // (probably) do the actual update of the summary field in
                    // the database.
                    String fieldName = getCountField(modificationClass).getInternalName();
                    int oldCountSummary = (Integer) getState().get(fieldName);
                    getState().put(fieldName, oldCountSummary + c);
                }
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(modificationClass).getDatabase(), "Error in CountRecord.incrementCount() : " + e.getLocalizedMessage());
            }
        }

        public void setCount(Class<? extends Modification<? extends Countable>> modificationClass, int c) {
            try {
                getCountRecord(modificationClass).setCount(c);
                if (getCountField(modificationClass) != null) {
                    // also set the summary field in the state so it can be
                    // immediately available, even though CountRecord will
                    // (probably) do the actual update of the summary field in
                    // the database.
                    String fieldName = getCountField(modificationClass).getInternalName();
                    getState().put(fieldName, c);
                }
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(modificationClass).getDatabase(), "Error in CountRecord.setCount() : " + e.getLocalizedMessage());
            }
        }

        public void deleteCount(Class<? extends Modification<? extends Countable>> modificationClass) {
            try {
                getCountRecord(modificationClass).deleteCount();
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(modificationClass).getDatabase(), "Error in CountRecord.deleteCount() : " + e.getLocalizedMessage());
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

        Set<ObjectField> getDimensions(Class<? extends Modification<? extends Countable>> modificationClass) {
            // Checking each field for @Dimension annotation
            Set<ObjectField> dimensions = new HashSet<ObjectField>();

            ObjectType objectType = ObjectType.getInstance(getState().getType().getObjectClass());
            for (ObjectField objectField : objectType.getFields()) {
                CountableFieldData countableFieldData = objectField.as(CountableFieldData.class);
                if (countableFieldData.isDimension()) {
                    dimensions.add(objectField);
                }
            }

            return dimensions;
        }

        public CountRecord getCountRecord(Class<? extends Modification<? extends Countable>> modificationClass) {
            if (! countRecords.containsKey(modificationClass)) {
                ObjectField countField = getCountField(modificationClass);
                ObjectField eventDateField = getEventDateField(modificationClass);
                CountRecord countRecord = new CountRecord(this, countField.getUniqueName(), this.getDimensions(modificationClass));
                if (eventDateField != null) {
                    countRecord.setEventDatePrecision(eventDateField.as(CountableFieldData.class).getEventDatePrecision());
                } else {
                    countRecord.setEventDatePrecision(CountRecord.EventDatePrecision.NONE);
                }
                countRecord.setSummaryField(countField);
                countRecords.put(modificationClass, countRecord);
            }
            return countRecords.get(modificationClass);
        }

    }

    static class DimensionProcessor implements ObjectField.AnnotationProcessor<Dimension> {

        @Override
        public void process(ObjectType type, ObjectField field, Dimension annotation) {
            SqlDatabase.FieldData fieldData = field.as(SqlDatabase.FieldData.class);
            fieldData.setIndexTable(CountRecord.Static.getIndexTable(field));
            fieldData.setIndexTableSameColumnNames(false);
            fieldData.setIndexTableSource(true);
            fieldData.setIndexTableReadOnly(true);

            CountableFieldData countableFieldData = field.as(CountableFieldData.class);
            countableFieldData.setDimension(true);
        }
    }

    static class CountFieldProcessor implements ObjectField.AnnotationProcessor<CountField> {

        @Override
        public void process(ObjectType type, ObjectField field, CountField annotation) {

            SqlDatabase.FieldData fieldData = field.as(SqlDatabase.FieldData.class);
            fieldData.setIndexTable("CountRecordSummary");
            fieldData.setIndexTableSameColumnNames(false);
            fieldData.setIndexTableSource(true);
            fieldData.setIndexTableReadOnly(true);

            CountableFieldData countableFieldData = field.as(CountableFieldData.class);
            countableFieldData.setCountField(true);
        }
    }

    static class EventDateProcessor implements ObjectField.AnnotationProcessor<EventDate> {

        @Override
        public void process(ObjectType type, ObjectField field, EventDate annotation) {
            SqlDatabase.FieldData fieldData = field.as(SqlDatabase.FieldData.class);
            //fieldData.setIndexTable("CountRecord");
            //fieldData.setIndexTableSameColumnNames(false);
            fieldData.setIndexTableColumnName("eventDate");
            fieldData.setIndexTableSource(true);
            fieldData.setIndexTableReadOnly(true);

            CountableFieldData countableFieldData = field.as(CountableFieldData.class);
            countableFieldData.setDimension(true);
            countableFieldData.setEventDatePrecision(annotation.value());

            countableFieldData.setEventDateField(true);
        }
    }

    @FieldInternalNamePrefix("countable.")
    public static class CountableFieldData extends Modification<ObjectField> {

        private boolean dimension;
        private boolean countField;
        private boolean eventDateField;
        private CountRecord.EventDatePrecision eventDatePrecision;

        public boolean isDimension() {
            return dimension;
        }

        public void setDimension(boolean dimension) {
            this.dimension = dimension;
        }

        public boolean isCountField() {
            return countField;
        }

        public void setCountField(boolean countField) {
            this.countField = countField;
        }

        public boolean isEventDateField() {
            return eventDateField;
        }

        public void setEventDateField(boolean eventDateField) {
            this.eventDateField = eventDateField;
        }

        public CountRecord.EventDatePrecision getEventDatePrecision() {
            return eventDatePrecision;
        }

        public void setEventDatePrecision(
                CountRecord.EventDatePrecision eventDatePrecision) {
            this.eventDatePrecision = eventDatePrecision;
        }

    }

}
