package com.psddev.dari.db;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

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
        String[] dimensions() default {};
        String eventDate() default "";
        boolean includeSelfDimension() default true;
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

        //private static final Logger LOGGER = LoggerFactory.getLogger(CountAction.class);

        private transient Map<Class<?>, Map<String, CountRecord>> countRecords = new HashMap<Class<?>, Map<String, CountRecord>>();

        private static ObjectField getCountField(Class<? extends Record> recordClass, String internalName) {
            ObjectType recordType = ObjectType.getInstance(recordClass);
            for (ObjectField objectField : recordType.getFields()) {
                if (objectField.as(CountableFieldData.class).isCountField() && 
                        (internalName == null || internalName.equals(objectField.getInternalName()))) {
                    return objectField;
                }
            }
            throw new RuntimeException("At least one numeric field must be marked as @Countable.CountField");
        }

        private static ObjectField getEventDateField(Class<? extends Record> recordClass, String countFieldInternalName) {
            ObjectType recordType = ObjectType.getInstance(recordClass);
            ObjectField countField = getCountField(recordClass, countFieldInternalName);
            String eventDateFieldName = countField.as(CountableFieldData.class).getEventDateFieldName();
            if (eventDateFieldName != null) {
                ObjectField eventDateField = recordType.getField(eventDateFieldName);
                if (eventDateField.as(CountableFieldData.class).isEventDateField()) {
                    return eventDateField;
                } else {
                    throw new RuntimeException("The field " + eventDateFieldName + " is not annotated as @EventDate.");
                }
            } else {
                return null;
            }
        }

        private Set<ObjectField> getDimensions(Class<? extends Record> recordClass, String countFieldInternalName) {
            // Checking each field for @Dimension annotation
            Set<ObjectField> dimensions = new HashSet<ObjectField>();
            ObjectField countField = getCountField(recordClass, countFieldInternalName);
            ObjectType objectType = ObjectType.getInstance(getState().getType().getObjectClass());
            for (String dimensionFieldName : countField.as(CountableFieldData.class).getDimensions()) {
                if (objectType.getField(dimensionFieldName) == null) {
                    throw new RuntimeException("Invalid dimension field : " + dimensionFieldName);
                }
                dimensions.add(objectType.getField(dimensionFieldName));
            }
            return dimensions;
        }

        public void incrementCount(Class<? extends Record> recordClass, double c) {
            incrementCount(recordClass, null, c);
        }

        public void incrementCount(Class<? extends Record> recordClass, String countFieldInternalName, double c) {
            try {
                getCountRecord(recordClass, countFieldInternalName).incrementCount(c);
                if (getCountField(recordClass, countFieldInternalName) != null) {
                    // also increment the summary field in the state so it can
                    // be immediately available, even though CountRecord will
                    // (probably) do the actual update of the summary field in
                    // the database.
                    String fieldName = getCountField(recordClass, countFieldInternalName).getInternalName();
                    double oldCountSummary = (Double) getState().get(fieldName);
                    getState().put(fieldName, oldCountSummary + c);
                }
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(recordClass, countFieldInternalName).getDatabase(), "Error in CountRecord.incrementCount() : " + e.getLocalizedMessage());
            }
        }

        public void setCount(Class<? extends Record> recordClass, double c) {
            setCount(recordClass, null, c);
        }

        public void setCount(Class<? extends Record> recordClass, String countFieldInternalName, double c) {
            try {
                getCountRecord(recordClass, countFieldInternalName).setCount(c);
                if (getCountField(recordClass, countFieldInternalName) != null) {
                    // also set the summary field in the state so it can be
                    // immediately available, even though CountRecord will
                    // (probably) do the actual update of the summary field in
                    // the database.
                    String fieldName = getCountField(recordClass, countFieldInternalName).getInternalName();
                    getState().put(fieldName, c);
                }
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(recordClass, countFieldInternalName).getDatabase(), "Error in CountRecord.setCount() : " + e.getLocalizedMessage());
            }
        }

        public void deleteCount(Class<? extends Record> recordClass) {
            deleteCount(recordClass, null);
        }

        public void deleteCount(Class<? extends Record> recordClass, String countFieldInternalName) {
            try {
                getCountRecord(recordClass, countFieldInternalName).deleteCount();
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(recordClass, countFieldInternalName).getDatabase(), "Error in CountRecord.deleteCount() : " + e.getLocalizedMessage());
            }
        }

        public double getCount(Class<? extends Record> recordClass) {
            return getCount(recordClass, null);
        }

        public double getCount(Class<? extends Record> recordClass, String countFieldInternalName) {
            try {
                return getCountRecord(recordClass, countFieldInternalName).getCount();
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(recordClass, countFieldInternalName).getDatabase(), "Error in CountRecord.getCount() : " + e.getLocalizedMessage());
            }
        }

        public void syncCountSummary(Class<? extends Record> recordClass) {
            syncCountSummary(recordClass, null);
        }

        public void syncCountSummary(Class<? extends Record> recordClass, String countFieldInternalName) {
            try {
                getCountRecord(recordClass, countFieldInternalName).syncCountSummary();
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(recordClass, countFieldInternalName).getDatabase(), "Error in CountRecord.getCount() : " + e.getLocalizedMessage());
            }
        }

        /*
        public CountRecord getCountRecord(Class<? extends Record> recordClass) {
            ObjectField countField = getCountField(recordClass, null);
            return getCountRecord(recordClass, countField.getInternalName());
        }
        */

        public void invalidateCountRecord(Class<? extends Record> recordClass, String countFieldInternalName) {
            try {
                this.countRecords.get(recordClass).remove(countFieldInternalName);
            } catch (Exception ex) { }
        }

        public CountRecord getCountRecord(Class<? extends Record> recordClass, String countFieldInternalName) {
            // if countFieldInternalName is null, it will return the *first* @CountField in the type
            if (! countRecords.containsKey(recordClass)) {
                Map<String, CountRecord> countRecordMap = new HashMap<String, CountRecord>();
                countRecords.put(recordClass, countRecordMap);
            }
            if (! countRecords.get(recordClass).containsKey(countFieldInternalName)) {
                ObjectField countField = getCountField(recordClass, countFieldInternalName);
                ObjectField eventDateField = getEventDateField(recordClass, countFieldInternalName);
                CountRecord countRecord = new CountRecord(this, countField.getUniqueName(), this.getDimensions(recordClass, countFieldInternalName));
                if (eventDateField != null) {
                    countRecord.setEventDatePrecision(eventDateField.as(CountableFieldData.class).getEventDatePrecision());
                } else {
                    countRecord.setEventDatePrecision(CountRecord.EventDatePrecision.NONE);
                }
                if (countField.as(CountableFieldData.class).isIncludeSelfDimension()) {
                    countRecord.setIncludeSelfDimension(true);
                }
                countRecord.setSummaryField(countField);
                countRecords.get(recordClass).put(countFieldInternalName, countRecord);
            }
            return countRecords.get(recordClass).get(countFieldInternalName);
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
            Set<String> dimensions = new HashSet<String>(Arrays.asList(annotation.dimensions()));

            if (annotation.includeSelfDimension()) {
                countableFieldData.setIncludeSelfDimension(true);
            }
            countableFieldData.setDimensions(dimensions);
            if (annotation.eventDate().equals("")) {
                countableFieldData.setEventDateFieldName(null);
            } else {
                countableFieldData.setEventDateFieldName(annotation.eventDate());
            }
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
        private boolean includeSelfDimension;
        private CountRecord.EventDatePrecision eventDatePrecision;
        private Set<String> dimensions = new HashSet<String>();
        private String eventDateFieldName;

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

        public boolean isIncludeSelfDimension() {
            return includeSelfDimension;
        }

        public void setIncludeSelfDimension(boolean includeSelfDimension) {
            this.includeSelfDimension = includeSelfDimension;
        }

        public CountRecord.EventDatePrecision getEventDatePrecision() {
            return eventDatePrecision;
        }

        public void setEventDatePrecision(CountRecord.EventDatePrecision eventDatePrecision) {
            this.eventDatePrecision = eventDatePrecision;
        }

        public Set<String> getDimensions() {
            return dimensions;
        }

        public void setDimensions(Set<String> dimensions) {
            this.dimensions = dimensions;
        }

        public String getEventDateFieldName() {
            return eventDateFieldName;
        }

        public void setEventDateFieldName(String eventDateFieldName) {
            this.eventDateFieldName = eventDateFieldName;
        }

    }

}
