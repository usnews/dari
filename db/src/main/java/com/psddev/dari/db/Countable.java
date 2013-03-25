package com.psddev.dari.db;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Countable extends Recordable {

    @FieldInternalNamePrefix("countable.")
    public static class CountableFieldData extends Modification<ObjectField> {

        private boolean dimension;
        private boolean countField;
        private boolean eventDateField;
        private boolean includeSelfDimension;
        private Record.CountEventDatePrecision eventDatePrecision;
        private Set<String> dimensions = new HashSet<String>();
        private String eventDateFieldName;
        private String recordIdJoinTableName;
        private String recordIdJoinColumnName;

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

        public Record.CountEventDatePrecision getEventDatePrecision() {
            return eventDatePrecision;
        }

        public void setEventDatePrecision(Record.CountEventDatePrecision eventDatePrecision) {
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

        public String getRecordIdJoinTableName() {
            return recordIdJoinTableName;
        }

        public void setRecordIdJoinTableName(String recordIdJoinTableName) {
            this.recordIdJoinTableName = recordIdJoinTableName;
        }

        public String getRecordIdJoinColumnName() {
            return recordIdJoinColumnName;
        }

        public void setRecordIdJoinColumnName(String recordIdJoinColumnName) {
            this.recordIdJoinColumnName = recordIdJoinColumnName;
        }

    }

    public static class CountAction extends Modification<Recordable> {

        public static final Logger LOGGER = LoggerFactory.getLogger(CountAction.class);

        private transient Map<Class<?>, Map<String, CountRecord>> countRecords = new HashMap<Class<?>, Map<String, CountRecord>>();

        private static ObjectField getCountField(Class<? extends Record> recordClass, String internalName) {
            ObjectType recordType = ObjectType.getInstance(recordClass);
            if (internalName == null) {
                for (ObjectField objectField : recordType.getFields()) {
                    if (objectField.as(CountableFieldData.class).isCountField()) {
                        return objectField;
                    }
                }
            } else {
                ObjectField objectField = recordType.getField(internalName);
                if (objectField != null && objectField.as(CountableFieldData.class).isCountField()) {
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
                if (eventDateField == null) {
                    throw new RuntimeException("Invalid eventDate field : " + eventDateFieldName);
                }
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

        public void setCount(Class<? extends Record> recordClass, String countFieldInternalName, double c) {
            try {
                getCountRecord(recordClass, countFieldInternalName).setCount(c);
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(recordClass, countFieldInternalName).getDatabase(), "Error in CountRecord.setCount() : " + e.getLocalizedMessage());
            }
        }

        public void deleteCounts(Class<? extends Record> recordClass) {
            try {
                getCountRecord(recordClass, null).deleteCounts();
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(recordClass, null).getDatabase(), "Error in CountRecord.deleteCounts() : " + e.getLocalizedMessage());
            }
        }

        protected double getCount(Class<? extends Record> recordClass, String countFieldInternalName) {
            try {
                CountRecord cr = getCountRecord(recordClass, countFieldInternalName);
                cr.setQueryDateRange(null, null);
                return getCountRecord(recordClass, countFieldInternalName).getCount();
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(recordClass, countFieldInternalName).getDatabase(), "Error in CountRecord.getCount() : " + e.getLocalizedMessage());
            }
        }

        protected double getCountSinceDate(Class<? extends Record> recordClass, String countFieldInternalName, Long startTimestamp) {
            return getCountOverDateRange(recordClass, countFieldInternalName, startTimestamp, null);
        }

        protected double getCountAsOfDate(Class<? extends Record> recordClass, String countFieldInternalName, Long endTimestamp) {
            return getCountOverDateRange(recordClass, countFieldInternalName, null, endTimestamp);
        }

        protected double getCountOverDateRange(Class<? extends Record> recordClass, String countFieldInternalName, Long startTimestamp, Long endTimestamp) {
            try {
                CountRecord cr = getCountRecord(recordClass, countFieldInternalName);
                if (cr.getEventDatePrecision().equals(Record.CountEventDatePrecision.NONE)) {
                    throw new RuntimeException("Date range does not apply for EventDatePrecision.NONE");
                }
                cr.setQueryDateRange(startTimestamp, endTimestamp);
                return getCountRecord(recordClass, countFieldInternalName).getCount();
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
                    countRecord.setEventDatePrecision(Record.CountEventDatePrecision.NONE);
                }
                if (countField.as(CountableFieldData.class).isIncludeSelfDimension()) {
                    countRecord.setIncludeSelfDimension(true);
                }
                countRecords.get(recordClass).put(countFieldInternalName, countRecord);
            }
            return countRecords.get(recordClass).get(countFieldInternalName);
        }

    }

}
