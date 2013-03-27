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
        private boolean countValue;
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

        public boolean isCountValue() {
            return countValue;
        }

        public void setCountValue(boolean countValue) {
            this.countValue = countValue;
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

        //private static final Logger LOGGER = LoggerFactory.getLogger(CountAction.class);

        private transient Map<String, CountRecord> countRecords = new HashMap<String, CountRecord>();
        private transient Set<Integer> dimensionHashCodes;;

        private ObjectField getCountField(String internalName) {
            ObjectType recordType = ObjectType.getInstance(this.getOriginalObject().getClass());
            if (internalName == null) {
                for (ObjectField objectField : recordType.getFields()) {
                    if (objectField.as(CountableFieldData.class).isCountValue()) {
                        return objectField;
                    }
                }
            } else {
                ObjectField objectField = recordType.getField(internalName);
                if (objectField != null && objectField.as(CountableFieldData.class).isCountValue()) {
                    return objectField;
                }
            }
            throw new RuntimeException("At least one numeric field must be marked as @Countable.CountField");
        }

        private ObjectField getEventDateField(String countFieldInternalName) {
            ObjectType recordType = ObjectType.getInstance(this.getOriginalObject().getClass());
            ObjectField countField = getCountField(countFieldInternalName);
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

        private Set<ObjectField> getDimensions(String countFieldInternalName) {
            // Checking each field for @Dimension annotation
            Set<ObjectField> dimensions = new HashSet<ObjectField>();
            ObjectField countField = getCountField(countFieldInternalName);
            ObjectType objectType = ObjectType.getInstance(getState().getType().getObjectClass());
            for (String dimensionFieldName : countField.as(CountableFieldData.class).getDimensions()) {
                if (objectType.getField(dimensionFieldName) == null) {
                    throw new RuntimeException("Invalid dimension field : " + dimensionFieldName);
                }
                dimensions.add(objectType.getField(dimensionFieldName));
            }
            return dimensions;
        }

        private boolean dimensionValuesHaveChanged(String countFieldInternalName) {
            Set<Integer> newDimensionHashCodes = new HashSet<Integer>();
            for (ObjectField field : this.getDimensions(countFieldInternalName)) {
                if (this.getState().getByPath(field.getInternalName()) != null) {
                    newDimensionHashCodes.add(this.getState().getByPath(field.getInternalName()).hashCode());
                }
            }
            if (dimensionHashCodes == null || ! newDimensionHashCodes.equals(dimensionHashCodes)) {
                dimensionHashCodes = newDimensionHashCodes;
                return true;
            } else {
                return false;
            }
        }

        public void incrementCount(String countFieldInternalName, double c) {
            try {
                getCountRecord(countFieldInternalName).incrementCount(c);
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(countFieldInternalName).getDatabase(), "Error in CountRecord.incrementCount() : " + e.getLocalizedMessage());
            }
        }

        public void setCount(String countFieldInternalName, double c) {
            try {
                getCountRecord(countFieldInternalName).setCount(c);
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(countFieldInternalName).getDatabase(), "Error in CountRecord.setCount() : " + e.getLocalizedMessage());
            }
        }

        public void deleteCounts() {
            try {
                getCountRecord(null).deleteCounts();
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(null).getDatabase(), "Error in CountRecord.deleteCounts() : " + e.getLocalizedMessage());
            }
        }

        public double getCount(String countFieldInternalName) {
            try {
                CountRecord cr = getCountRecord(countFieldInternalName);
                cr.setQueryDateRange(null, null);
                return getCountRecord(countFieldInternalName).getCount();
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(countFieldInternalName).getDatabase(), "Error in CountRecord.getCount() : " + e.getLocalizedMessage());
            }
        }

        public double getCountByRecordId(String countFieldInternalName) {
            try {
                CountRecord cr = getCountRecord(countFieldInternalName);
                cr.setQueryDateRange(null, null);
                return getCountRecord(countFieldInternalName).getCountByRecordId();
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(countFieldInternalName).getDatabase(), "Error in CountRecord.getCountByRecordId() : " + e.getLocalizedMessage());
            }
        }

        public double getCountSinceDate(String countFieldInternalName, Long startTimestamp) {
            return getCountOverDateRange(countFieldInternalName, startTimestamp, null);
        }

        public double getCountAsOfDate(String countFieldInternalName, Long endTimestamp) {
            return getCountOverDateRange(countFieldInternalName, null, endTimestamp);
        }

        public double getCountOverDateRange(String countFieldInternalName, Long startTimestamp, Long endTimestamp) {
            try {
                CountRecord cr = getCountRecord(countFieldInternalName);
                if (cr.getEventDatePrecision().equals(Record.CountEventDatePrecision.NONE)) {
                    throw new RuntimeException("Date range does not apply for EventDatePrecision.NONE");
                }
                cr.setQueryDateRange(startTimestamp, endTimestamp);
                return getCountRecord(countFieldInternalName).getCount();
            } catch (SQLException e) {
                throw new DatabaseException(getCountRecord(countFieldInternalName).getDatabase(), "Error in CountRecord.getCount() : " + e.getLocalizedMessage());
            }
        }

        public CountRecord getCountRecord(String countFieldInternalName) {
            // if countFieldInternalName is null, it will return the *first* @CountField in the type

            if (dimensionValuesHaveChanged(countFieldInternalName)) {
                if (countRecords.containsKey(countFieldInternalName)) {
                    countRecords.remove(countFieldInternalName);
                }
            }

            if (! countRecords.containsKey(countFieldInternalName)) {
                ObjectField countField = getCountField(countFieldInternalName);
                ObjectField eventDateField = getEventDateField(countFieldInternalName);
                CountRecord countRecord = new CountRecord(this, countField.getUniqueName(), this.getDimensions(countFieldInternalName));
                if (eventDateField != null) {
                    countRecord.setEventDatePrecision(eventDateField.as(CountableFieldData.class).getEventDatePrecision());
                } else {
                    countRecord.setEventDatePrecision(Record.CountEventDatePrecision.NONE);
                }
                if (countField.as(CountableFieldData.class).isIncludeSelfDimension()) {
                    countRecord.setIncludeSelfDimension(true);
                }
                countRecords.put(countFieldInternalName, countRecord);
            }
            return countRecords.get(countFieldInternalName);
        }

    }

}
