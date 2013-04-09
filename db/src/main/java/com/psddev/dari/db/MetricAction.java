package com.psddev.dari.db;

import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

class MetricAction extends Modification<Object> {

    //private static final Logger LOGGER = LoggerFactory.getLogger(MetricAction.class);

    private transient final Map<String, Metric> recordMetrics = new HashMap<String, Metric>();
    private transient final Map<String, ObjectField> eventDateFields = new HashMap<String, ObjectField>();
    private transient final Map<String, ObjectField> metricFields = new HashMap<String, ObjectField>();
    private transient Date oldEventDateValue;

    private ObjectField findMetricField(String internalName) {
        ObjectType recordType = ObjectType.getInstance(getOriginalObject().getClass());
        if (internalName == null) {
            for (ObjectField objectField : recordType.getFields()) {
                if (objectField.as(Metric.FieldData.class).isMetricValue()) {
                    return objectField;
                }
            }
        } else {
            ObjectField objectField = recordType.getField(internalName);
            if (objectField != null && objectField.as(Metric.FieldData.class).isMetricValue()) {
                return objectField;
            }
        }
        throw new RuntimeException("At least one numeric field must be marked as @MetricValue");
    }

    private ObjectField getMetricField(String internalName) {
        if (!metricFields.containsKey(internalName)) {
            metricFields.put(internalName, findMetricField(internalName));
        }
        return metricFields.get(internalName);
    }

    private ObjectField getEventDateField(String metricFieldInternalName) {
        if (! eventDateFields.containsKey(metricFieldInternalName)) {
            ObjectType recordType = ObjectType.getInstance(getOriginalObject().getClass());
            ObjectField metricField = getMetricField(metricFieldInternalName);
            String eventDateFieldName = metricField.as(Metric.FieldData.class).getEventDateFieldName();
            if (eventDateFieldName != null) {
                ObjectField eventDateField = recordType.getField(eventDateFieldName);
                if (eventDateField == null) {
                    throw new RuntimeException("Invalid eventDate field : " + eventDateFieldName);
                }
                if (eventDateField.as(Metric.FieldData.class).isEventDateField()) {
                    eventDateFields.put(metricFieldInternalName, eventDateField);
                } else {
                    throw new RuntimeException("The field " + eventDateFieldName + " is not annotated as @EventDate.");
                }
            } else {
                eventDateFields.put(metricFieldInternalName, null);
            }
        }
        return eventDateFields.get(metricFieldInternalName);
    }

    public void incrementMetric(String metricFieldInternalName, double c) {
        try {
            getMetricObject(metricFieldInternalName).incrementMetric(c);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricObject(metricFieldInternalName).getDatabase(), "Error in Metric.incrementMetric() : " + e.getLocalizedMessage());
        }
    }

    public void setMetric(String metricFieldInternalName, double c) {
        try {
            getMetricObject(metricFieldInternalName).setMetric(c);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricObject(metricFieldInternalName).getDatabase(), "Error in Metric.setMetric() : " + e.getLocalizedMessage());
        }
    }

    public void deleteMetrics() {
        try {
            getMetricObject(null).deleteMetrics();
        } catch (SQLException e) {
            throw new DatabaseException(getMetricObject(null).getDatabase(), "Error in Metric.deleteMetrics() : " + e.getLocalizedMessage());
        }
    }

    public double getMetric(String metricFieldInternalName) {
        try {
            Metric cr = getMetricObject(metricFieldInternalName);
            cr.setQueryDateRange(null, null);
            Double metricValue = cr.getMetric();
            if (metricValue == null) return 0.0d;
            return metricValue;
        } catch (SQLException e) {
            throw new DatabaseException(getMetricObject(metricFieldInternalName).getDatabase(), "Error in Metric.getMetric() : " + e.getLocalizedMessage());
        }
    }

    public double getMetricSinceDate(String metricFieldInternalName, Long startTimestamp) {
        return getMetricOverDateRange(metricFieldInternalName, startTimestamp, null);
    }

    public double getMetricAsOfDate(String metricFieldInternalName, Long endTimestamp) {
        return getMetricOverDateRange(metricFieldInternalName, null, endTimestamp);
    }

    public double getMetricOverDateRange(String metricFieldInternalName, Long startTimestamp, Long endTimestamp) {
        try {
            Metric cr = getMetricObject(metricFieldInternalName);
            if (cr.getEventDateProcessor().equals(MetricEventDateProcessor.None.class)) {
                throw new RuntimeException("Date range does not apply - no MetricEventDateProcessor");
            }
            cr.setQueryDateRange(startTimestamp, endTimestamp);
            return getMetricObject(metricFieldInternalName).getMetric();
        } catch (SQLException e) {
            throw new DatabaseException(getMetricObject(metricFieldInternalName).getDatabase(), "Error in Metric.getMetric() : " + e.getLocalizedMessage());
        }
    }

    private Metric getMetricObject(String metricFieldInternalName) {
        // if metricFieldInternalName is null, it will return the *first* @MetricValue in the type

        ObjectField eventDateField = getEventDateField(metricFieldInternalName);
        if (! recordMetrics.containsKey(metricFieldInternalName)) {
            ObjectField metricField = getMetricField(metricFieldInternalName);
            Metric recordMetric = new Metric(this, metricField.getUniqueName());
            if (eventDateField != null) {
                recordMetric.setEventDateProcessor(eventDateField.as(Metric.FieldData.class).getEventDateProcessor());
            } else {
                recordMetric.setEventDateProcessor(null);
            }
            recordMetrics.put(metricFieldInternalName, recordMetric);
        }

        if (eventDateField != null) {
            Object eventDateValue = getState().getByPath(eventDateField.getInternalName());
            if (eventDateValue != null && eventDateValue instanceof Date) {
                if (! ((Date) eventDateValue).equals(oldEventDateValue)) {
                    recordMetrics.get(metricFieldInternalName).setEventDate(((Date) eventDateValue).getTime());
                    oldEventDateValue = (Date)((Date) eventDateValue).clone();
                }
            }
        }

        return recordMetrics.get(metricFieldInternalName);
    }
}
