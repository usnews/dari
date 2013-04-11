package com.psddev.dari.db;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

class MetricAction extends Modification<Object> {

    //private static final Logger LOGGER = LoggerFactory.getLogger(MetricAction.class);

    private transient final Map<String, Metric> recordMetrics = new HashMap<String, Metric>();
    private transient final Map<String, ObjectField> metricFields = new HashMap<String, ObjectField>();
    private transient final Map<String, Map<String, Double>> metricValueCache = new HashMap<String, Map<String, Double>>();

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

    private Map<String, Double> getMetricValueCache(String internalName) {
        if (! metricValueCache.containsKey(internalName)) {
            metricValueCache.put(internalName, new HashMap<String, Double>());
        }
        return metricValueCache.get(internalName);
    }
    private ObjectField getMetricField(String internalName) {
        if (!metricFields.containsKey(internalName)) {
            metricFields.put(internalName, findMetricField(internalName));
        }
        return metricFields.get(internalName);
    }

    public void incrementMetric(String metricFieldInternalName, double c) {
        metricValueCache.remove(metricFieldInternalName);
        incrementMetric(metricFieldInternalName, c, System.currentTimeMillis());
    }

    public void incrementMetric(String metricFieldInternalName, double c, long eventDateMillis) {
        try {
            metricValueCache.remove(metricFieldInternalName);
            getMetricObject(metricFieldInternalName).setEventDate(eventDateMillis);
            getMetricObject(metricFieldInternalName).incrementMetric(c);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricObject(metricFieldInternalName).getDatabase(), "Error in Metric.incrementMetric() : " + e.getLocalizedMessage());
        }
    }

    public void setMetric(String metricFieldInternalName, double c) {
        try {
            metricValueCache.remove(metricFieldInternalName);
            getMetricObject(metricFieldInternalName).setMetric(c);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricObject(metricFieldInternalName).getDatabase(), "Error in Metric.setMetric() : " + e.getLocalizedMessage());
        }
    }

    public void deleteMetric(String metricFieldInternalName) {
        try {
            getMetricObject(metricFieldInternalName).deleteMetric();
        } catch (SQLException e) {
            throw new DatabaseException(getMetricObject(null).getDatabase(), "Error in Metric.deleteMetric() : " + e.getLocalizedMessage());
        }
    }

    public double getMetric(String metricFieldInternalName) {
        final String cacheKey = null;
        if (! getMetricValueCache(metricFieldInternalName).containsKey(cacheKey)) {
            try {
                Metric cr = getMetricObject(metricFieldInternalName);
                cr.setQueryDateRange(null, null);
                Double metricValue = cr.getMetric();
                if (metricValue == null) {
                    getMetricValueCache(metricFieldInternalName).put(cacheKey, 0.0d);
                } else {
                    getMetricValueCache(metricFieldInternalName).put(cacheKey, metricValue);
                }
            } catch (SQLException e) {
                throw new DatabaseException(getMetricObject(metricFieldInternalName).getDatabase(), "Error in Metric.getMetric() : " + e.getLocalizedMessage());
            }
        }
        return getMetricValueCache(metricFieldInternalName).get(cacheKey);
    }

    public double getMetricSinceDate(String metricFieldInternalName, Long startTimestamp) {
        return getMetricOverDateRange(metricFieldInternalName, startTimestamp, null);
    }

    public double getMetricAsOfDate(String metricFieldInternalName, Long endTimestamp) {
        return getMetricOverDateRange(metricFieldInternalName, null, endTimestamp);
    }

    public double getMetricOverDateRange(String metricFieldInternalName, Long startTimestamp, Long endTimestamp) {
        final String cacheKey = String.valueOf(startTimestamp) + ":" + String.valueOf(endTimestamp);
        if (! getMetricValueCache(metricFieldInternalName).containsKey(cacheKey)) {
            try {
                Metric cr = getMetricObject(metricFieldInternalName);
                if (cr.getEventDateProcessor().equals(MetricInterval.None.class)) {
                    throw new RuntimeException("Date range does not apply - no MetricInterval");
                }
                cr.setQueryDateRange(startTimestamp, endTimestamp);
                Double metricValue = cr.getMetric();
                if (metricValue == null) {
                    getMetricValueCache(metricFieldInternalName).put(cacheKey, 0.0d);
                } else {
                    getMetricValueCache(metricFieldInternalName).put(cacheKey, metricValue);
                }
            } catch (SQLException e) {
                throw new DatabaseException(getMetricObject(metricFieldInternalName).getDatabase(), "Error in Metric.getMetric() : " + e.getLocalizedMessage());
            }
        }
        return getMetricValueCache(metricFieldInternalName).get(cacheKey);
    }

    private Metric getMetricObject(String metricFieldInternalName) {
        // if metricFieldInternalName is null, it will return the *first* @MetricValue in the type

        if (! recordMetrics.containsKey(metricFieldInternalName)) {
            ObjectField metricField = getMetricField(metricFieldInternalName);
            Metric recordMetric = new Metric(this, metricField.getUniqueName());
            recordMetric.setEventDateProcessor(metricField.as(Metric.FieldData.class).getEventDateProcessor());
            recordMetrics.put(metricFieldInternalName, recordMetric);
        }

        return recordMetrics.get(metricFieldInternalName);
    }
}
