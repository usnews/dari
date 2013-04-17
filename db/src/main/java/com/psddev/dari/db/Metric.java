package com.psddev.dari.db;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class Metric implements Recordable {

    private static final int CACHE_SIZE = 50; // This is per State, it doesn't need to be big.

    private transient final String metricFieldInternalName;
    private transient State state;
    private transient final ObjectField metricField;
    private transient final ObjectType recordType;
    private transient final MetricDatabase metricDatabase;

    //private static final Logger LOGGER = LoggerFactory.getLogger(Metric.class);

    private transient final Cache<String, Double> metricCache = CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).build();
    private transient final Cache<String, Map<String, Double>> metricValuesCache = CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).build();
    private transient final Cache<String, Map<DateTime, Double>> metricTimelineCache = CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).build();

    public Metric(State state, String metricFieldInternalName) {
        this.state = state;
        this.metricFieldInternalName = metricFieldInternalName;
        this.recordType = ObjectType.getInstance(state.getOriginalObject().getClass());
        this.metricField = recordType.getField(metricFieldInternalName);
        this.metricDatabase = new MetricDatabase(state, metricField.getUniqueName());
        this.metricDatabase.setEventDateProcessor(metricField.as(MetricDatabase.FieldData.class).getEventDateProcessor());
    }

    @Override
    public void setState(State state) {
        this.state = state;
    }

    @Override
    public State getState() {
        return this.state;
    }

    @Override
    public <T> T as(Class<T> modificationClass) {
        return getState().as(modificationClass);
    }

    private void clearCaches() {
        metricCache.invalidateAll();
        metricTimelineCache.invalidateAll();
        metricValuesCache.invalidateAll();
    }

    // Implicit null dimension
    public void increment(double amount) {
        incrementDimensionOnEventDate(null, amount, null);
    }

    // Implicit null dimension
    public void incrementOnEventDate(double amount, DateTime eventDate) {
        incrementDimensionOnEventDate(null, amount, eventDate);
    }

    // Explicit dimension
    public void incrementDimension(String dimensionValue, double amount) {
        incrementDimensionOnEventDate(dimensionValue, amount, null);
    }

    // Explicit dimension
    public void incrementDimensionOnEventDate(String dimensionValue, double amount, DateTime eventDate) {
        try {
            clearCaches();
            metricDatabase.setEventDate(eventDate);
            metricDatabase.incrementMetric(dimensionValue, amount);
        } catch (SQLException e) {
            throw new DatabaseException(metricDatabase.getDatabase(), "Error in MetricDatabase.incrementMetric() : " + e.getLocalizedMessage());
        }
    }

    // All dimensions
    public void delete() {
        try {
            metricDatabase.deleteMetric();
        } catch (SQLException e) {
            throw new DatabaseException(metricDatabase.getDatabase(), "Error in MetricDatabase.deleteMetric() : " + e.getLocalizedMessage());
        }
    }

    //////////////////////////////////////////////////

    // Implicit null dimension
    public double getValue() {
        return getDimensionValueBetween(null, null, null);
    }

    // Implicit null dimension
    public double getValueBetween(DateTime startTimestamp, DateTime endTimestamp) {
        return getDimensionValueBetween(null, startTimestamp, endTimestamp);
    }

    // Explicit dimension
    public double getDimensionValue(String dimensionValue) {
        return getDimensionValueBetween(dimensionValue, null, null);
    }

    // Explicit dimension
    public double getDimensionValueBetween(String dimensionValue, DateTime startTimestamp, DateTime endTimestamp) {
        final String cacheKey = "m:" + String.valueOf(dimensionValue) + ":" + String.valueOf(startTimestamp) + ":" + String.valueOf(endTimestamp);
        Double metricValue = metricCache.getIfPresent(cacheKey);
        if (metricValue == null) {
            try {
                metricDatabase.setQueryDateRange(startTimestamp, endTimestamp);
                metricValue = metricDatabase.getMetric(dimensionValue);
                if (metricValue == null) {
                    metricValue = 0.0d;
                }
                metricCache.put(cacheKey, metricValue);
            } catch (SQLException e) {
                throw new DatabaseException(metricDatabase.getDatabase(), "Error in MetricDatabase.getMetric() : " + e.getLocalizedMessage());
            }
        }
        return metricValue;
    }

    // Implicit null dimension
    public Map<DateTime, Double> getTimeline() {
        return getDimensionTimelineBetween(null, null, null, null);
    }

    // Implicit null dimension
    public Map<DateTime, Double> getTimeline(MetricInterval metricInterval) {
        return getDimensionTimelineBetween(null, metricInterval, null, null);
    }

    // Implicit null dimension
    public Map<DateTime, Double> getTimelineBetween(MetricInterval metricInterval, DateTime startTimestamp, DateTime endTimestamp) {
        return getDimensionTimelineBetween(null, metricInterval, startTimestamp, startTimestamp);
    }

    // Explicit dimension
    public Map<DateTime, Double> getDimensionTimeline(String dimensionValue) {
        return getDimensionTimelineBetween(dimensionValue, null, null, null);
    }

    // Explicit dimension
    public Map<DateTime, Double> getDimensionTimeline(String dimensionValue, MetricInterval metricInterval) {
        return getDimensionTimelineBetween(dimensionValue, metricInterval, null, null);
    }

    // Explicit dimension
    public Map<DateTime, Double> getDimensionTimelineBetween(String dimensionValue, MetricInterval metricInterval, DateTime startTimestamp, DateTime endTimestamp) {
        final String cacheKey = "m:"+String.valueOf(dimensionValue)+":"+String.valueOf(startTimestamp) + ":" + String.valueOf(endTimestamp) + ":" + ( metricInterval == null ? "default" : metricInterval.getClass().getName());
        Map<DateTime, Double> metricTimeline = metricTimelineCache.getIfPresent(cacheKey);
        if (metricTimeline == null) {
            try {
                metricDatabase.setQueryDateRange(startTimestamp, endTimestamp);
                metricTimeline = metricDatabase.getMetricTimeline(dimensionValue, metricInterval);
                if (metricTimeline == null) {
                    metricTimeline = new HashMap<DateTime, Double>();
                }
                metricTimelineCache.put(cacheKey, metricTimeline);
            } catch (SQLException e) {
                throw new DatabaseException(metricDatabase.getDatabase(), "Error in MetricDatabase.getMetricTimeline() : " + e.getLocalizedMessage());
            }
        }
        return metricTimeline;
    }

    // Sum of all dimensions
    public Map<DateTime, Double> getSumTimeline() {
        return getSumTimelineBetween(null, null, null);
    }

    // Sum of all dimensions
    public Map<DateTime, Double> getSumTimeline(MetricInterval metricInterval) {
        return getSumTimelineBetween(metricInterval, null, null);
    }

    // Sum of all dimensions
    public Map<DateTime, Double> getSumTimelineBetween(MetricInterval metricInterval, DateTime startTimestamp, DateTime endTimestamp) {
        final String cacheKey = "s:"+String.valueOf(startTimestamp) + ":" + String.valueOf(endTimestamp) + ":" + ( metricInterval == null ? "default" : metricInterval.getClass().getName());
        Map<DateTime, Double> metricTimeline = metricTimelineCache.getIfPresent(cacheKey);
        if (metricTimeline == null) {
            try {
                metricDatabase.setQueryDateRange(startTimestamp, endTimestamp);
                metricTimeline = metricDatabase.getMetricSumTimeline(metricInterval);
                if (metricTimeline == null) {
                    metricTimeline = new HashMap<DateTime, Double>();
                }
                metricTimelineCache.put(cacheKey, metricTimeline);
            } catch (SQLException e) {
                throw new DatabaseException(metricDatabase.getDatabase(), "Error in MetricDatabase.getSumTimeline() : " + e.getLocalizedMessage());
            }
        }
        return metricTimeline;
    }

    // Sum of all dimensions
    public double getSum() {
        return getSumBetween(null, null);
    }

    // Sum of all dimensions
    public double getSumBetween(DateTime startTimestamp, DateTime endTimestamp) {
        final String cacheKey = "sum:"+String.valueOf(startTimestamp) + ":" + String.valueOf(endTimestamp);
        Double metricValue = metricCache.getIfPresent(cacheKey);
        if (metricValue == null) {
            try {
                metricDatabase.setQueryDateRange(startTimestamp, endTimestamp);
                metricValue = metricDatabase.getMetricSum();
                if (metricValue == null) {
                    metricValue = 0.0d;
                }
                metricCache.put(cacheKey, metricValue);
            } catch (SQLException e) {
                throw new DatabaseException(metricDatabase.getDatabase(), "Error in MetricDatabase.getMetric() : " + e.getLocalizedMessage());
            }
        }
        return metricValue;
    }

    // All dimensions
    public Map<String, Double> getValues() {
        return getValuesBetween(null, null);
    }

    // All dimensions
    public Map<String, Double> getValuesBetween(DateTime startTimestamp, DateTime endTimestamp) {
        final String cacheKey = String.valueOf(startTimestamp) + ":" + String.valueOf(endTimestamp);
        Map<String, Double> metricValues = metricValuesCache.getIfPresent(cacheKey);
        if (metricValues == null) {
            try {
                metricDatabase.setQueryDateRange(startTimestamp, endTimestamp);
                metricValues = metricDatabase.getMetricValues();
                if (metricValues == null) {
                    metricValues = new HashMap<String, Double>();
                }
                metricValuesCache.put(cacheKey, metricValues);
            } catch (SQLException e) {
                throw new DatabaseException(metricDatabase.getDatabase(), "Error in MetricDatabase.getMetric() : " + e.getLocalizedMessage());
            }
        }
        return metricValues;
    }

    //////////////////////////////////////////////////

}
