package com.psddev.dari.db;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

@Metric.Embedded
public class Metric extends Record {

    private transient final ObjectField metricField;
    private transient final MetricDatabase metricDatabase;

    //private static final Logger LOGGER = LoggerFactory.getLogger(Metric.class);

    public Metric(State state, String metricFieldInternalName) {
        this.metricField = state.getField(metricFieldInternalName);
        this.metricDatabase = new MetricDatabase(state, metricField.getUniqueName());
        this.metricDatabase.setEventDateProcessor(metricField.as(MetricDatabase.FieldData.class).getEventDateProcessor());
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
        try {
            metricDatabase.setQueryDateRange(startTimestamp, endTimestamp);
            Double metricValue = metricDatabase.getMetric(dimensionValue);
            return metricValue == null ? 0.0 : metricValue;
        } catch (SQLException e) {
            throw new DatabaseException(metricDatabase.getDatabase(), "Error in MetricDatabase.getMetric() : " + e.getLocalizedMessage());
        }
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
        try {
            metricDatabase.setQueryDateRange(startTimestamp, endTimestamp);
            Map<DateTime, Double> metricTimeline = metricDatabase.getMetricTimeline(dimensionValue, metricInterval);
            return metricTimeline == null ? new HashMap<DateTime, Double>() : metricTimeline;
        } catch (SQLException e) {
            throw new DatabaseException(metricDatabase.getDatabase(), "Error in MetricDatabase.getMetricTimeline() : " + e.getLocalizedMessage());
        }
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
        try {
            metricDatabase.setQueryDateRange(startTimestamp, endTimestamp);
            Map<DateTime, Double> metricTimeline = metricDatabase.getMetricSumTimeline(metricInterval);
            return metricTimeline == null ? new HashMap<DateTime, Double>() : metricTimeline;
        } catch (SQLException e) {
            throw new DatabaseException(metricDatabase.getDatabase(), "Error in MetricDatabase.getSumTimeline() : " + e.getLocalizedMessage());
        }
    }

    // Sum of all dimensions
    public double getSum() {
        return getSumBetween(null, null);
    }

    // Sum of all dimensions
    public double getSumBetween(DateTime startTimestamp, DateTime endTimestamp) {
        try {
            metricDatabase.setQueryDateRange(startTimestamp, endTimestamp);
            Double metricValue = metricDatabase.getMetricSum();
            return metricValue == null ? 0.0 : metricValue;
        } catch (SQLException e) {
            throw new DatabaseException(metricDatabase.getDatabase(), "Error in MetricDatabase.getMetric() : " + e.getLocalizedMessage());
        }
    }

    // All dimensions
    public Map<String, Double> getValues() {
        return getValuesBetween(null, null);
    }

    // All dimensions
    public Map<String, Double> getValuesBetween(DateTime startTimestamp, DateTime endTimestamp) {
        try {
            metricDatabase.setQueryDateRange(startTimestamp, endTimestamp);
            Map<String, Double> metricValues = metricDatabase.getMetricValues();
            return metricValues == null ? new HashMap<String, Double>() : metricValues;
        } catch (SQLException e) {
            throw new DatabaseException(metricDatabase.getDatabase(), "Error in MetricDatabase.getMetric() : " + e.getLocalizedMessage());
        }
    }

    //////////////////////////////////////////////////

}
