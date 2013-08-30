package com.psddev.dari.db;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import org.joda.time.DateTime;

@Metric.Embedded
public class Metric extends Record {

    private final transient State owner;
    private final transient ObjectField field;
    private final transient MetricDatabase metricDatabase;

    /**
     * @param owner Can't be {@code null}.
     * @param field Can't be {@code null}.
     */
    public Metric(State owner, ObjectField field) {
        this.owner = owner;
        this.field = field;
        SqlDatabase sqlDatabase = null;
        Database db = owner.getDatabase();
        if (db != null) {
            // Drill down until we find the SqlDatabase
            while (db instanceof ForwardingDatabase) {
                db = ((ForwardingDatabase) db).getDelegate();
            }
            if (db instanceof SqlDatabase) {
                sqlDatabase = (SqlDatabase) db;
            } else if (db instanceof AggregateDatabase) {
                if (((AggregateDatabase) db).getDefaultDelegate() instanceof SqlDatabase) {
                    sqlDatabase = (SqlDatabase) ((AggregateDatabase) db).getDefaultDelegate();
                } else {
                    sqlDatabase = (SqlDatabase) ((AggregateDatabase) db).getFirstDelegateByClass(SqlDatabase.class);
                }
            }
        }

        if (sqlDatabase == null) {
            this.metricDatabase = null;
        } else {
            this.metricDatabase = new MetricDatabase(sqlDatabase, owner, field.getUniqueName());
            this.metricDatabase.setEventDateProcessor(field.as(MetricDatabase.FieldData.class).getEventDateProcessor());
        }
    }

    /**
     * Returns the state that owns this metric instance.
     *
     * @return Never {@code null}.
     */
    public State getOwner() {
        return owner;
    }

    /**
     * Returns the field that this metric instance updates.
     *
     * @return Never {@code null}.
     */
    public ObjectField getField() {
        return field;
    }

    /**
     * Returns the MetricDatabase or throw an exception if it could not find the SQL database. 
     * 
     */
    private MetricDatabase getMetricDatabase() {
        if (metricDatabase == null) {
            throw new RuntimeException ("Metric field " +field.getUniqueName()+" cannot determine SQL database for database " + owner.getDatabase().getName() + " (" + owner.getDatabase().getClass().getName() + "), this Metric object is unusable!");
        }
        return metricDatabase;
    }

    /**
     * Increases the metric value by the given {@code amount}.
     */
    public void increment(double amount) {
        incrementDimensionAt(amount, null, null);
    }

    /**
     * Increases the metric value by the given {@code amount} and associate it
     * with the given {@code dimension} and {@code time}.
     *
     * @param dimension May be {@code null}.
     * @param time If {@code null}, right now.
     */
    public void incrementDimensionAt(double amount, String dimension, DateTime time) {
        try {
            getMetricDatabase().setEventDate(time);
            getMetricDatabase().incrementMetric(dimension, amount);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricDatabase().getDatabase(), "Error in MetricDatabase.incrementMetric() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Asynchronously (within the given {@code within} seconds) increases the
     * metric value by the given {@code amount} and associate it with the given
     * {@code dimension} and {@code time}.
     *
     * @param dimension May be {@code null}.
     * @param time May be {@code null}.
     * @param within In seconds. If less than or equal to {@code 0}, uses
     * {@code 1.0} instead.
     */
    public void incrementEventually(double amount, String dimension, DateTime time, double within) {
        if (within <= 0) {
            within = 1.0;
        }

        getMetricDatabase().setEventDate(time);
        UUID dimensionId;
        try {
            dimensionId = getMetricDatabase().getDimensionId(dimension);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricDatabase().getDatabase(), "Error in MetricDatabase.getDimensionId() : " + e.getLocalizedMessage());
        }
        MetricIncrementQueue.queueIncrement(getMetricDatabase(), dimensionId, amount, within);
    }

    /**
     * Returns when the metric value associated with the given
     * {@code dimension} was last updated.
     *
     * @param dimension May be {@code null}.
     * @return May be {@code null}.
     */
    public DateTime getLastDimensionUpdate(String dimension) {
        try {
            return getMetricDatabase().getLastUpdate(dimension);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricDatabase().getDatabase(), "Error in MetricDatabase.getLastUpdate() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Returns when the metric value was last updated.
     *
     * @return May be {@code null}.
     */
    public DateTime getLastUpdate() {
        return getLastDimensionUpdate(null);
    }

    /**
     * Sets the metric value to the given {@code amount} and associates it with
     * the given {@code dimension} and {@code time}.
     *
     * @param dimension May be {@code null}.
     * @param time If {@code null}, right now.
     */
    public void setDimensionAt(double amount, String dimension, DateTime time) {
        try {
            getMetricDatabase().setEventDate(time);
            getMetricDatabase().setMetric(dimension, amount);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricDatabase().getDatabase(), "Error in MetricDatabase.setMetric() : " + e.getLocalizedMessage());
        }
    }

    /** Deletes all metric values. */
    public void deleteAll() {
        try {
            getMetricDatabase().deleteMetric();
        } catch (SQLException e) {
            throw new DatabaseException(getMetricDatabase().getDatabase(), "Error in MetricDatabase.deleteMetric() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Returns the metric value associated with the given {@code dimension}
     * between the given {@code start} and {@code end}.
     *
     * @param dimension May be {@code null}.
     * @param start If {@code null}, beginning of time.
     * @param end If {@code null}, end of time.
     */
    public double getByDimensionBetween(String dimension, DateTime start, DateTime end) {
        try {
            getMetricDatabase().setQueryDateRange(start, end);
            Double metricValue = getMetricDatabase().getMetric(dimension);
            return metricValue == null ? 0.0 : metricValue;
        } catch (SQLException e) {
            throw new DatabaseException(getMetricDatabase().getDatabase(), "Error in MetricDatabase.getMetric() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Returns the metric value associated with the given {@code dimension}.
     *
     * @param dimension May be {@code null}.
     */
    public double getByDimension(String dimension) {
        return getByDimensionBetween(dimension, null, null);
    }

    /**
     * Returns the sum of all metric values in each dimension between the given
     * {@code start} and {@code end}.
     *
     * @param start If {@code null}, beginning of time.
     * @param end If {@code null}, end of time.
     */
    public double getSumBetween(DateTime start, DateTime end) {
        return getByDimensionBetween(null, start, end);
    }

    /**
     * Returns the sum of all metric values in each dimension.
     */
    public double getSum() {
        return getSumBetween(null, null);
    }

    /**
     * Groups the metric values between the given {@code start} and {@code end}
     * by each dimension.
     *
     * @param start If {@code null}, beginning of time.
     * @param end If {@code null}, end of time.
     * @return Never {@code null}.
     */
    public Map<String, Double> groupByDimensionBetween(DateTime start, DateTime end) {
        try {
            getMetricDatabase().setQueryDateRange(start, end);
            Map<String, Double> metricValues = getMetricDatabase().getMetricValues();
            return metricValues;
        } catch (SQLException e) {
            throw new DatabaseException(getMetricDatabase().getDatabase(), "Error in MetricDatabase.getMetricValues() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Groups the metric values by each dimension.
     *
     * @return Never {@code null}.
     */
    public Map<String, Double> groupByDimension() {
        return groupByDimensionBetween(null, null);
    }

    /**
     * Groups the metric values associated with the given {@code dimension}
     * between the given {@code start} and {@code end} by the given
     * {@code interval}.
     *
     * @param dimension May be {@code null}.
     * @param start If {@code null}, beginning of time.
     * @param end If {@code null}, end of time.
     * @return Never {@code null}.
     */
    public Map<DateTime, Double> groupByDate(String dimension, MetricInterval interval, DateTime start, DateTime end) {
        try {
            getMetricDatabase().setQueryDateRange(start, end);
            Map<DateTime, Double> metricTimeline = getMetricDatabase().getMetricTimeline(dimension, interval);
            return metricTimeline;
        } catch (SQLException e) {
            throw new DatabaseException(getMetricDatabase().getDatabase(), "Error in MetricDatabase.getMetricTimeline() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Groups the sum of all metric values between the given {@code start} and
     * {@code end} by the given {@code interval}.
     *
     * @param start If {@code null}, beginning of time.
     * @param end If {@code null}, end of time.
     * @return Never {@code null}.
     */
    public Map<DateTime, Double> groupSumByDate(MetricInterval interval, DateTime start, DateTime end) {
        return groupByDate(null, interval, start, end);
    }

    /**
     * Repairs the cumulativeAmount value for all rows since the beginning of
     * time. This method takes a long time to complete and should only be used
     * if something has gone wrong.
     *
     */
    public void repair() {
        try {
            getMetricDatabase().reconstructCumulativeAmounts();
        } catch (SQLException e) {
            throw new DatabaseException(getMetricDatabase().getDatabase(), "Error in MetricDatabase.reconstructCumulativeAmounts() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Returns the metric value associated with the main ({@code null})
     * dimension between the given {@code start} and {@code end}.
     *
     * @deprecated Use {@link #getSumBetween} instead.
     */
    @Deprecated
    public double getValue(DateTime start, DateTime end) {
        return getByDimensionBetween(null, start, end);
    }

    /**
     * Returns the metric value associated with the main ({@code null})
     * dimension between the given {@code start} and {@code end}.
     * @deprecated Use {@link #getSumBetween} instead
     */
    @Deprecated
    public double getValueBetween(DateTime start, DateTime end) {
        return getByDimensionBetween(null, start, end);
    }

    /**
     * Returns the metric value associated with the main ({@code null})
     * dimension.
     * @deprecated Use {@link #getSum} instead.
     */
    @Deprecated
    public double getValue() {
        return getByDimensionBetween(null, null, null);
    }


}
