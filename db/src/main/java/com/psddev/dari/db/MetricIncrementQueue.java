package com.psddev.dari.db;

import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.Task;

final class MetricIncrementQueue {

    private MetricIncrementQueue() {
    }

    // private static final Logger LOGGER = LoggerFactory.getLogger(MetricIncrementQueue.class);

    private static final ConcurrentHashMap<String, QueuedMetricIncrement> queuedIncrements = new ConcurrentHashMap<String, QueuedMetricIncrement>();;

    public static void queueIncrement(MetricDatabase metricDatabase, UUID dimensionId, double amount, double withinSeconds) {

        putInMap(metricDatabase, dimensionId, amount);

        // If the task is already running or has been scheduled, this won't do anything.
        MetricIncrementQueueTask.getInstance(queuedIncrements).schedule(withinSeconds);

    }

    private static void putInMap(MetricDatabase metricDatabase, UUID dimensionId, double amount) {

        String key = getKey(metricDatabase, dimensionId);
        QueuedMetricIncrement placeholder = new QueuedMetricIncrement(metricDatabase, dimensionId, 0d);
        while (true) {
            QueuedMetricIncrement current = queuedIncrements.putIfAbsent(key, placeholder);
            if (current == null) current = placeholder;
            QueuedMetricIncrement next = new QueuedMetricIncrement(metricDatabase, dimensionId, current.amount + amount);
            if (queuedIncrements.replace(key, current, next)) {
                return;
            } else {
                continue;
            }
        }

    }

    private static String getKey(MetricDatabase metricDatabase, UUID dimensionId) {
        return metricDatabase.toKeyString() + ":" + dimensionId.toString();
    }

}

class QueuedMetricIncrement {
    public final MetricDatabase metricDatabase;
    public final UUID dimensionId;
    public final double amount;

    public QueuedMetricIncrement(MetricDatabase metricDatabase, UUID dimensionId, double amount) {
        this.metricDatabase = metricDatabase;
        this.dimensionId = dimensionId;
        this.amount = amount;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof QueuedMetricIncrement)) {
            return false;
        } else {
            if (this.amount == ((QueuedMetricIncrement) other).amount &&
                this.metricDatabase.equals(((QueuedMetricIncrement) other).metricDatabase) &&
                this.dimensionId.equals(((QueuedMetricIncrement) other).dimensionId)) {
                return true;
            } else {
                return false;
            }
        }
    }

}

class MetricIncrementQueueTask extends Task {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricIncrementQueue.class);
    private static MetricIncrementQueueTask instance;

    private final transient ConcurrentHashMap<String, QueuedMetricIncrement> queuedIncrements;

    private MetricIncrementQueueTask(ConcurrentHashMap<String, QueuedMetricIncrement> queuedIncrements) {
        this.queuedIncrements = queuedIncrements;
    }

    public static MetricIncrementQueueTask getInstance(ConcurrentHashMap<String, QueuedMetricIncrement> queuedIncrements) {
        if (instance == null) {
            instance = new MetricIncrementQueueTask(queuedIncrements);
        }
        return instance;
    }

    public void doTask() {

        // LOGGER.info("EXECUTING MetricIncrementQueueTask");
        while (true) {
            if (queuedIncrements.isEmpty()) break;
            String key = queuedIncrements.keySet().iterator().next();
            QueuedMetricIncrement queuedIncrement = queuedIncrements.remove(key);
            // LOGGER.info("Incrementing : " + queuedIncrement.metricDatabase.toKeyString() + " : " + queuedIncrement.dimensionId.toKeyString() + " += " + queuedIncrement.amount );
            try {
                queuedIncrement.metricDatabase.incrementMetricByDimensionId(queuedIncrement.dimensionId, queuedIncrement.amount);
            } catch (SQLException ex) {
                LOGGER.error("SQLException during incrementMetricByDimensionId: " + ex.getLocalizedMessage());
                throw new DatabaseException(queuedIncrement.metricDatabase.getDatabase(), "SQLException during MetricDatabase.incrementMetricByDimensionId", ex);
            }
        }

    }

}
