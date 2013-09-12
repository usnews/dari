package com.psddev.dari.db;

import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.Task;

final class MetricIncrementQueue {

    // private static final Logger LOGGER = LoggerFactory.getLogger(MetricIncrementQueue.class);

    private static final ConcurrentHashMap<String, QueuedMetricIncrement> queuedIncrements = new ConcurrentHashMap<String, QueuedMetricIncrement>();;

    public static void queueIncrement(UUID id, UUID dimensionId, DateTime eventDate, MetricAccess metricAccess, double amount, double withinSeconds) {

        putInMap(id, dimensionId, eventDate, metricAccess, amount);

        // If the task is already running or has been scheduled, this won't do anything.
        MetricIncrementQueueTask.getInstance(queuedIncrements).schedule(withinSeconds);

    }

    private static void putInMap(UUID id, UUID dimensionId, DateTime eventDate, MetricAccess metricAccess, double amount) {

        String key = getKey(id, dimensionId, eventDate, metricAccess);
        QueuedMetricIncrement placeholder = new QueuedMetricIncrement(id, dimensionId, eventDate, metricAccess, 0d);
        while (true) {
            QueuedMetricIncrement current = queuedIncrements.putIfAbsent(key, placeholder);
            if (current == null) {
                current = placeholder;
            }
            QueuedMetricIncrement next = new QueuedMetricIncrement(id, dimensionId, eventDate, metricAccess, current.amount + amount);
            if (queuedIncrements.replace(key, current, next)) {
                return;
            } else {
                continue;
            }
        }

    }

    private static String getKey(UUID id, UUID dimensionId, DateTime eventDate, MetricAccess metricAccess) {
        StringBuilder str = new StringBuilder();
        str.append(id);
        str.append(':');
        str.append(metricAccess.getTypeId());
        str.append(':');
        str.append(metricAccess.getSymbolId());
        str.append(':');
        if (eventDate != null) {
            str.append(eventDate.getMillis());
        }
        str.append(':');
        str.append(dimensionId);
        return str.toString();
    }

}

class QueuedMetricIncrement {
    public final UUID id;
    public final UUID dimensionId;
    public final DateTime eventDate;
    public final MetricAccess metricAccess;
    public final double amount;

    public QueuedMetricIncrement(UUID id, UUID dimensionId, DateTime eventDate, MetricAccess metricAccess, double amount) {
        this.id = id;
        this.dimensionId = dimensionId;
        this.eventDate = eventDate;
        this.metricAccess = metricAccess;
        this.amount = amount;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof QueuedMetricIncrement)) {
            return false;
        } else {
            QueuedMetricIncrement otherIncr = (QueuedMetricIncrement) other;
            if (this.amount == otherIncr.amount &&
                this.metricAccess.equals(otherIncr.metricAccess) &&
                this.dimensionId.equals(otherIncr.dimensionId) &&
                ((this.eventDate == null && otherIncr.eventDate == null) || (this.eventDate != null && this.eventDate.equals(otherIncr.eventDate))) &&
                this.id.equals(otherIncr.id)) {
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

        while (true) {
            if (queuedIncrements.isEmpty()) {
                break;
            }
            String key = queuedIncrements.keySet().iterator().next();
            QueuedMetricIncrement queuedIncrement = queuedIncrements.remove(key);
            try {
                queuedIncrement.metricAccess.incrementMetricByDimensionId(queuedIncrement.id, queuedIncrement.eventDate, queuedIncrement.dimensionId, queuedIncrement.amount);
            } catch (SQLException ex) {
                LOGGER.error("SQLException during incrementMetricByDimensionId: " + ex.getLocalizedMessage());
                throw new DatabaseException(queuedIncrement.metricAccess.getDatabase(), "SQLException during MetricAccess.incrementMetricByDimensionId", ex);
            }
        }

    }

}
