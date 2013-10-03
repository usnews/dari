package com.psddev.dari.db;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.Task;

class MetricIncrementQueue {

    //private static final Logger LOGGER = LoggerFactory.getLogger(MetricIncrementQueue.class);

    private static final ConcurrentHashMap<Double, ConcurrentHashMap<String, QueuedMetricIncrement>> queuedIncrements = new ConcurrentHashMap<Double,ConcurrentHashMap<String, QueuedMetricIncrement>>();

    public static void queueIncrement(MetricDatabase metricDatabase, UUID dimensionId, double amount, double withinSeconds) {

        double waitSeconds = new BigDecimal(withinSeconds * .75d).setScale(2).doubleValue();
        double executeSeconds = new BigDecimal(withinSeconds * .25d).setScale(2).doubleValue();

        putInMap(metricDatabase, dimensionId, amount, waitSeconds);

        // If the task is already running or has been scheduled, this won't do anything.
        MetricIncrementQueueTask task = MetricIncrementQueueTask.getInstance(executeSeconds, waitSeconds, queuedIncrements.get(waitSeconds));
        task.schedule(waitSeconds);

    }

    private static void putInMap(MetricDatabase metricDatabase, UUID dimensionId, double amount, double waitSeconds) {

        ConcurrentHashMap<String,QueuedMetricIncrement> queue = queuedIncrements.get(waitSeconds);
        if (queue == null) {
            queuedIncrements.putIfAbsent(waitSeconds, new ConcurrentHashMap<String, QueuedMetricIncrement>());
            queue = queuedIncrements.get(waitSeconds);
        }

        String key = getKey(metricDatabase, dimensionId);
        QueuedMetricIncrement placeholder = new QueuedMetricIncrement(metricDatabase, dimensionId, 0d);
        while (true) {
            QueuedMetricIncrement current = queue.putIfAbsent(key, placeholder);
            if (current == null) current = placeholder;
            QueuedMetricIncrement next = new QueuedMetricIncrement(metricDatabase, dimensionId, current.amount + amount);
            if (queue.replace(key, current, next)) {
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
    //private static MetricIncrementQueueTask instance;
    private static final transient ConcurrentHashMap<Double, MetricIncrementQueueTask> instances = new ConcurrentHashMap<Double, MetricIncrementQueueTask>();

    private transient final ConcurrentHashMap<String, QueuedMetricIncrement> queuedIncrements;

    private transient final double executeSeconds;
    private transient final double waitSeconds;

    private MetricIncrementQueueTask(double executeSeconds, double waitSeconds, ConcurrentHashMap<String, QueuedMetricIncrement> queuedIncrements) {
        this.queuedIncrements = queuedIncrements;
        this.executeSeconds = executeSeconds;
        this.waitSeconds = waitSeconds;
    }

    public static MetricIncrementQueueTask getInstance(double executeSeconds, double waitSeconds, ConcurrentHashMap<String, QueuedMetricIncrement> queuedIncrements) {

        MetricIncrementQueueTask instance = instances.get(executeSeconds);
        if (instance == null) {
            instances.putIfAbsent(executeSeconds, new MetricIncrementQueueTask(executeSeconds, waitSeconds, queuedIncrements));
            instance = instances.get(executeSeconds);
        }

        return instance;
    }

    public void doTask() {

        long sleepMilliseconds = 0L;
        Iterator<String> iter = null;
        while (true) {
            if (queuedIncrements.isEmpty()) break;
            if (iter == null || ! iter.hasNext()) {
                if (iter != null) {
                    long waitMilliseconds = (long) (1000 * waitSeconds);
                    try {
                        Thread.sleep(waitMilliseconds);
                    } catch (InterruptedException ex) {
                        getThread().interrupt();
                    }
                }
                sleepMilliseconds = (long) (1000 * (executeSeconds / (double) queuedIncrements.size())) - 10L;
                if (sleepMilliseconds <= 0) sleepMilliseconds = 10L;
                //LOGGER.info("running MetricIncrementQueueTask within " + executeSeconds + " seconds, approx. size: " + queuedIncrements.size() + " sleeping for " + sleepMilliseconds + " milliseconds between executions");
                iter = queuedIncrements.keySet().iterator();
            }
            String key = iter.next();
            QueuedMetricIncrement queuedIncrement = queuedIncrements.remove(key);
            //LOGGER.info("Incrementing : " + queuedIncrement.metricDatabase.toKeyString() + " : " + queuedIncrement.dimensionId + " += " + queuedIncrement.amount );
            try {
                queuedIncrement.metricDatabase.incrementMetricByDimensionId(queuedIncrement.dimensionId, queuedIncrement.amount);
            } catch (SQLException ex) {
                LOGGER.error("SQLException during incrementMetricByDimensionId: " + ex.getLocalizedMessage());
                // TODO: log this somewhere so it can be recovered if the database fails catastrophically
                throw new DatabaseException(queuedIncrement.metricDatabase.getDatabase(), "SQLException during MetricDatabase.incrementMetricByDimensionId", ex);
            }
            try {
                Thread.sleep(sleepMilliseconds);
            } catch (InterruptedException ex) {
                getThread().interrupt();
            }
        }

    }

}
