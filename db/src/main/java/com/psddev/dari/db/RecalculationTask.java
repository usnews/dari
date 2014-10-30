package com.psddev.dari.db;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.RepeatingTask;
import com.psddev.dari.util.StringUtils;

/**
 * Periodically updates indexes annotated with {@code \@Recalculate}.
 */
public class RecalculationTask extends RepeatingTask {

    private static final int UPDATE_LATEST_EVERY_SECONDS = 60;
    private static final int QUERY_ITERABLE_SIZE = 200;
    private static final Logger LOGGER = LoggerFactory.getLogger(RecalculationTask.class);

    private int progressIndex = 0;
    private int progressTotal = 0;

    @Override
    protected DateTime calculateRunTime(DateTime currentTime) {
        return everyMinute(currentTime);
    }

    @Override
    protected void doRepeatingTask(DateTime runTime) throws Exception {

        progressIndex = 0;
        progressTotal = 0;

        for (RecalculationContext context : getIndexableMethods()) {
            recalculateIfNecessary(context);
        }

    }

    /**
     * Checks the LastRecalculation and DistributedLock and executes recalculate if it should.
     */
    private void recalculateIfNecessary(RecalculationContext context) {
        String updateKey = context.getKey();

        LastRecalculation last = Query.from(LastRecalculation.class).master().noCache().where("key = ?", updateKey).first();

        boolean shouldExecute = false;
        boolean canExecute = false;
        if (last == null) {
            last = new LastRecalculation();
            last.setKey(updateKey);
            shouldExecute = true;
        } else {
            if (last.getLastExecutedDate() == null && last.getCurrentRunningDate() == null) {
                // this has never been executed, and it's not currently executing.
                shouldExecute = true;
            } else if (last.getLastExecutedDate() != null && context.delay.isUpdateDue(new DateTime(), last.getLastExecutedDate())) {
                // this has been executed before and an update is due.
                shouldExecute = true;
            }
            if (last.getCurrentRunningDate() != null) {
                // this is currently executing
                shouldExecute = false;
                if (context.delay.isUpdateDue(new DateTime(), last.getCurrentRunningDate())) {
                    // the task is running, but another update is already due.
                    // It probably died. Clear it out and pick it up next
                    // time if it hasn't updated, but don't run this time.
                    last.setCurrentRunningDate(null);
                    last.saveImmediately();
                }
            }

        }

        if (shouldExecute) {
            // Check to see if any other processes are currently running on other hosts.
            if (Query.from(LastRecalculation.class).where("currentRunningDate > ?", new DateTime().minusSeconds(UPDATE_LATEST_EVERY_SECONDS * 5)).hasMoreThan(0)) {
                shouldExecute = false;
            }
        }

        if (shouldExecute) {
            boolean locked = false;
            DistributedLock lock = new DistributedLock(Database.Static.getDefault(), updateKey);
            try {
                if (lock.tryLock()) {
                    locked = true;
                    last.setCurrentRunningDate(new DateTime());
                    last.saveImmediately();
                    canExecute = true;
                }
            } finally {
                if (locked) {
                    lock.unlock();
                }
            }

            if (canExecute) {
                try {
                    recalculate(context, last);
                } finally {
                    last.setLastExecutedDate(new DateTime());
                    last.setCurrentRunningDate(null);
                    last.saveImmediately();
                }
            }

        }

    }

    /**
     * Actually does the work of iterating through the records and updating indexes.
     */
    private void recalculate(RecalculationContext context, LastRecalculation last) {

        // Still testing this out.
        boolean useMetricQuery = false;

        try {
            Query<?> query = Query.fromAll().noCache();
            query.getOptions().put(SqlDatabase.USE_JDBC_FETCH_SIZE_QUERY_OPTION, false);
            for (ObjectMethod method : context.methods) {
                query.or(method.getUniqueName() + " != missing");
            }

            ObjectField metricField = context.getMetric();
            DateTime processedLastRunDate = last.getLastExecutedDate();
            if (metricField != null) {
                if (last.getLastExecutedDate() != null) {
                    MetricInterval interval = metricField.as(MetricAccess.FieldData.class).getEventDateProcessor();
                    if (interval != null) {
                        processedLastRunDate = new DateTime(interval.process(processedLastRunDate));
                        if (useMetricQuery) {
                            query.and(metricField.getUniqueName() + "#date >= ?", processedLastRunDate);
                            query.and(metricField.getUniqueName() + " != 0");
                        }
                    }
                }
            }

            for (Object obj : query.iterable(QUERY_ITERABLE_SIZE)) {
                try {
                    if (!shouldContinue()) {
                        break;
                    }
                    setProgressTotal(++progressTotal);
                    State objState = State.getInstance(obj);
                    if (objState == null) {
                        continue;
                    }
                    if (!useMetricQuery) {
                        if (last.getLastExecutedDate() != null) {
                            if (metricField != null) {
                                Metric metric = new Metric(objState, metricField);
                                DateTime lastMetricUpdate = metric.getLastUpdate();
                                if (lastMetricUpdate == null) {
                                    // there's no metric data, so just pass.
                                    continue;
                                }
                                if (lastMetricUpdate.isBefore(processedLastRunDate.minusSeconds(1))) {
                                    // metric data is older than the last run date, so skip it.
                                    continue;
                                }
                            }
                        }
                    }
                    setProgressIndex(++progressIndex);
                    for (ObjectMethod method : context.methods) {
                        LOGGER.debug("Updating Index: " + method.getInternalName() + " for " + objState.getId());
                        method.recalculate(objState);
                    }
                } finally {
                    if (last.getCurrentRunningDate().plusSeconds(UPDATE_LATEST_EVERY_SECONDS).isBeforeNow()) {
                        last.setCurrentRunningDate(new DateTime());
                        last.saveImmediately();
                    }
                }
            }

        } finally {
            last.setCurrentRunningDate(new DateTime());
            last.saveImmediately();
        }

    }

    /**
     * Saves the last time the index update was executed for each context.
     */
    public static class LastRecalculation extends Record {

        // null if it's not running
        @Indexed
        private Long currentRunningDate;

        private Long lastExecutedDate;

        @Indexed(unique = true)
        private String key;

        public DateTime getCurrentRunningDate() {
            return (currentRunningDate == null ? null : new DateTime(currentRunningDate));
        }

        public void setCurrentRunningDate(DateTime currentRunningDate) {
            this.currentRunningDate = (currentRunningDate == null ? null : currentRunningDate.getMillis());
        }

        public DateTime getLastExecutedDate() {
            return (lastExecutedDate == null ? null : new DateTime(lastExecutedDate));
        }

        public void setLastExecutedDate(DateTime lastExecutedDate) {
            this.lastExecutedDate = (lastExecutedDate == null ? null : lastExecutedDate.getMillis());
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

    }

    private static Collection<RecalculationContext> getIndexableMethods() {

        Map<String, RecalculationContext> contextsByGroupsAndDelayAndMetric = new HashMap<String, RecalculationContext>();

        for (ObjectType type : Database.Static.getDefault().getEnvironment().getTypes()) {
            for (ObjectMethod method : type.getMethods()) {
                if (method.getJavaDeclaringClassName().equals(type.getObjectClassName()) &&
                        !method.as(RecalculationFieldData.class).isImmediate()) {

                    TreeSet<String> groups = new TreeSet<String>();
                    if (Modification.class.isAssignableFrom(type.getObjectClass())) {
                        @SuppressWarnings("unchecked")
                        Class<? extends Modification<?>> modClass = ((Class<? extends Modification<?>>) type.getObjectClass());
                        for (Class<?> modifiedClass : Modification.Static.getModifiedClasses(modClass)) {
                            ObjectType modifiedType = ObjectType.getInstance(modifiedClass);
                            if (modifiedType != null) {
                                groups.add(modifiedType.getInternalName());
                            } else {
                                groups.add(modifiedClass.getName());
                            }
                        }
                    } else {
                        groups.add(type.getInternalName());
                    }

                    RecalculationContext context = new RecalculationContext(type, groups, method.as(RecalculationFieldData.class).getRecalculationDelay());
                    context.methods.add(method);
                    String key = context.getKey();
                    if (!contextsByGroupsAndDelayAndMetric.containsKey(key)) {
                        contextsByGroupsAndDelayAndMetric.put(key, context);
                    }
                    contextsByGroupsAndDelayAndMetric.get(key).methods.add(method);

                }
            }

        }

        return contextsByGroupsAndDelayAndMetric.values();
    }

    private static final class RecalculationContext {
        public final ObjectType type;
        public final RecalculationDelay delay;
        public final TreeSet<String> groups;
        public final Set<ObjectMethod> methods = new HashSet<ObjectMethod>();

        public RecalculationContext(ObjectType type, TreeSet<String> groups, RecalculationDelay delay) {
            this.type = type;
            this.groups = groups;
            this.delay = delay;
        }

        public ObjectField getMetric() {
            boolean first = true;
            ObjectField metricField = null;
            ObjectField useMetricField = null;
            for (ObjectMethod method : methods) {
                String methodMetricFieldName = method.as(MetricAccess.FieldData.class).getRecalculableFieldName();
                ObjectField methodMetricField = null;
                if (methodMetricFieldName != null) {
                    methodMetricField = type.getState().getDatabase().getEnvironment().getField(methodMetricFieldName);
                    if (methodMetricField == null) {
                        methodMetricField = type.getField(methodMetricFieldName);
                    }
                    if (methodMetricField == null) {
                        LOGGER.warn("Invalid metric field: " + methodMetricFieldName);
                    }
                }
                if (first) {
                    metricField = methodMetricField;
                    useMetricField = metricField;
                } else {
                    if (!ObjectUtils.equals(metricField, methodMetricField)) {
                        useMetricField = null;
                    }
                }
                first = false;
            }
            return useMetricField;
        }

        public String getKey() {
            if (methods.isEmpty()) {
                throw new IllegalStateException("Add a method before you get the key!");
            }

            ObjectField metricField = getMetric();
            return type.getInternalName() + " " +
                StringUtils.join(groups.toArray(new String[0]), ",") + " " +
                delay.getClass().getName() +
                (metricField != null ? " " + metricField.getUniqueName() : "");
        }

    }

}
