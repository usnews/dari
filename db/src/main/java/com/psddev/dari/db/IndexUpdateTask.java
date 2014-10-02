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
 * Periodically updates indexes annotated with {@code \@IndexUpdate}.
 */
public class IndexUpdateTask extends RepeatingTask {

    private static final int UPDATE_LATEST_EVERY_SECONDS = 60;
    private static final int QUERY_ITERABLE_SIZE = 200;
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexUpdateTask.class);

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

        for (IndexUpdateContext context : getIndexableMethods()) {
            updateIndexesIfNecessary(context);
        }

    }

    /**
     * Checks the LastIndexUpdate and DistributedLock and executes updateIndexes if it should.
     */
    private void updateIndexesIfNecessary(IndexUpdateContext context) {
        String updateKey = context.getKey();

        LastIndexUpdate last = Query.from(LastIndexUpdate.class).master().noCache().where("key = ?", updateKey).first();

        boolean shouldExecute = false;
        boolean canExecute = false;
        if (last == null) {
            last = new LastIndexUpdate();
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
            if (Query.from(LastIndexUpdate.class).where("currentRunningDate > ?", new DateTime().minusSeconds(UPDATE_LATEST_EVERY_SECONDS * 5)).hasMoreThan(0)) {
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
                    updateIndexes(context, last);
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
    private void updateIndexes(IndexUpdateContext context, LastIndexUpdate last) {

        // Still testing this out.
        boolean useMetricQuery = false;

        try {
            Query<?> query = Query.fromAll().noCache();
            query.getOptions().put(SqlDatabase.USE_JDBC_FETCH_SIZE_QUERY_OPTION, false);
            for (ObjectMethod method : context.methods) {
                query.or(method.getUniqueName() + " != missing");
            }

            ObjectField metricField = context.getMetricField();
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
                        method.updateIndex(objState);
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
    public static class LastIndexUpdate extends Record {

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

    private static Collection<IndexUpdateContext> getIndexableMethods() {

        Map<String, IndexUpdateContext> contextsByGroupsAndDelayAndMetricField = new HashMap<String, IndexUpdateContext>();

        for (ObjectType type : Database.Static.getDefault().getEnvironment().getTypes()) {
            for (ObjectMethod method : type.getMethods()) {
                if (method.getJavaDeclaringClassName().equals(type.getObjectClassName()) &&
                        !method.as(IndexUpdateFieldData.class).isImmediate()) {

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

                    IndexUpdateContext context = new IndexUpdateContext(type, groups, method.as(IndexUpdateFieldData.class).getIndexUpdateDelay());
                    context.methods.add(method);
                    String key = context.getKey();
                    if (!contextsByGroupsAndDelayAndMetricField.containsKey(key)) {
                        contextsByGroupsAndDelayAndMetricField.put(key, context);
                    }
                    contextsByGroupsAndDelayAndMetricField.get(key).methods.add(method);

                }
            }

        }

        return contextsByGroupsAndDelayAndMetricField.values();
    }

    private static final class IndexUpdateContext {
        public final ObjectType type;
        public final IndexUpdateDelay delay;
        public final TreeSet<String> groups;
        public final Set<ObjectMethod> methods = new HashSet<ObjectMethod>();

        public IndexUpdateContext(ObjectType type, TreeSet<String> groups, IndexUpdateDelay delay) {
            this.type = type;
            this.groups = groups;
            this.delay = delay;
        }

        public ObjectField getMetricField() {
            boolean first = true;
            ObjectField metricField = null;
            ObjectField useMetricField = null;
            for (ObjectMethod method : methods) {
                String methodMetricFieldName = method.as(MetricAccess.FieldData.class).getIndexUpdateFieldName();
                ObjectField methodMetricfield = null;
                if (methodMetricFieldName != null) {
                    methodMetricfield = type.getState().getDatabase().getEnvironment().getField(methodMetricFieldName);
                    if (methodMetricfield == null) {
                        methodMetricfield = type.getField(methodMetricFieldName);
                    }
                    if (methodMetricfield == null) {
                        LOGGER.warn("Invalid metric field: " + methodMetricFieldName);
                    }
                }
                if (first) {
                    metricField = methodMetricfield;
                    useMetricField = metricField;
                } else {
                    if (!ObjectUtils.equals(metricField, methodMetricfield)) {
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

            ObjectField metricField = getMetricField();
            return type.getInternalName() + " " +
                StringUtils.join(groups.toArray(new String[0]), ",") + " " +
                delay.getClass().getName() +
                (metricField != null ? " " + metricField.getUniqueName() : "");
        }

    }

}
