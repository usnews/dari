package com.psddev.dari.db;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.RepeatingTask;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.Task;
import com.psddev.dari.util.TypeReference;

public class PeriodicMetricResummarizationTask extends RepeatingTask {

    private static Logger LOGGER = LoggerFactory.getLogger(PeriodicMetricResummarizationTask.class);
    private static String CONFIG_PREFIX = "dari/metricResummary";
    private static String CONFIG_FIELDS = "fields";
    private static String CONFIG_BEFORE_DAYS = "beforeDays";
    private static String CONFIG_INTERVAL_CLASS = "intervalClass";
    private static String CONFIG_DATABASE = "database";
    private static Map<String,Map<String,Object>> CONFIG = Settings.get(new TypeReference<Map<String,Map<String,Object>>>() {}, CONFIG_PREFIX);

    @Override
    protected DateTime calculateRunTime(DateTime currentTime) {
        if (CONFIG == null || CONFIG.isEmpty()) {
            return every(currentTime, DateTimeFieldType.era(), 0, 1);
        } else {
            return everyDay(currentTime);
        }
    }

    @Override
    protected void doRepeatingTask(DateTime runTime) throws Exception {
        if (CONFIG == null || CONFIG.isEmpty()) {
            return;
        }

        for (Map.Entry<String, Map<String, Object>> entry : CONFIG.entrySet()) {

            String key = entry.getKey();
            Map<String,Object> settings = entry.getValue();
            String fieldsStr = ObjectUtils.to(String.class, settings.get(CONFIG_FIELDS));

            if (ObjectUtils.isBlank(fieldsStr)) {
                LOGGER.warn("Metric Resummarization: " + CONFIG_PREFIX + "/" + key + "/" + CONFIG_FIELDS + " is required; aborting.");
                continue;
            }
            String[] fieldSpecs = fieldsStr.split("\\s+");

            Integer beforeDays = ObjectUtils.to(Integer.class, settings.get(CONFIG_BEFORE_DAYS));
            if (beforeDays == null) {
                LOGGER.warn("Metric Resummarization: " + CONFIG_PREFIX + "/" + key + "/" + CONFIG_BEFORE_DAYS + " is required; aborting.");
                continue;
            }

            String intervalClassName = ObjectUtils.to(String.class, settings.get(CONFIG_INTERVAL_CLASS));
            if (intervalClassName == null) {
                LOGGER.warn("Metric Resummarization: " + CONFIG_PREFIX + "/" + key + "/" + CONFIG_INTERVAL_CLASS + " is required; aborting.");
                continue;
            }

            Database database = Database.Static.getDefault();
            String databaseName = ObjectUtils.to(String.class, settings.get(CONFIG_DATABASE));
            if (databaseName != null) {
                database = Database.Static.getInstance(databaseName);
                if (database == null) {
                    LOGGER.warn("Metric Resummarization: " + CONFIG_PREFIX + "/" + key + "/" + CONFIG_DATABASE + " is an invalid database; aborting.");
                    continue;
                }
            }

            submitResummarizationTask (database, key, fieldSpecs, beforeDays, intervalClassName);

        }
    }

    private void submitResummarizationTask (Database database, String key, String[] fieldSpecs, int beforeDays, String intervalClassName) {

        Set<ObjectField> fields = resolveFieldSpecs(database, key, fieldSpecs);
        if (fields == null || fields.isEmpty()) {
            LOGGER.warn("Metric Resummarization: " + CONFIG_PREFIX + "/" + key + " specifies no valid fields to resummarize; aborting.");
            return;
        }

        Set<String> debugFieldNames = new HashSet<String>();
        for (ObjectField field : fields) {
            debugFieldNames.add(field.getUniqueName());
        }

        MetricInterval interval;
        try {
            @SuppressWarnings("unchecked")
            Class<MetricInterval> intervalClass = (Class<MetricInterval>) Class.forName(intervalClassName);
            interval = intervalClass.newInstance();
        } catch (Exception e) {
            LOGGER.warn("Metric Resummarization: " + CONFIG_PREFIX + "/" + key + "/" + CONFIG_INTERVAL_CLASS + " specifies an invalid MetricInterval class; aborting.");
            return;
        }

        LastResummarization last = Query.from(LastResummarization.class).using(database).where("key = ?", key).first();
        if (last == null) {
            last = new LastResummarization();
            last.getState().setDatabase(database);
            last.setKey(key);
        }

        // Ensure we're only running once per day. . .
        if (last.getRunDate() != null && last.getRunDate().isAfter(new DateTime().minusDays(1))) {
            // LOGGER.warn("Metric Resummarization: " + CONFIG_PREFIX + "/" + key + " last ran on " + last.getRunDate().toString("yyyy/MM/dd HH:mm:ss") + ", aborting." );
            return;
        }

        // Set up the new date range
        DateTime startDate = last.getEndDate();
        DateTime endDate = new DateTime().dayOfMonth().roundFloorCopy().minusDays(beforeDays);
        last.setRunDate(new DateTime());
        last.setStartDate(startDate);
        last.setEndDate(endDate);
        last.save();
        for (ObjectField field : fields) {
            Task task = Metric.Static.submitResummarizeAllBetweenTask(database, field.getParentType(), field, interval, startDate, endDate, 1, "Periodic Metric Resummarization", key + " (" + field.getUniqueName()+")");
            do {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    break;
                }
                if (!task.isRunning()) {
                    break;
                }
            } while (shouldContinue());
        }

    }

    private Set<ObjectField> resolveFieldSpecs(Database database, String key, String[] fieldSpecs) {

        Set<ObjectField> fields = new HashSet<ObjectField>();
        for (String fieldSpec : fieldSpecs) {
            String[] parts = fieldSpec.split("/");
            if (parts.length == 1) {
                // looking for a global field
                if (fieldSpec.endsWith("*")) {
                    String fieldPrefix = fieldSpec.substring(0, fieldSpec.length()-1);
                    for (ObjectField field : database.getEnvironment().getFields()) {
                        if (field.getInternalName().startsWith(fieldPrefix) && field.isMetric()) {
                            fields.add(field);
                        }
                    }
                } else {
                    ObjectField field = database.getEnvironment().getField(fieldSpec);
                    if (field == null || !field.isMetric()) {
                        LOGGER.warn("Metric Resummarization: " +CONFIG_PREFIX + "/" + key + "/" + CONFIG_FIELDS + " : " + fieldSpec + " specifies an invalid field.");
                        return null;
                    }
                    fields.add(field);
                }
            } else if (parts.length == 2) {
                // looking for a type field
                String typeName = parts[0];
                String typeFieldSpec = parts[1];
                ObjectType type = database.getEnvironment().getTypeByName(typeName);
                if (type == null) {
                    LOGGER.warn("Metric Resummarization: " +CONFIG_PREFIX + "/" + key + "/" + CONFIG_FIELDS + " : " + fieldSpec + " specifies an invalid type.");
                    return null;
                }
                if (typeFieldSpec.endsWith("*")) {
                    String typeFieldPrefix = typeFieldSpec.substring(0, typeFieldSpec.length()-1);
                    for (ObjectField field : type.getFields()) {
                        if (field.getInternalName().startsWith(typeFieldPrefix) && field.isMetric()) {
                            fields.add(field);
                        }
                    }
                } else {
                    ObjectField field = type.getField(typeFieldSpec);
                    if (field == null || !field.isMetric()) {
                        LOGGER.warn("Metric Resummarization: " +CONFIG_PREFIX + "/" + key + "/" + CONFIG_FIELDS + " : " + fieldSpec + " specifies an invalid field.");
                        return null;
                    }
                    fields.add(field);
                }

            } else {
                LOGGER.warn("Metric Resummarization: " +CONFIG_PREFIX + "/" + key + "/" + CONFIG_FIELDS + " : " + fieldSpec + " specifies an invalid field.");
                return null;
            }

        }

        return fields;
    }

    public static class LastResummarization extends Record {

        private Long startDate;

        private Long endDate;

        private Long runDate;

        @Indexed(unique = true)
        private String key;

        public DateTime getStartDate() {
            return (startDate == null ? null : new DateTime(startDate));
        }

        public void setStartDate(DateTime startDate) {
            this.startDate = (startDate == null ? null : startDate.getMillis());
        }

        public DateTime getEndDate() {
            return (endDate == null ? null : new DateTime(endDate));
        }

        public void setEndDate(DateTime endDate) {
            this.endDate = (endDate == null ? null : endDate.getMillis());
        }

        public DateTime getRunDate() {
            return (runDate == null ? null : new DateTime(runDate));
        }

        public void setRunDate(DateTime runDate) {
            this.runDate = (runDate == null ? null : runDate.getMillis());
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

    }

}
