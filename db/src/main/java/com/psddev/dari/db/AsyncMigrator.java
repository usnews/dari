package com.psddev.dari.db;

import com.psddev.dari.db.Record;
import com.psddev.dari.db.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.AsyncProcessor;
import com.psddev.dari.util.AsyncProducer;
import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.ObjectUtils;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public abstract class AsyncMigrator<T extends Record> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncMigrator.class);

    private Integer rateLimit = 0;
    private boolean remigrate = false;
    private boolean remigrateComplete = false;
    private boolean remigrateFailed = false;
    private boolean saveToSolr = false;
    private boolean saveSafely = false;
    private boolean singleQuery = false;
    private String whereClause = null;

    private List<UUID> migrationIds = new ArrayList<UUID>();

    public abstract Query<T> getMigrationQuery();

    public void addMigrationIds(String ids) {
        if (ObjectUtils.isBlank(ids)) {
            return;
        }

        String[] split = ids.trim().split(" ");
        for(String id : split) {
            try {
                migrationIds.add(UUID.fromString(id));
                LOGGER.info("Adding " + id + " to list to migrate.");
            } catch(Exception ex) {
                LOGGER.error("Failed to parse migration ids.", ex);
            }
        }
    }

    public void addWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }

    public void setSaveToSolr(boolean saveToSolr) {
        this.saveToSolr = saveToSolr;
    }

    public boolean shouldSaveToSolr() {
        return saveToSolr;
    }

    public boolean shouldRemigrateComplete(T object) {
        if (remigrateComplete && object != null && object.as(Migration.class).getStatus() == Migration.Status.COMPLETE) {
            return true;
        }

        return false;
    }

    public void setRemigrateComplete(boolean remigrateComplete) {
        this.remigrateComplete = remigrateComplete;
    }

    public void setRemigrateFailed(boolean remigrateFailed) {
        this.remigrateFailed = remigrateFailed;
    }

    public boolean shouldRemigrateFailed(T object) {
        if (remigrateFailed && object != null && object.as(Migration.class).getStatus() == Migration.Status.FAILED) {
            return true;
        }

        return false;
    }

    public void setRemigrate(boolean remigrate) {
        this.remigrate = remigrate;
    }

    public boolean shouldRemigrate() {
        return remigrate;
    }

    public boolean shouldMigrate(T object) {
        if (shouldRemigrate() ||
                shouldRemigrateComplete(object) ||
                shouldRemigrateFailed(object) ||
                object.as(Migration.class).getStatus() == null ||
                object.as(Migration.class).getStatus() == Migration.Status.INGESTED) {
            return true;
        }

        return false;
    }

    public void didMigrate(T object) {
        if (!ObjectUtils.isBlank(object))
            object.as(Migration.class).setStatus(Migration.Status.COMPLETE);
    }

    public T doMigrate(T object) {
        return object;
    }

    public T doMigrate(T object, AsyncQueue queue) {
        return doMigrate(object);
    }

    public void migrate(int processorThread, int writerThreads, int batchSize) {
        LOGGER.info("Starting migration: " + this.getClass());

        Database.Static.setDefaultOverride(null);
        Query query = getMigrationQuery();
        if (migrationIds.size() > 0) {
            setRemigrate(true);
            query.and("id = ?", migrationIds);

            LOGGER.info("Only migrating provided IDs.");
        }

        if (!singleQuery) {
            query.option(SqlDatabase.USE_JDBC_FETCH_SIZE_QUERY_OPTION, false);
        }

        if (whereClause != null) {
            query.where(whereClause);
        }

        final Integer minTimeBetween = rateLimit == 0 ? 0 : 1000 / rateLimit;
        final Long count = 0L;
        final Iterator<T> iterator = query.iterable(batchSize).iterator();
        AsyncProducer<T> producer = new AsyncProducer<T>(this.getClass().getName(), null) {
            private Long timeSinceLast = 0L;

            @Override
            protected T produce() {
                Long timeSpent = System.currentTimeMillis() - timeSinceLast;
                if (timeSpent < minTimeBetween) {
                    try {
                        Thread.sleep(minTimeBetween - timeSpent);
                    } catch (Exception ex) {
                        LOGGER.error("Rate limiting failed.", ex);
                        return null;
                    }
                }

                timeSinceLast = System.currentTimeMillis();

                return iterator.hasNext() ? iterator.next() : null;
            }

            @Override
            protected void beforeStart() {
                setProgressTotal(count);
            }
        };
        producer.start();

        AsyncQueue<T> queue = new AsyncQueue<T>(new ArrayBlockingQueue<T>(batchSize * writerThreads));
        final AsyncQueue<T> failQueue = new AsyncQueue<T>(new ArrayBlockingQueue<T>(10));

        for (int i = 0; i < processorThread; i++) {
            AsyncProcessor<T, T> processor = new AsyncProcessor<T, T>(this.getClass().getName(), producer.getOutput(), queue) {

                long current;

                @Override
                protected T process(T item) {
                    if (shouldMigrate(item)) {
                        try {
                            T result = doMigrate(item, getOutput());

                            if (result != null) {
                                didMigrate(result);
                                result.getState().remove("dari.migration.error");
                            }

                            return result;
                        } catch(Exception ex) {
                            item.as(Migration.class).setStatus(Migration.Status.FAILED);
                            item.getState().putValue("dari.migration.error", ex.getMessage());

                            item.getState().addError(null, "");

                            LOGGER.warn("Migration of " + item.getClass() + " failed: " +
                                    item.getState().getId() + " at " +
                                    ex.getStackTrace()[0] + " with error: " + ex.getMessage());

                            failQueue.add(item);

                            return null;
                        }
                    }

                    return null;
                }
            };

            processor.start();
        }

        Database database = saveToSolr ? Database.Static.getDefault() :
                Database.Static.getFirst(SqlDatabase.class);
        for (int i = 0; i < writerThreads; i++) {
            AsyncDatabaseWriter<T> writer = new AsyncDatabaseWriter<T>(this.getClass().getName(), queue, database,WriteOperation.SAVE, batchSize, true);
            writer.start();
        }

        AsyncDatabaseWriter<T> failWriter = new AsyncDatabaseWriter<T>(this.getClass().getName(),
                failQueue, database, WriteOperation.SAVE, batchSize, true);
        failWriter.start();

        queue.closeAutomatically();
        producer.getOutput().closeAutomatically();
    }

    public boolean isSaveSafely() {
        return saveSafely;
    }

    public void setSaveSafely(boolean saveSafely) {
        this.saveSafely = saveSafely;
    }

    public boolean isSingleQuery() {
        return singleQuery;
    }

    public void setSingleQuery(boolean singleQuery) {
        this.singleQuery = singleQuery;
    }

    public Integer getRateLimit() {
        return rateLimit;
    }

    public void setRateLimit(Integer rateLimit) {
        this.rateLimit = rateLimit;
    }
}