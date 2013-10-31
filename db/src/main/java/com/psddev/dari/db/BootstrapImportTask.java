package com.psddev.dari.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Task;

class BootstrapImportTask extends Task {

    public static final String EXECUTOR_PREFIX = "Bootstrap Import";

    private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapImportTask.class);

    private final Database database;
    private final String filename;
    private final InputStream fileInputStream;
    private final boolean deleteFirst;
    private final int queueSize;
    private final int numWriters;
    private final int commitSize;
    private final AsyncQueue<Record> saveQueue;
    private final List<AsyncDatabaseWriter<Record>> savers = new ArrayList<AsyncDatabaseWriter<Record>>();
    private final List<AsyncDatabaseWriter<Record>> deleters = new ArrayList<AsyncDatabaseWriter<Record>>();
    private AsyncQueue<Record> deleteQueue;
    private final Map<UUID,ObjectType> unknownTypes = new HashMap<UUID,ObjectType>();

    public BootstrapImportTask(Database database, String filename, InputStream fileInputStream, boolean deleteFirst, int numWriters, int commitSize) {
        super(EXECUTOR_PREFIX, EXECUTOR_PREFIX + " " + filename);
        this.database = database;
        this.filename = filename;
        this.fileInputStream = fileInputStream;
        this.deleteFirst = deleteFirst;
        this.numWriters = numWriters;
        this.commitSize = commitSize;
        this.queueSize = numWriters * commitSize;
        this.saveQueue = new AsyncQueue<Record>(new ArrayBlockingQueue<Record>(queueSize));
        if (deleteFirst) {
            this.deleteQueue = new AsyncQueue<Record>(new ArrayBlockingQueue<Record>(queueSize));
        }
    }

    public void doTask() throws IOException {
        List<Task> tasks = new ArrayList<Task>();
        try {
            for (int i = 0; i < numWriters; i++) {
                AsyncDatabaseWriter<Record> saver = new AsyncDatabaseWriter<Record>(EXECUTOR_PREFIX, saveQueue, database, WriteOperation.SAVE_UNSAFELY, commitSize, false);
                savers.add(saver);
                saver.submit();
                if (deleteFirst) {
                    AsyncDatabaseWriter<Record> deleter = new AsyncDatabaseWriter<Record>("Bootstrap Delete " + filename, deleteQueue, database, WriteOperation.DELETE, commitSize, false);
                    deleters.add(deleter);
                    deleter.submit();
                }
            }
            tasks.addAll(deleters);
            tasks.addAll(savers);

            BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
            String line;
            // get headers
            // read leading blank lines
            do {
                line = reader.readLine(); 
                if (line != null) line = line.trim();
            } while ("".equals(line));

            Map<String, String> headers = new HashMap<String, String>();
            do {
                if (line != null) line = line.trim();
                String[] parts = line.split(":", 2);
                if (parts.length == 2) {
                    headers.put(parts[0].trim(), parts[1].trim());
                }
            } while (! "".equals((line = reader.readLine())));

            if (deleteFirst) {
                if (! headers.containsKey(BootstrapPackage.Static.TYPES_HEADER) || headers.get(BootstrapPackage.Static.TYPES_HEADER) == null || "".equals(headers.get(BootstrapPackage.Static.TYPES_HEADER).trim())) {
                    throw new RuntimeException("Missing " + BootstrapPackage.Static.TYPES_HEADER + " header");
                }
            }
            if (! headers.containsKey(BootstrapPackage.Static.ROW_COUNT_HEADER) || headers.get(BootstrapPackage.Static.ROW_COUNT_HEADER) == null || "".equals(headers.get(BootstrapPackage.Static.ROW_COUNT_HEADER).trim())) {
            } else {
                setProgressTotal(ObjectUtils.to(Long.class, headers.get(BootstrapPackage.Static.ROW_COUNT_HEADER)));
            }
            UUID localObjTypeId = database.getEnvironment().getTypeByClass(ObjectType.class).getId();
            UUID globalsId = new UUID(-1L, -1L);
            if (headers.get(BootstrapPackage.Static.TYPES_HEADER).trim().equals(BootstrapPackage.Static.ALL_TYPES_HEADER_VALUE)) {
                if (deleteFirst) {
                    LOGGER.info("Deleting all records in database to load " + filename);
                    for (Object obj : Query.fromAll().where("_type != ?", localObjTypeId).and("_id != ?", globalsId).noCache().using(database).resolveToReferenceOnly().iterable(100)) {
                        if (!shouldContinue()) break;
                        if (obj instanceof Record) {
                            deleteQueue.add((Record) obj);
                        }
                    }
                }
            } else {
                List<UUID> typeIds = new ArrayList<UUID>();
                for (String remoteTypeName : headers.get(BootstrapPackage.Static.TYPES_HEADER).split(",")) {
                    remoteTypeName = remoteTypeName.trim();
                    ObjectType localType = database.getEnvironment().getTypeByName(remoteTypeName);
                    if (localType == null) {
                        localType = new ObjectType();
                        localType.setInternalName(remoteTypeName);
                        localType.setDisplayName("Unknown Type: " + remoteTypeName);
                        unknownTypes.put(localType.getId(), localType);
                    } else {
                        typeIds.add(localType.getId());
                    }
                }
                if (deleteFirst) {
                    LOGGER.info("Deleting all records of types specified in " + filename);
                    for (Object obj : Query.fromAll().where("_type = ?", typeIds).and("_type != ?", localObjTypeId).and("_id != ?", globalsId).noCache().using(database).resolveToReferenceOnly().iterable(100)) {
                        if (!shouldContinue()) break;
                        if (obj instanceof Record) {
                            deleteQueue.add((Record) obj);
                        }
                    }
                }
            }

            // block until deleters are done
            if (deleteQueue != null) {
                deleteQueue.closeAutomatically();
                boolean done = false;
                while (! done) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        continue;
                    }
                    if (! shouldContinue()) {
                        for (Task deleter : deleters) {
                            deleter.stop();
                        }
                        break;
                    }
                    done = true;
                    for (Task deleter : deleters) {
                        if (deleter.isRunning()) {
                            done = false;
                            break;
                        }
                    }
                }
            }
            LOGGER.info("Importing data from " + filename + " . . . ");
            int numRows = 0;
            while (null != (line = reader.readLine())) {
                if (!shouldContinue()) break;
                line = line.trim();
                if ("".equals(line)) continue;
                if (line.startsWith("#")) continue;
                if (! line.startsWith("{") || ! line.endsWith("}")) throw new RuntimeException("Invalid line in input file: " + line);
                // line = translateTypeIds(line);
                @SuppressWarnings("unchecked")
                Map<String, Object> stateMap = (Map<String, Object>) ObjectUtils.fromJson(line);
                State state;
                try {
                    state = new State();
                    state.setValues(stateMap);
                    Object obj = state.getOriginalObjectOrNull();
                    if (obj instanceof Record) {
                        if (obj instanceof ObjectType) {
                            ObjectType localObjType = database.getEnvironment().getTypeByName(((ObjectType) obj).getInternalName());
                            if (localObjType != null && ! localObjType.getId().equals(((ObjectType) obj).getId())) {
                                ((ObjectType) obj).getState().setId(localObjType.getId());
                            }
                        }
                        saveQueue.add((Record) obj);
                    } else {
                        Record record = new Record();
                        record.setState(state);
                        saveQueue.add(record);
                    }
                    setProgressIndex(++numRows);
                } catch (RuntimeException t) {
                    LOGGER.error("Error when saving state at "+stateMap.get("_id")+": ", t);
                }
            }
        } catch (RuntimeException e) {
            for (Task task : tasks) {
                task.stop();
            }
            throw e;
        } finally {
            // block until all tasks are done
            saveQueue.closeAutomatically();
            if (deleteQueue != null) {
                deleteQueue.closeAutomatically();
            }
            boolean done = false;
            while (! done) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    continue;
                }
                if (! shouldContinue()) {
                    for (Task task : tasks) {
                        task.stop();
                    }
                    break;
                }
                done = true;
                for (Task saver : savers) {
                    if (saver.isRunning()) {
                        done = false;
                        break;
                    }
                }
            }
            LOGGER.info("Done with import of " + filename + ".");
        }
    }

}
