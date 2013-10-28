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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Task;
import com.psddev.dari.util.UuidUtils;

class BootstrapImportTask extends Task {

    public final static String EXECUTOR_PREFIX = "Bootstrap Import";

    private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapImportTask.class);

    private static final int COMMIT_SIZE = 100;
    private static final int NUM_WRITERS = 5;
    private static final int QUEUE_SIZE = COMMIT_SIZE * NUM_WRITERS;

    private final Database database;
    private final String filename;
    private final InputStream fileInputStream;
    private final boolean deleteFirst;
    private final AsyncQueue<Record> saveQueue = new AsyncQueue<Record>(new ArrayBlockingQueue<Record>(QUEUE_SIZE));
    private final List<AsyncDatabaseWriter<Record>> savers = new ArrayList<AsyncDatabaseWriter<Record>>();
    private final List<AsyncDatabaseWriter<Record>> deleters = new ArrayList<AsyncDatabaseWriter<Record>>();
    private AsyncQueue<Record> deleteQueue;
    private final Map<UUID,UUID> remoteToLocalTypeIdMap = new HashMap<UUID,UUID>();
    private final Map<String,String> remoteToLocalTypeIdStringMap = new HashMap<String,String>();
    private final Map<String,UUID> remoteTypeNameToTypeIdMap = new HashMap<String,UUID>();
    private final Map<UUID,ObjectType> unknownTypes = new HashMap<UUID,ObjectType>();
    private boolean needsTranslation;
    private Pattern typeIdTranslationPattern;

    public BootstrapImportTask(Database database, String filename, InputStream fileInputStream, boolean deleteFirst) {
        super(EXECUTOR_PREFIX, EXECUTOR_PREFIX + " " + filename);
        this.database = database;
        this.filename = filename;
        this.fileInputStream = fileInputStream;
        this.deleteFirst = deleteFirst;
        if (deleteFirst) {
            this.deleteQueue = new AsyncQueue<Record>(new ArrayBlockingQueue<Record>(QUEUE_SIZE));
        }
    }

    public void doTask() throws IOException {

        List<Task> tasks = new ArrayList<Task>();
        try {
            for (int i = 0; i < NUM_WRITERS; i++) {
                AsyncDatabaseWriter<Record> saver = new AsyncDatabaseWriter<Record>(EXECUTOR_PREFIX, saveQueue, database, WriteOperation.SAVE_UNSAFELY, COMMIT_SIZE, false);
                savers.add(saver);
                saver.submit();
                if (deleteFirst) {
                    AsyncDatabaseWriter<Record> deleter = new AsyncDatabaseWriter<Record>("Bootstrap Delete " + filename, deleteQueue, database, WriteOperation.DELETE, COMMIT_SIZE, false);
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

            if (! headers.containsKey(Bootstrap.Static.TYPES_HEADER) || headers.get(Bootstrap.Static.TYPES_HEADER) == null || "".equals(headers.get(Bootstrap.Static.TYPES_HEADER).trim())) {
                throw new RuntimeException("Missing " + Bootstrap.Static.TYPES_HEADER + " header");
            }
            if (! headers.containsKey(Bootstrap.Static.TYPE_MAP_HEADER) || headers.get(Bootstrap.Static.TYPE_MAP_HEADER) == null || "".equals(headers.get(Bootstrap.Static.TYPE_MAP_HEADER).trim())) {
                throw new RuntimeException("Missing " + Bootstrap.Static.TYPE_MAP_HEADER + " header");
            }
            if (! headers.containsKey(Bootstrap.Static.ROW_COUNT_HEADER) || headers.get(Bootstrap.Static.ROW_COUNT_HEADER) == null || "".equals(headers.get(Bootstrap.Static.ROW_COUNT_HEADER).trim())) {
                throw new RuntimeException("Missing "+Bootstrap.Static.ROW_COUNT_HEADER+" header");
            }
            setProgressTotal(ObjectUtils.to(Long.class, headers.get(Bootstrap.Static.ROW_COUNT_HEADER)));
            UUID localObjTypeId = database.getEnvironment().getTypeByClass(ObjectType.class).getId();
            UUID globalsId = new UUID(-1L, -1L);
            for (String typeIdEqType : headers.get(Bootstrap.Static.TYPE_MAP_HEADER).split(",")) {
                String[] p = typeIdEqType.split("=", 2);
                if (p.length != 2) continue;
                UUID remoteTypeId = UuidUtils.fromString(p[0].trim());
                String remoteTypeName = p[1].trim();
                remoteTypeNameToTypeIdMap.put(remoteTypeName, remoteTypeId);
                ObjectType localType = database.getEnvironment().getTypeByName(remoteTypeName);
                if (localType == null) {
                    localType = new ObjectType();
                    localType.setInternalName(remoteTypeName);
                    localType.setDisplayName("Unknown Type: " + remoteTypeName);
                    unknownTypes.put(localType.getId(), localType);
                }
                UUID localTypeId = localType.getId();
                if (! localTypeId.equals(remoteTypeId)) {
                    remoteToLocalTypeIdMap.put(remoteTypeId, localTypeId);
                }
            }
            if (headers.get(Bootstrap.Static.TYPES_HEADER).trim().equals(Bootstrap.Static.ALL_TYPES_HEADER_VALUE)) {
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
                for (String remoteTypeName : headers.get(Bootstrap.Static.TYPES_HEADER).split(",")) {
                    remoteTypeName = remoteTypeName.trim();
                    UUID remoteTypeId = remoteTypeNameToTypeIdMap.get(remoteTypeName);
                    UUID localTypeId = remoteToLocalTypeIdMap.get(remoteTypeId);
                    if (localTypeId == null) {
                        ObjectType localType = database.getEnvironment().getTypeById(remoteTypeId);
                        if (localType != null) {
                            localTypeId = localType.getId();
                        }
                    }
                    if (localTypeId == null) {
                        throw new RuntimeException("This Bootstrap Package contains an unknown type: " + remoteTypeId + " ("+remoteTypeName+")");
                    }
                    typeIds.add(remoteTypeId);
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
            prepareTypeIdTranslation();
            while (null != (line = reader.readLine())) {
                if (!shouldContinue()) break;
                line = line.trim();
                if ("".equals(line)) continue;
                if (line.startsWith("#")) continue;
                if (! line.startsWith("{") || ! line.endsWith("}")) throw new RuntimeException("Invalid line in input file: " + line);
                line = translateTypeIds(line);
                @SuppressWarnings("unchecked")
                Map<String, Object> stateMap = (Map<String, Object>) ObjectUtils.fromJson(line);
                State state;
                try {
                    state = new State();
                    state.setValues(stateMap);
                    Object obj = state.getOriginalObjectOrNull();
                    if (obj instanceof Record) {
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

    private void prepareTypeIdTranslation() {
        needsTranslation = ! remoteToLocalTypeIdMap.isEmpty();
        for (Map.Entry<UUID,UUID> entry : remoteToLocalTypeIdMap.entrySet()) {
            remoteToLocalTypeIdStringMap.put(entry.getKey().toString(), entry.getValue().toString());
        }
        if (!needsTranslation) return;
        StringBuilder regexBuilder = new StringBuilder();
        regexBuilder.append("(");
        for (String remoteTypeId : remoteToLocalTypeIdStringMap.keySet()) {
            regexBuilder.append(remoteTypeId);
            regexBuilder.append("|");
        }
        regexBuilder.setLength(regexBuilder.length()-1);
        regexBuilder.append(")");
        typeIdTranslationPattern = Pattern.compile(regexBuilder.toString());
    }

    private String translateTypeIds(String line) {
        if (!needsTranslation) return line;
        StringBuilder newLine = new StringBuilder();
        Matcher remoteTypeIdMatcher = typeIdTranslationPattern.matcher(line);
        int cursor = 0;
        while (remoteTypeIdMatcher.find()) {
            int start = remoteTypeIdMatcher.start();
            int end = remoteTypeIdMatcher.end();
            newLine.append(line.substring(cursor, start));
            String remoteTypeId = line.substring(start, end);
            String localTypeId;
            if ((localTypeId = remoteToLocalTypeIdStringMap.get(remoteTypeId)) != null) {
                newLine.append(localTypeId);
            } else {
                newLine.append(remoteTypeId);
            }
            cursor = end;
            ObjectType unknownType;
            if ((unknownType = unknownTypes.remove(localTypeId)) != null) {
                saveQueue.add(unknownType);
            }
        }
        newLine.append(line.substring(cursor));

        return newLine.toString();
    }

}
