package com.psddev.dari.db;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventHeaderDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventHeaderV4Deserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.FormatDescriptionEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.GtidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.RotateEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.RowsQueryEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.TableMapEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.XidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.psddev.dari.util.StringUtils;

/*
 * NOT Thread-safe. This should always run in a single thread.
 */
public final class MySQLBinaryLogReader implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBinaryLogReader.class);

    private static MySQLBinaryLogReader instance;

    private volatile Cache<ByteBuffer, byte[][]> binLogCache;

    private BinaryLogClient binaryLogClient;

    private String host;

    private int port;

    private String catalog;

    private String username;

    private String password;

    private MySQLBinaryLogReader() { }

    public static MySQLBinaryLogReader getInstance(Cache<ByteBuffer, byte[][]> binLogCache, DataSource dataSource) {

        synchronized (MySQLBinaryLogReader.class) {
            if (instance == null) {

                String jdbcUrl = null;
                String username = null;
                String password = null;

                try {
                    if (dataSource instanceof org.apache.tomcat.dbcp.dbcp.BasicDataSource) {
                        jdbcUrl = ((org.apache.tomcat.dbcp.dbcp.BasicDataSource) dataSource).getUrl();
                        username = ((org.apache.tomcat.dbcp.dbcp.BasicDataSource) dataSource).getUsername();
                        password = ((org.apache.tomcat.dbcp.dbcp.BasicDataSource) dataSource).getPassword();
                    } else if (dataSource instanceof org.apache.commons.dbcp2.BasicDataSource) {
                        jdbcUrl = ((org.apache.commons.dbcp2.BasicDataSource) dataSource).getUrl();
                        username = ((org.apache.commons.dbcp2.BasicDataSource) dataSource).getUsername();
                        password = ((org.apache.commons.dbcp2.BasicDataSource) dataSource).getPassword();
                    } else if (dataSource instanceof com.jolbox.bonecp.BoneCPDataSource) {
                        jdbcUrl = ((com.jolbox.bonecp.BoneCPDataSource) dataSource).getJdbcUrl();
                        username = ((com.jolbox.bonecp.BoneCPDataSource) dataSource).getUsername();
                        password = ((com.jolbox.bonecp.BoneCPDataSource) dataSource).getPassword();
                    } else {
                        LOGGER.error("Cannot recognize datasource. [{}]", dataSource);
                        return null;
                    }
                } catch (NoClassDefFoundError e) {
                    LOGGER.error("Cannot recognize datasource. [{}]", dataSource);
                    return null;
                }

                // Parse jdbc url
                String[] jdbcUrlParts = jdbcUrl.split("://", 2);
                if (!jdbcUrlParts[0].startsWith("jdbc:mysql") || jdbcUrlParts.length < 2) {
                    LOGGER.error("Cannot recognize jdbc url. [{}]", jdbcUrl);
                    return null;
                }
                String[] urlParts = jdbcUrlParts[1].split("/", 2);
                if (urlParts.length < 2) {
                    LOGGER.error("Cannot recognize jdbc url. [{}]", jdbcUrl);
                    return null;
                }
                String[] hostParts = urlParts[0].split(":", 2);
                String catalog = urlParts[1].split("\\?", 2)[0];

                MySQLBinaryLogReader newInstance = new MySQLBinaryLogReader();
                newInstance.setHost(hostParts[0]);
                newInstance.setPort(hostParts.length > 1 ? Integer.valueOf(hostParts[1]) : 3306);
                newInstance.setCatalog(catalog);
                newInstance.setUsername(username);
                newInstance.setPassword(password);
                newInstance.setBinLogCache(binLogCache);
                instance = newInstance;
            }
            return instance;
        }
    }

    private synchronized BinaryLogClient getBinaryLogClient() {
        if (binaryLogClient == null) {
            binaryLogClient = new BinaryLogClient(host, port, catalog, username, password);
            // Custom event handler because shyiko implementation for MySQL byte column decodes it into String type, which can't be converted back to byte[] correctly.
            EventHeaderDeserializer eventHeaderDeserializer = new EventHeaderV4Deserializer();
            EventDataDeserializer defaultEventDataDeserializer = new NullEventDataDeserializer();
            Map<EventType, EventDataDeserializer> eventDataDeserializers = new HashMap<EventType, EventDataDeserializer>();
            Map<Long, TableMapEventData> tableMapEventByTableId = new HashMap<Long, TableMapEventData>();

            eventDataDeserializers.put(EventType.FORMAT_DESCRIPTION, new FormatDescriptionEventDataDeserializer());
            eventDataDeserializers.put(EventType.ROTATE, new RotateEventDataDeserializer());
            eventDataDeserializers.put(EventType.QUERY, new DariQueryEventDataDeserializer());
            eventDataDeserializers.put(EventType.TABLE_MAP, new TableMapEventDataDeserializer());
            eventDataDeserializers.put(EventType.XID, new XidEventDataDeserializer());
            eventDataDeserializers.put(EventType.WRITE_ROWS,
                    new DariWriteRowsEventDataDeserializer(tableMapEventByTableId));
            eventDataDeserializers.put(EventType.UPDATE_ROWS,
                    new DariUpdateRowsEventDataDeserializer(tableMapEventByTableId));
            eventDataDeserializers.put(EventType.DELETE_ROWS,
                    new DariDeleteRowsEventDataDeserializer(tableMapEventByTableId));
            eventDataDeserializers.put(EventType.EXT_WRITE_ROWS,
                    new DariWriteRowsEventDataDeserializer(tableMapEventByTableId).
                            setMayContainExtraInformation(true));
            eventDataDeserializers.put(EventType.EXT_UPDATE_ROWS,
                    new DariUpdateRowsEventDataDeserializer(tableMapEventByTableId).
                            setMayContainExtraInformation(true));
            eventDataDeserializers.put(EventType.EXT_DELETE_ROWS,
                    new DariDeleteRowsEventDataDeserializer(tableMapEventByTableId).
                            setMayContainExtraInformation(true));
            eventDataDeserializers.put(EventType.ROWS_QUERY, new RowsQueryEventDataDeserializer());
            eventDataDeserializers.put(EventType.GTID, new GtidEventDataDeserializer());

            EventDeserializer eventDeserializer = new EventDeserializer(eventHeaderDeserializer,
                    defaultEventDataDeserializer,
                    eventDataDeserializers,
                    tableMapEventByTableId);

            binaryLogClient.setEventDeserializer(eventDeserializer);

            binaryLogClient.registerEventListener(new CacheEventListener());
        }
        return binaryLogClient;
    }

    private void setBinLogCache(Cache<ByteBuffer, byte[][]> binLogCache) {
        this.binLogCache = binLogCache;
    }

    private void setHost(String host) {
        this.host = host;
    }

    private void setPort(int port) {
        this.port = port;
    }

    private void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    private void setUsername(String username) {
        this.username = username;
    }

    private void setPassword(String password) {
        this.password = password;
    }

    @Override
    public void run() {

        BinaryLogClient client = getBinaryLogClient();

        try {
            client.connect();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                client.disconnect();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public class CacheEventListener implements EventListener {

        private final Pattern deletePattern = Pattern.compile("DELETE\\s+FROM\\s+`?(?<table>\\p{Alnum}+)`?\\s+WHERE\\s+`?id`?\\s*(?:(?:IN\\s*\\()|(?:=))\\s*(?<id>(?:(?:[^\']+'){2},?\\s*){1,})\\)?", Pattern.CASE_INSENSITIVE);

        private final Pattern updatePattern = Pattern.compile("UPDATE\\s+`?(?<table>\\p{Alnum}+)`?\\s+SET\\s+`?typeId`?\\s*=\\s*(?<typeId>(?:[^\']+'){2})\\s*,\\s*`?data`?\\s*=\\s*(?<data>.+)\\s*WHERE\\s+`?id`?\\s*(?:(?:IN\\s*\\()|(?:=))\\s*(?<id>(?:[^\']+'){2}).*", Pattern.CASE_INSENSITIVE);

        private boolean transactionBegin = false;

        private TableMapEventData tableMapEventData;

        private final List<Event> events = new ArrayList<Event>();

        private boolean isFlushCache = false;

        /**
         * Makes sure length of the given {@code in} is 16.
         */
        private byte[] confirm16Bytes(byte[] in) {
            if (in == null) {
                return null;
            }
            byte[] bytes16 = new byte[16];
            if (in.length == 16) {
                return in;
            }
            for (int i = 0; i < bytes16.length && i < in.length; i++) {
                bytes16[i] |= in[i];
            }
            return bytes16;
        }

        private void updateCache(byte[] id, byte[] typeId, byte[] data) {
            id = confirm16Bytes(id);
            if (id != null) {
                ByteBuffer bid = ByteBuffer.wrap(id);
                byte[][] cachedValue = binLogCache.getIfPresent(bid);
                if (cachedValue != null) {
                    // populate cache
                    byte[][] value = new byte[2][];
                    value[0] = typeId == null || typeId.length == 0 ? cachedValue[0] : confirm16Bytes(typeId);
                    value[1] = data;
                    binLogCache.put(bid, value);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("[BINLOG] UPDATING CACHE: ID [{}]", StringUtils.hex(id));
                    }
                }
            }
        }

        private void invalidateCache(byte[] id) {
            id = confirm16Bytes(id);
            if (id != null) {
                ByteBuffer bid = ByteBuffer.wrap(id);
                if (LOGGER.isInfoEnabled() && binLogCache.getIfPresent(bid) != null) {
                    LOGGER.info("[BINLOG] DELETING CACHE: ID [{}]", StringUtils.hex(id));
                }
                binLogCache.invalidate(bid);
            }
        }

        private void commitTransaction() {

            for (Event event : events) {
                EventHeader eventHeader = event.getHeader();
                EventType eventType = eventHeader.getEventType();
                EventData eventData = event.getData();
                LOGGER.debug("BIN LOG TEST [{}] [{}]", event.getHeader().getEventType().toString(), event.getData().toString());
                if (eventType == EventType.UPDATE_ROWS || eventType == EventType.EXT_UPDATE_ROWS) {
                    for (Map.Entry<Serializable[], Serializable[]> row : ((UpdateRowsEventData) eventData).getRows()) {
                        Serializable[] newValue = row.getValue();
                        updateCache((byte[]) newValue[0], (byte[]) newValue[1], (byte[]) newValue[2]);
                        LOGGER.debug("UpdateRow HEX [{}][{}]", StringUtils.hex((byte[]) newValue[0]), ((byte[]) newValue[0]).length);
                    }
                } else if (eventType == EventType.DELETE_ROWS || eventType == EventType.EXT_DELETE_ROWS) {
                    for (Serializable[] row : ((DeleteRowsEventData) eventData).getRows()) {
                        invalidateCache((byte[]) row[0]);
                        LOGGER.debug("DeleteRow HEX [{}][{}]", StringUtils.hex((byte[]) row[0]), ((byte[]) row[0]).length);
                    }
                } else if (eventType == EventType.QUERY) {
                    DariQueryEventData queryEventData = (DariQueryEventData) eventData;
                    if (queryEventData.getAction() == DariQueryEventData.Action.UPDATE) {
                        updateCache(queryEventData.getId(), queryEventData.getTypeId(), queryEventData.getData());
                    } else if (queryEventData.getAction() == DariQueryEventData.Action.DELETE) {
                        invalidateCache(queryEventData.getId());
                    }

                } else {
                    LOGGER.error("NOT RECOGNIZED TYPE: {}", eventType);
                }
            }
        }

        private void flushCache() {
            binLogCache.invalidateAll();
        }

        private byte[] getByteData(byte[] source, String strSource, int begin, int end) {
            byte[] target = null;
            if (strSource.startsWith("_binary")) {
                int targetIndex = 0;
                target = new byte[end - begin - 9];
                for (int sourceIndex = begin + 8; sourceIndex < end - 1; sourceIndex++) {
                    byte value = 0;
                    if (source[sourceIndex] == 92) { // '\'
                        switch (source[++sourceIndex]) {
                            case 34: // "
                                value = 34;
                                break;
                            case 39: // '
                                value = 39;
                                break;
                            case 48: // 0
                                value = 0;
                                break;
                            case 97: // a
                                value = 7;
                                break;
                            case 98: // b
                                value = 8;
                                break;
                            case 116: // t
                                value = 9;
                                break;
                            case 110: // n
                                value = 10;
                                break;
                            case 118: // v
                                value = 11;
                                break;
                            case 102: // f
                                value = 12;
                                break;
                            case 114: // r
                                value = 13;
                                break;
                            case 101: // e
                                value = 27;
                                break;
                            default:
                                value = source[--sourceIndex];
                                break;
                        }
                    } else {
                        value = source[sourceIndex];
                    }
                    target[targetIndex++] = value;
                }
            } else if (strSource.startsWith("X")) {
                target = StringUtils.hexToBytes(strSource.substring(2, strSource.length() - 1));
            } else {
                // TODO: error
            }
            return target;
        }

        private boolean processStatement(DariQueryEventData queryEventData) {

            boolean processed = false;
            if (queryEventData.getErrorCode() == 0 && queryEventData.getDatabase().equals(catalog)) {

                String sql = queryEventData.getSql();
                // TODO: parse sql statement to handle full syntax such as [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE] [INTO]
                String[] statementParts = sql.split("`?\\s+`?", 4);
                String table = null;
                if (statementParts[0].equalsIgnoreCase("UPDATE") ||
                        statementParts[0].equalsIgnoreCase("HANDLER")) {
                    table = statementParts[1];
                } else if (statementParts[0].equalsIgnoreCase("DELETE") ||
                        statementParts[0].equalsIgnoreCase("INSERT") ||
                        statementParts[0].equalsIgnoreCase("REPLACE")) {
                    table = statementParts[2];
                } else if ((statementParts[0].equalsIgnoreCase("ALTER") ||
                        statementParts[0].equalsIgnoreCase("CREATE") ||
                        statementParts[0].equalsIgnoreCase("RENAME") ||
                        statementParts[0].equalsIgnoreCase("TRUNCATE") ||
                        statementParts[0].equalsIgnoreCase("DROP")) &&
                        statementParts[1].equalsIgnoreCase("TABLE")) {
                    table = statementParts[2];
                }
                if (SqlDatabase.RECORD_TABLE.equalsIgnoreCase(table)) {
                    byte[] byteStatement = queryEventData.getStatement();
                    if (statementParts[0].equalsIgnoreCase("UPDATE")) {
                        queryEventData.setActionl(DariQueryEventData.Action.UPDATE);
                        Matcher matcher = updatePattern.matcher(sql);
                        if (matcher.matches()) {
                            queryEventData.setId(getByteData(byteStatement, matcher.group(4), matcher.start(4), matcher.end(4)));
                            queryEventData.setTypeId(getByteData(byteStatement, matcher.group(2), matcher.start(2), matcher.end(2)));
                            queryEventData.setData(getByteData(byteStatement, matcher.group(3), matcher.start(3), matcher.end(3)));
                            processed = true;
                            LOGGER.debug("[DEBUG] QUERY EVENT UPDATE [{}]", queryEventData);
                        } else {
                            isFlushCache = true;
                            LOGGER.info("Bin log cache flushed due to [{}]", sql);
                        }
                    } else if (statementParts[0].equalsIgnoreCase("DELETE")) {
                        queryEventData.setActionl(DariQueryEventData.Action.DELETE);
                        Matcher matcher = deletePattern.matcher(sql);
                        if (matcher.matches()) {
                            queryEventData.setId(getByteData(byteStatement, matcher.group(2), matcher.start(2), matcher.end(2)));
                            processed = true;
                            LOGGER.debug("[DEBUG] QUERY EVENT DELETE [{}]", queryEventData);
                        } else {
                            isFlushCache = true;
                            LOGGER.info("Bin log cache flushed due to [{}]", sql);
                        }
                    } else if (statementParts[0].equalsIgnoreCase("INSERT")) {
                        // Do nothing
                    } else {
                        isFlushCache = true;
                        LOGGER.info("Bin log cache flushed due to [{}]", sql);
                    }
                }
            }
            return processed;
        }

        @Override
        public void onEvent(Event event) {

            EventHeader eventHeader = event.getHeader();
            EventType eventType = eventHeader.getEventType();
            EventData eventData = event.getData();
            long tableId = 0;

            LOGGER.debug("TYPE: {}", eventType);

            if (transactionBegin) {
                if ((eventType == EventType.QUERY && ((DariQueryEventData) eventData).getSql().equalsIgnoreCase("COMMIT")) ||
                        (eventType == EventType.XID)) {
                    LOGGER.debug("[DEBUG] QUERY EVENT TRANSACTION COMMIT: [{}]", events.size());
                    try {
                        if (isFlushCache) {
                            flushCache();
                        } else {
                            commitTransaction();
                        }
                    } finally {
                        events.clear();
                        isFlushCache = false;
                        transactionBegin = false;
                    }
                } else {
                    if (tableMapEventData != null) {
                        // TODO: check column metadata to get length.
                        try {
                            if (EventType.isUpdate(eventType)) {
                                tableId = ((UpdateRowsEventData) eventData).getTableId();
                            } else if (EventType.isDelete(eventType)) {
                                tableId = ((DeleteRowsEventData) eventData).getTableId();
                            } else if (EventType.isWrite(eventType)) {
                                // Do nothing
                            } else {
                                LOGGER.error("NOT RECOGNIZED TYPE: {}", eventType);
                            }
                            if (tableMapEventData.getTableId() == tableId) {
                                events.add(event);
                            }
                        } finally {
                            tableMapEventData = null;
                        }
                    } else if (eventType == EventType.TABLE_MAP) {
                        if (((TableMapEventData) eventData).getDatabase().equals(catalog) && ((TableMapEventData) eventData).getTable().equalsIgnoreCase(SqlDatabase.RECORD_TABLE)) {
                            tableMapEventData = (TableMapEventData) eventData;
                        }
                    } else if (eventType == EventType.QUERY) {
                        if (processStatement((DariQueryEventData) eventData)) {
                            events.add(event);
                        }
                    }
                }
            } else if (eventType == EventType.QUERY && ((DariQueryEventData) eventData).getSql().equalsIgnoreCase("BEGIN")) {
                transactionBegin = true;
                LOGGER.debug("[DEBUG] QUERY EVENT TRANSACTION BEGIN");
            }
        }
    }

    /**
     * Whole class is basically a mix of <a href="https://code.google.com/p/open-replicator">open-replicator</a>'s
     * AbstractRowEventParser and MySQLUtils. Main purpose here is to ease rows deserialization.<p>
     *
     * Current {@link ColumnType} to java type mapping is following:
     * <pre>
     * Integer: {@link ColumnType#TINY}, {@link ColumnType#SHORT}, {@link ColumnType#LONG}, {@link ColumnType#INT24},
     * {@link ColumnType#YEAR}, {@link ColumnType#ENUM}, {@link ColumnType#SET},
     * Long: {@link ColumnType#LONGLONG},
     * Float: {@link ColumnType#FLOAT},
     * Double: {@link ColumnType#DOUBLE},
     * String: {@link ColumnType#VARCHAR}, {@link ColumnType#VAR_STRING}, {@link ColumnType#STRING},
     * java.util.BitSet: {@link ColumnType#BIT},
     * java.util.Date: {@link ColumnType#DATETIME}, {@link ColumnType#DATETIME_V2},
     * java.math.BigDecimal: {@link ColumnType#NEWDECIMAL},
     * java.sql.Timestamp: {@link ColumnType#TIMESTAMP}, {@link ColumnType#TIMESTAMP_V2},
     * java.sql.Date: {@link ColumnType#DATE},
     * java.sql.Time: {@link ColumnType#TIME}, {@link ColumnType#TIME_V2},
     * byte[]: {@link ColumnType#BLOB},
     * </pre>
     *
     * At the moment {@link ColumnType#GEOMETRY} is unsupported.
     *
     * @param <T> event data this deserializer is responsible for
     * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
     */
    public abstract static class AbstractDariRowsEventDataDeserializer<T extends EventData> implements EventDataDeserializer<T> {

        private static final int DIG_PER_DEC = 9;
        private static final int[] DIG_TO_BYTES = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

        private final Map<Long, TableMapEventData> tableMapEventByTableId;

        public AbstractDariRowsEventDataDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId) {
            this.tableMapEventByTableId = tableMapEventByTableId;
        }

        protected Serializable[] deserializeRow(long tableId, BitSet includedColumns, ByteArrayInputStream inputStream)
                throws IOException {
            TableMapEventData tableMapEvent = tableMapEventByTableId.get(tableId);
            byte[] types = tableMapEvent.getColumnTypes();
            int[] metadata = tableMapEvent.getColumnMetadata();
            BitSet nullColumns = inputStream.readBitSet(types.length, true);
            Serializable[] result = new Serializable[numberOfBitsSet(includedColumns)];
            for (int i = 0, numberOfSkippedColumns = 0; i < types.length; i++) {
                int typeCode = types[i] & 0xFF, meta = metadata[i], length = 0;
                if (typeCode == ColumnType.STRING.getCode() && meta > 256) {
                    int meta0 = meta >> 8, meta1 = meta & 0xFF;
                    if ((meta0 & 0x30) != 0x30) { // long CHAR field
                        typeCode = meta0 | 0x30;
                        length = meta1 | (((meta0 & 0x30) ^ 0x30) << 4);
                    } else {
                        if (meta0 == ColumnType.SET.getCode() || meta0 == ColumnType.ENUM.getCode() ||
                                meta0 == ColumnType.STRING.getCode()) {
                            typeCode = meta0;
                            length = meta1;
                        } else {
                            throw new IOException("Unexpected meta " + meta + " for column of type " + typeCode);
                        }
                    }
                }
                if (!includedColumns.get(i)) {
                    numberOfSkippedColumns++;
                    continue;
                }
                int index = i - numberOfSkippedColumns;
                if (!nullColumns.get(index)) {
                    result[index] = deserializeCell(ColumnType.byCode(typeCode), meta, length, inputStream);
                }
            }
            return result;
        }

        private Serializable deserializeCell(ColumnType type, int meta, int length, ByteArrayInputStream inputStream)
                throws IOException {
            switch (type) {
                case BIT:
                    int bitSetLength = (meta >> 8) * 8 + (meta & 0xFF);
                    return inputStream.readBitSet(bitSetLength, false);
                case TINY:
                    return (int) ((byte) inputStream.readInteger(1));
                case SHORT:
                    return (int) ((short) inputStream.readInteger(2));
                case INT24:
                    return (inputStream.readInteger(3) << 8) >> 8;
                case LONG:
                    return inputStream.readInteger(4);
                case LONGLONG:
                    return inputStream.readLong(8);
                case FLOAT:
                    return Float.intBitsToFloat(inputStream.readInteger(4));
                case DOUBLE:
                    return Double.longBitsToDouble(inputStream.readLong(8));
                case NEWDECIMAL:
                    int precision = meta & 0xFF, scale = meta >> 8,
                        decimalLength = determineDecimalLength(precision, scale);
                    return toDecimal(precision, scale, inputStream.read(decimalLength));
                case DATE:
                    return deserializeDate(inputStream);
                case TIME:
                    return deserializeTime(inputStream);
                case TIME_V2:
                    return deserializeTimeV2(meta, inputStream);
                case TIMESTAMP:
                    return deserializeTimestamp(inputStream);
                case TIMESTAMP_V2:
                    return deserializeTimestampV2(meta, inputStream);
                case DATETIME:
                    return deserializeDatetime(inputStream);
                case DATETIME_V2:
                    return deserializeDatetimeV2(meta, inputStream);
                case YEAR:
                    return 1900 + inputStream.readInteger(1);
                case STRING:
                    int stringLength = length < 256 ? inputStream.readInteger(1) : inputStream.readInteger(2);
                    // We overrides this to read data as binary format.
                    return inputStream.read(stringLength);
                case VARCHAR:
                case VAR_STRING:
                    int varcharLength = meta < 256 ? inputStream.readInteger(1) : inputStream.readInteger(2);
                    return inputStream.readString(varcharLength);
                case BLOB:
                    int blobLength = inputStream.readInteger(meta);
                    return inputStream.read(blobLength);
                case ENUM:
                    return inputStream.readInteger(length);
                case SET:
                    return inputStream.readLong(length);
                default:
                    throw new IOException("Unsupported type " + type);
            }
        }

        private java.sql.Date deserializeDate(ByteArrayInputStream inputStream) throws IOException {
            int value = inputStream.readInteger(3);
            int day = value % 32;
            value >>>= 5;
            int month = value % 16;
            int year = value >> 4;
            Calendar cal = Calendar.getInstance();
            cal.clear();
            cal.set(Calendar.YEAR, year);
            cal.set(Calendar.MONTH, month - 1);
            cal.set(Calendar.DATE, day);
            return new java.sql.Date(cal.getTimeInMillis());
        }

        private static java.sql.Time deserializeTime(ByteArrayInputStream inputStream) throws IOException {
            int value = inputStream.readInteger(3);
            int[] split = split(value, 100, 3);
            Calendar c = Calendar.getInstance();
            c.clear();
            c.set(Calendar.HOUR_OF_DAY, split[2]);
            c.set(Calendar.MINUTE, split[1]);
            c.set(Calendar.SECOND, split[0]);
            return new java.sql.Time(c.getTimeInMillis());
        }

        private java.sql.Time deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            /*
                in big endian:

                1 bit sign (1= non-negative, 0= negative)
                1 bit unused (reserved for future extensions)
                10 bits hour (0-838)
                6 bits minute (0-59)
                6 bits second (0-59)
                = (3 bytes in total)
                +
                fractional-seconds storage (size depends on meta)
            */
            long time = bigEndianLong(inputStream.read(3), 0, 3);
            Calendar c = Calendar.getInstance();
            c.clear();
            c.set(Calendar.HOUR_OF_DAY, extractBits(time, 2, 10, 24));
            c.set(Calendar.MINUTE, extractBits(time, 12, 6, 24));
            c.set(Calendar.SECOND, extractBits(time, 18, 6, 24));
            c.set(Calendar.MILLISECOND, getFractionalSeconds(meta, inputStream));
            return new java.sql.Time(c.getTimeInMillis());
        }

        private java.sql.Timestamp deserializeTimestamp(ByteArrayInputStream inputStream) throws IOException {
            long value = inputStream.readLong(4);
            return new java.sql.Timestamp(value * 1000L);
        }

        private java.sql.Timestamp deserializeTimestampV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            // big endian
            long timestamp = bigEndianLong(inputStream.read(4), 0, 4);
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(timestamp * 1000);
            c.set(Calendar.MILLISECOND, getFractionalSeconds(meta, inputStream));
            return new java.sql.Timestamp(c.getTimeInMillis());
        }

        private java.util.Date deserializeDatetime(ByteArrayInputStream inputStream) throws IOException {
            long value = inputStream.readLong(8);
            int[] split = split(value, 100, 6);
            Calendar c = Calendar.getInstance();
            c.set(Calendar.YEAR, split[5]);
            c.set(Calendar.MONTH, split[4] - 1);
            c.set(Calendar.DAY_OF_MONTH, split[3]);
            c.set(Calendar.HOUR_OF_DAY, split[2]);
            c.set(Calendar.MINUTE, split[1]);
            c.set(Calendar.SECOND, split[0]);
            c.set(Calendar.MILLISECOND, 0);
            return c.getTime();
        }

        private java.util.Date deserializeDatetimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            /*
                in big endian:

                1 bit sign (1= non-negative, 0= negative)
                17 bits year*13+month (year 0-9999, month 0-12)
                5 bits day (0-31)
                5 bits hour (0-23)
                6 bits minute (0-59)
                6 bits second (0-59)
                = (5 bytes in total)
                +
                fractional-seconds storage (size depends on meta)
            */
            long datetime = bigEndianLong(inputStream.read(5), 0, 5);
            int yearMonth = extractBits(datetime, 1, 17, 40);
            Calendar c = Calendar.getInstance();
            c.set(Calendar.YEAR, yearMonth / 13);
            c.set(Calendar.MONTH, yearMonth % 13 - 1);
            c.set(Calendar.DAY_OF_MONTH, extractBits(datetime, 18, 5, 40));
            c.set(Calendar.HOUR_OF_DAY, extractBits(datetime, 23, 5, 40));
            c.set(Calendar.MINUTE, extractBits(datetime, 28, 6, 40));
            c.set(Calendar.SECOND, extractBits(datetime, 34, 6, 40));
            c.set(Calendar.MILLISECOND, getFractionalSeconds(meta, inputStream));
            return c.getTime();
        }

        private int getFractionalSeconds(int meta, ByteArrayInputStream inputStream) throws IOException {
            int fractionalSecondsStorageSize = getFractionalSecondsStorageSize(meta);
            if (fractionalSecondsStorageSize > 0) {
                long fractionalSeconds = bigEndianLong(inputStream.read(meta), 0, meta);
                if (meta % 2 == 1) {
                    fractionalSeconds /= 10;
                }
                return (int) (fractionalSeconds / 1000);
            }
            return 0;
        }

        private static int getFractionalSecondsStorageSize(int fsp) {
            switch (fsp) {
                case 1:
                case 2:
                    return 1;
                case 3:
                case 4:
                    return 2;
                case 5:
                case 6:
                    return 3;
                default:
                    return 0;
            }
        }

        private static int extractBits(long value, int bitOffset, int numberOfBits, int payloadSize) {
            long result = value >> payloadSize - (bitOffset + numberOfBits);
            return (int) (result & ((1 << numberOfBits) - 1));
        }

        private static int numberOfBitsSet(BitSet bitSet) {
            int result = 0;
            for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                result++;
            }
            return result;
        }

        private static int[] split(long value, int divider, int length) {
            int[] result = new int[length];
            for (int i = 0; i < length - 1; i++) {
                result[i] = (int) (value % divider);
                value /= divider;
            }
            result[length - 1] = (int) value;
            return result;
        }

        private static int determineDecimalLength(int precision, int scale) {
            int x = precision - scale;
            int ipDigits = x / DIG_PER_DEC;
            int fpDigits = scale / DIG_PER_DEC;
            int ipDigitsX = x - ipDigits * DIG_PER_DEC;
            int fpDigitsX = scale - fpDigits * DIG_PER_DEC;
            return (ipDigits << 2) + DIG_TO_BYTES[ipDigitsX] + (fpDigits << 2) + DIG_TO_BYTES[fpDigitsX];
        }

        /**
         * see mysql/strings/decimal.c
         */
        private static BigDecimal toDecimal(int precision, int scale, byte[] value) {
            boolean positive = (value[0] & 0x80) == 0x80;
            value[0] ^= 0x80;
            if (!positive) {
                for (int i = 0; i < value.length; i++) {
                    value[i] ^= 0xFF;
                }
            }
            int x = precision - scale;
            int ipDigits = x / DIG_PER_DEC;
            int ipDigitsX = x - ipDigits * DIG_PER_DEC;
            int ipSize = (ipDigits << 2) + DIG_TO_BYTES[ipDigitsX];
            int offset = DIG_TO_BYTES[ipDigitsX];
            BigDecimal ip = offset > 0 ? BigDecimal.valueOf(bigEndianInteger(value, 0, offset)) : BigDecimal.ZERO;
            for (; offset < ipSize; offset += 4) {
                int i = bigEndianInteger(value, offset, 4);
                ip = ip.movePointRight(DIG_PER_DEC).add(BigDecimal.valueOf(i));
            }
            int shift = 0;
            BigDecimal fp = BigDecimal.ZERO;
            for (; shift + DIG_PER_DEC <= scale; shift += DIG_PER_DEC, offset += 4) {
                int i = bigEndianInteger(value, offset, 4);
                fp = fp.add(BigDecimal.valueOf(i).movePointLeft(shift + DIG_PER_DEC));
            }
            if (shift < scale) {
                int i = bigEndianInteger(value, offset, DIG_TO_BYTES[scale - shift]);
                fp = fp.add(BigDecimal.valueOf(i).movePointLeft(scale));
            }
            BigDecimal result = ip.add(fp);
            return positive ? result : result.negate();
        }

        private static int bigEndianInteger(byte[] bytes, int offset, int length) {
            int result = 0;
            for (int i = offset; i < (offset + length); i++) {
                byte b = bytes[i];
                result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
            }
            return result;
        }

        private static long bigEndianLong(byte[] bytes, int offset, int length) {
            long result = 0;
            for (int i = offset; i < (offset + length); i++) {
                byte b = bytes[i];
                result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
            }
            return result;
        }

    }

    public static class DariUpdateRowsEventDataDeserializer extends AbstractDariRowsEventDataDeserializer<UpdateRowsEventData> {

        private boolean mayContainExtraInformation;

        public DariUpdateRowsEventDataDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId) {
            super(tableMapEventByTableId);
        }

        public DariUpdateRowsEventDataDeserializer setMayContainExtraInformation(boolean mayContainExtraInformation) {
            this.mayContainExtraInformation = mayContainExtraInformation;
            return this;
        }

        @Override
        public UpdateRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
            UpdateRowsEventData eventData = new UpdateRowsEventData();
            eventData.setTableId(inputStream.readLong(6));
            inputStream.skip(2); // reserved
            if (mayContainExtraInformation) {
                int extraInfoLength = inputStream.readInteger(2);
                inputStream.skip(extraInfoLength - 2);
            }
            int numberOfColumns = inputStream.readPackedInteger();
            eventData.setIncludedColumnsBeforeUpdate(inputStream.readBitSet(numberOfColumns, true));
            eventData.setIncludedColumns(inputStream.readBitSet(numberOfColumns, true));
            eventData.setRows(deserializeRows(eventData, inputStream));
            return eventData;
        }

        private List<Map.Entry<Serializable[], Serializable[]>> deserializeRows(UpdateRowsEventData eventData,
                ByteArrayInputStream inputStream) throws IOException {
            long tableId = eventData.getTableId();
            BitSet includedColumnsBeforeUpdate = eventData.getIncludedColumnsBeforeUpdate(),
                   includedColumns = eventData.getIncludedColumns();
            List<Map.Entry<Serializable[], Serializable[]>> rows =
                    new ArrayList<Map.Entry<Serializable[], Serializable[]>>();
            while (inputStream.available() > 0) {
                rows.add(new AbstractMap.SimpleEntry<Serializable[], Serializable[]>(
                        deserializeRow(tableId, includedColumnsBeforeUpdate, inputStream),
                        deserializeRow(tableId, includedColumns, inputStream)
                ));
            }
            return rows;
        }

    }

    public static class DariDeleteRowsEventDataDeserializer extends AbstractDariRowsEventDataDeserializer<DeleteRowsEventData> {

        private boolean mayContainExtraInformation;

        public DariDeleteRowsEventDataDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId) {
            super(tableMapEventByTableId);
        }

        public DariDeleteRowsEventDataDeserializer setMayContainExtraInformation(boolean mayContainExtraInformation) {
            this.mayContainExtraInformation = mayContainExtraInformation;
            return this;
        }

        @Override
        public DeleteRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
            DeleteRowsEventData eventData = new DeleteRowsEventData();
            eventData.setTableId(inputStream.readLong(6));
            inputStream.readInteger(2); // reserved
            if (mayContainExtraInformation) {
                int extraInfoLength = inputStream.readInteger(2);
                inputStream.skip(extraInfoLength - 2);
            }
            int numberOfColumns = inputStream.readPackedInteger();
            eventData.setIncludedColumns(inputStream.readBitSet(numberOfColumns, true));
            eventData.setRows(deserializeRows(eventData.getTableId(), eventData.getIncludedColumns(), inputStream));
            return eventData;
        }

        private List<Serializable[]> deserializeRows(long tableId, BitSet includedColumns, ByteArrayInputStream inputStream)
                throws IOException {
            List<Serializable[]> result = new LinkedList<Serializable[]>();
            while (inputStream.available() > 0) {
                result.add(deserializeRow(tableId, includedColumns, inputStream));
            }
            return result;
        }

    }

    public static class DariWriteRowsEventDataDeserializer extends AbstractDariRowsEventDataDeserializer<WriteRowsEventData> {

        private boolean mayContainExtraInformation;

        public DariWriteRowsEventDataDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId) {
            super(tableMapEventByTableId);
        }

        public DariWriteRowsEventDataDeserializer setMayContainExtraInformation(boolean mayContainExtraInformation) {
            this.mayContainExtraInformation = mayContainExtraInformation;
            return this;
        }

        @Override
        public WriteRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
            WriteRowsEventData eventData = new WriteRowsEventData();
            eventData.setTableId(inputStream.readLong(6));
            inputStream.skip(2); // reserved
            if (mayContainExtraInformation) {
                int extraInfoLength = inputStream.readInteger(2);
                inputStream.skip(extraInfoLength - 2);
            }
            int numberOfColumns = inputStream.readPackedInteger();
            eventData.setIncludedColumns(inputStream.readBitSet(numberOfColumns, true));
            eventData.setRows(deserializeRows(eventData.getTableId(), eventData.getIncludedColumns(), inputStream));
            return eventData;
        }

        private List<Serializable[]> deserializeRows(long tableId, BitSet includedColumns, ByteArrayInputStream inputStream)
                throws IOException {
            List<Serializable[]> result = new LinkedList<Serializable[]>();
            while (inputStream.available() > 0) {
                result.add(deserializeRow(tableId, includedColumns, inputStream));
            }
            return result;
        }

    }

    /*
     * Override to keep the binary data
     */
    public static class DariQueryEventData extends QueryEventData {

        private byte[] statement;
        private Action action;
        private byte[] id;
        private byte[] typeId;
        private byte[] data;

        @Override
        public String getSql() {
            if (super.getSql() == null) {
                setSql(new String(statement, Charsets.US_ASCII));
            }
            return super.getSql();
        }

        public byte[] getStatement() {
            return statement;
        }

        public void setStatement(byte[] statement) {
            this.statement = statement;
        }

        public Action getAction() {
            return action;
        }

        public void setActionl(Action action) {
            this.action = action;
        }

        public byte[] getId() {
            return id;
        }

        public void setId(byte[] id) {
            this.id = id;
        }

        public byte[] getTypeId() {
            return typeId;
        }

        public void setTypeId(byte[] typeId) {
            this.typeId = typeId;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("DariQueryEventData");
            sb.append("{threadId=").append(getThreadId());
            sb.append(", executionTime=").append(getExecutionTime());
            sb.append(", errorCode=").append(getErrorCode());
            sb.append(", database='").append(getDatabase()).append('\'');
            sb.append(", sql='").append(getSql()).append('\'');
            sb.append(", statement='").append(getStatement()).append('\'');
            sb.append(", id='").append(StringUtils.hex(id)).append('\'');
            sb.append(", typeId='").append(StringUtils.hex(typeId)).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public enum Action {
            INSERT,
            UPDATE,
            DELETE;
        }
    }

    public static class DariQueryEventDataDeserializer implements EventDataDeserializer<DariQueryEventData> {

        @Override
        public DariQueryEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
            DariQueryEventData eventData = new DariQueryEventData();
            eventData.setThreadId(inputStream.readLong(4));
            eventData.setExecutionTime(inputStream.readLong(4));
            inputStream.skip(1); // length of the name of the database
            eventData.setErrorCode(inputStream.readInteger(2));
            inputStream.skip(inputStream.readInteger(2)); // status variables block
            eventData.setDatabase(inputStream.readZeroTerminatedString());
            eventData.setStatement(inputStream.read(inputStream.available()));
            return eventData;
        }
    }
}
