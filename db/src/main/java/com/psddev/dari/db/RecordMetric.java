package com.psddev.dari.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.psddev.dari.util.UuidUtils;

public class RecordMetric {

    //private static final Logger LOGGER = LoggerFactory.getLogger(RecordMetric.class);

    public static final String RECORDMETRIC_TABLE = "RecordMetric";
    public static final String RECORDRECORDMETRIC_TABLE = "RecordMetricRecord";
    public static final String RECORDMETRIC_METRICID_FIELD = "metricId";
    public static final String RECORDMETRIC_DATA_FIELD = "data";

    public static final int AMOUNT_DECIMAL_PLACES = 6;
    public static final long AMOUNT_DECIMAL_SHIFT = (long) Math.pow(10, AMOUNT_DECIMAL_PLACES);
    public static final long DATE_DECIMAL_SHIFT = 60000L;
    public static final int CUMULATIVEAMOUNT_POSITION = 1;
    public static final int AMOUNT_POSITION = 2;

    private static final String RECORDMETRIC_STRINGINDEX_TABLE = "RecordMetricString";
    private static final String RECORDMETRIC_NUMBERINDEX_TABLE = "RecordMetricNumber";
    private static final String RECORDMETRIC_UUIDINDEX_TABLE = "RecordMetricUuid";
    private static final String RECORDMETRIC_LOCATIONINDEX_TABLE = "RecordMetricLocation";

    private static final int QUERY_TIMEOUT = 3;
    private static final int DATE_BYTE_SIZE = 4;
    private static final int AMOUNT_BYTE_SIZE = 8;

    private final DimensionSet dimensions;
    private final String dimensionsSymbol;
    private final SqlDatabase db;
    private final MetricQuery query;
    private final Record record;

    private MetricEventDateProcessor eventDateProcessor;
    private UUID metricId;

    private Long updateDate;
    private Long eventDate;
    private Boolean dimensionsSaved;

    public RecordMetric(SqlDatabase database, Record record, String actionSymbol, Set<ObjectField> dimensions) {
        this.dimensions = DimensionSet.createDimensionSet(dimensions, record);
        this.dimensionsSymbol = this.getDimensionsSymbol(); // requires this.dimensions
        this.db = database;
        this.query = new MetricQuery(dimensionsSymbol, actionSymbol, record, this.dimensions);
        this.record = record;
    }

    public RecordMetric(Record record, String actionSymbol, Set<ObjectField> dimensions) {
        this(Database.Static.getFirst(SqlDatabase.class), record, actionSymbol, dimensions);
    }

    public void setEventDateProcessor(MetricEventDateProcessor processor) {
        this.eventDateProcessor = processor;
    }

    public Record getRecord() {
        return record;
    }

    public MetricEventDateProcessor getEventDateProcessor() {
        if (this.eventDateProcessor == null) {
            this.eventDateProcessor = new MetricEventDateProcessor.None();
        }
        return this.eventDateProcessor;
    }

    public SqlDatabase getDatabase() {
        return db;
    }

    public MetricQuery getQuery() {
        return query;
    }

    public void setUpdateDate(long timestampMillis) {
        this.updateDate = timestampMillis;
    }

    public long getUpdateDate() {
        if (this.updateDate == null) {
            this.updateDate = System.currentTimeMillis();
        }
        return this.updateDate;
    }

    // This method should strip the minutes and seconds off of a timestamp, or otherwise process it
    public void setEventDate(long timestampMillis) {
        this.eventDate = this.getEventDateProcessor().process(timestampMillis);
    }

    public long getEventDate() {
        if (this.eventDate == null) {
            setEventDate((System.currentTimeMillis()));
        }
        return this.eventDate;
    }

    public void setIncludeSelfDimension(boolean includeSelfDimension) {
        getQuery().setIncludeSelfDimension(includeSelfDimension);
    }

    public void setQueryDateRange(Long startTimestamp, Long endTimestamp) {
        getQuery().setDateRange(startTimestamp, endTimestamp);
    }

    public Double getMetric() throws SQLException {
        if (dimensionsSaved != null && dimensionsSaved == true) {
            // We already know we're dealing with exactly 1 metricId, so look it up directly.
            return Static.getMetricByMetricId(getDatabase(), getMetricId(), getQuery().getActionSymbol(), getQuery().getStartTimestamp(), getQuery().getEndTimestamp());
        } else {
            return Static.getMetricByDimensions(getDatabase(), getQuery());
        }
    }

    public Double getMetricByRecordId() throws SQLException {
        if (getRecordIdForInsert() == null) {
            throw new RuntimeException("Can't look up metric by recordId when using includeSelfDimension=false");
        }
        return Static.getMetricByRecordId(getDatabase(), getRecordIdForInsert(), this.record.getState().getTypeId(), getQuery().getActionSymbol(), getQuery().getStartTimestamp(), getQuery().getEndTimestamp());
    }

    public void incrementMetric(Double amount) throws SQLException {
        // find the metricId, it might be null
        if (amount == 0) return; // This actually causes some problems if it's not here
        UUID metricId = getMetricId();
        if (dimensionsSaved) {
            Static.doIncrementUpdateOrInsert(getDatabase(), metricId, this.getRecordIdForInsert(), this.record.getState().getTypeId(), getQuery().getActionSymbol(), getDimensionsSymbol(), amount, getUpdateDate(), getEventDate());
        } else {
            Static.doInserts(getDatabase(), metricId, this.getRecordIdForInsert(), this.record.getState().getTypeId(), getQuery().getActionSymbol(), getDimensionsSymbol(), dimensions, amount, getUpdateDate(), getEventDate());
            dimensionsSaved = true;
        }
    }

    public void setMetric(Double amount) throws SQLException {
        // This only works if we're not tracking eventDate
        if (! getEventDateProcessor().equals(MetricEventDateProcessor.None.class)) {
            throw new RuntimeException("RecordMetric.setMetric() can only be used if EventDateProcessor is None");
        }
        // find the metricId, it might be null
        UUID metricId = getMetricId();
        if (dimensionsSaved) {
            Static.doSetUpdateOrInsert(getDatabase(), metricId, this.getRecordIdForInsert(), this.record.getState().getTypeId(), getQuery().getActionSymbol(),
                    getDimensionsSymbol(), amount, getUpdateDate(),
                    getEventDate());
        } else {
            Static.doInserts(getDatabase(), metricId, this.getRecordIdForInsert(), this.record.getState().getTypeId(), getQuery().getActionSymbol(), getDimensionsSymbol(),
                    dimensions, amount, getUpdateDate(), getEventDate());
            dimensionsSaved = true;
        }
    }

    public void deleteMetrics() throws SQLException {
        Static.doMetricDelete(getDatabase(), this.getRecordIdForInsert(), this.record.getState().getTypeId(), getQuery().getDimensions());
    }

    private UUID getRecordIdForInsert() {
        return this.getQuery().getRecordIdForInsert();
    }

    public UUID getMetricId() throws SQLException {
        if (metricId == null) {
            metricId = Static.getMetricIdByDimensions(getDatabase(), getQuery());
            if (metricId == null) {
                // create a new metricId
                dimensionsSaved = false;
                metricId = UuidUtils.createSequentialUuid();
            } else {
                // this metricId came from the DB
                dimensionsSaved = true;
            }
        }
        return metricId;
    }

    public String getDimensionsSymbol() {
        if (this.dimensionsSymbol != null) {
            return dimensionsSymbol;
        } else {
            return this.dimensions.getSymbol();
        }
    }

    /** {@link RecordMetric} utility methods. */
    public static final class Static {

        private Static() {
        }

        public static Set<String> getIndexTables(DimensionSet... dimensionSets) {
            LinkedHashSet<String> tables = new LinkedHashSet<String>();
            for (DimensionSet dimensions : dimensionSets) {
                if (dimensions != null) {
                    for (Dimension dimension : dimensions) {
                        tables.add(dimension.getIndexTable());
                    }
                }
            }
            return tables;
        }

        public static String getIndexTable(ObjectField field) {
            String fieldType = field.getInternalItemType();
            if (fieldType.equals(ObjectField.UUID_TYPE)) {
                return RecordMetric.RECORDMETRIC_UUIDINDEX_TABLE;
            } else if (fieldType.equals(ObjectField.LOCATION_TYPE)) {
                return RecordMetric.RECORDMETRIC_LOCATIONINDEX_TABLE;
            } else if (fieldType.equals(ObjectField.NUMBER_TYPE) ||
                    fieldType.equals(ObjectField.DATE_TYPE)) {
                return RecordMetric.RECORDMETRIC_NUMBERINDEX_TABLE;
            } else {
                return RecordMetric.RECORDMETRIC_STRINGINDEX_TABLE;
            }
        }

        private static DimensionSet getDimensionsByIndexTable(String table, DimensionSet dimensions) {
            //HashMap<String, Object> dims = new HashMap<String, Object>();
            DimensionSet dims = new DimensionSet();
            for (Dimension dimension : dimensions) {
                if (table.equals(dimension.getIndexTable())) {
                    dims.add(dimension);
                }
            }
            return dims;
        }

        private static List<String> getInsertSqls(SqlDatabase db, List<List<Object>> parametersList, UUID metricId, UUID recordId, UUID typeId, String actionSymbol, String dimensionsSymbol, DimensionSet dimensions, double amount, long createDate, long eventDate) {
            ArrayList<String> sqls = new ArrayList<String>();
            // insert RecordMetric
            List<Object> parameters = new ArrayList<Object>();
            sqls.add(getRecordMetricInsertSql(db, parameters, metricId, recordId, typeId, actionSymbol, dimensionsSymbol, amount, amount, createDate, eventDate));
            parametersList.add(parameters);

            if (recordId != null) {
                parameters = new ArrayList<Object>();
                sqls.add(getRecordRecordMetricInsertSql(db, parameters, metricId, recordId, typeId));
                parametersList.add(parameters);
            }
            // insert indexes
            for (Dimension dimension : dimensions) {
                Set<Object> values = dimension.getValues();
                String table = dimension.getIndexTable();
                for (Object value : values) {
                    parameters = new ArrayList<Object>();
                    sqls.add(getDimensionInsertRowSql(db, parameters, metricId, recordId, dimensionsSymbol, dimension, value, table));
                    parametersList.add(parameters);
                }
            }
            return sqls;
        }

        // methods that generate SQL

        private static String getDataByMetricIdSql(SqlDatabase db, UUID metricId, String actionSymbol, Long minEventDate, Long maxEventDate, boolean selectMinData) {
            StringBuilder sqlBuilder = new StringBuilder();
            SqlVendor vendor = db.getVendor();

            sqlBuilder.append("SELECT ");

            sqlBuilder.append("MAX(");
            vendor.appendIdentifier(sqlBuilder, RECORDMETRIC_DATA_FIELD);
            sqlBuilder.append(") ");
            vendor.appendIdentifier(sqlBuilder, "maxData");

            if (selectMinData) {
                sqlBuilder.append(", MIN(");
                vendor.appendIdentifier(sqlBuilder, RECORDMETRIC_DATA_FIELD);
                sqlBuilder.append(") ");
                vendor.appendIdentifier(sqlBuilder, "minData");
            }

            sqlBuilder.append(" FROM ");
            vendor.appendIdentifier(sqlBuilder, RECORDMETRIC_TABLE);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, RECORDMETRIC_METRICID_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, metricId);

            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, "actionSymbolId");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, db.getSymbolId(actionSymbol));

            if (maxEventDate != null) {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, RECORDMETRIC_DATA_FIELD);
                sqlBuilder.append(" <= ");
                appendBinEncodeTimestampSql(sqlBuilder, null, vendor, maxEventDate, 'F');
            }

            if (minEventDate != null) {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, RECORDMETRIC_DATA_FIELD);
                sqlBuilder.append(" >= ");
                appendBinEncodeTimestampSql(sqlBuilder, null, vendor, minEventDate, '0');
            }

            return sqlBuilder.toString();
        }

        public static void appendSelectAmountSql(StringBuilder str, SqlVendor vendor, String columnIdentifier, int position) {
            // This does NOT shift the decimal place or round to 6 places. Do it yourself AFTER any other arithmetic.
            // position is 1 or 2
            // columnIdentifier is "`data`" or "MAX(`data`)" - already escaped
            str.append("CONV(");
                str.append("HEX(");
                    str.append("SUBSTR(");
                        str.append(columnIdentifier);
                        str.append(",");
                        vendor.appendValue(str, 1+DATE_BYTE_SIZE + ((position-1)*AMOUNT_BYTE_SIZE));
                        str.append(",");
                        vendor.appendValue(str, AMOUNT_BYTE_SIZE);
                    str.append(")");
                str.append(")");
            str.append(", 16, 10)");
        }

        private static String getRecordMetricInsertSql(SqlDatabase db, List<Object> parameters, UUID metricId, UUID recordId, UUID typeId, String actionSymbol, String dimensionsSymbol, double amount, double cumulativeAmount, long createDate, long eventDate) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            vendor.appendIdentifier(insertBuilder, RECORDMETRIC_TABLE);
            insertBuilder.append(" (");
            LinkedHashMap<String, Object> cols = new LinkedHashMap<String, Object>();
            cols.put(RECORDMETRIC_METRICID_FIELD, metricId);
            cols.put("actionSymbolId", db.getSymbolId(actionSymbol));
            cols.put("dimensionsSymbolId", db.getSymbolId(dimensionsSymbol));
            cols.put("createDate", createDate);
            cols.put("updateDate", createDate);
            cols.put(RECORDMETRIC_DATA_FIELD, toBytes(eventDate, cumulativeAmount, amount));
            for (Map.Entry<String, Object> entry : cols.entrySet()) {
                vendor.appendIdentifier(insertBuilder, entry.getKey());
                insertBuilder.append(", ");
            }
            insertBuilder.setLength(insertBuilder.length()-2);
            insertBuilder.append(") VALUES (");
            for (Map.Entry<String, Object> entry : cols.entrySet()) {
                vendor.appendBindValue(insertBuilder, entry.getValue(), parameters);
                insertBuilder.append(", ");
            }
            insertBuilder.setLength(insertBuilder.length()-2);
            insertBuilder.append(")");
            return insertBuilder.toString();
        }

        private static String getRecordRecordMetricInsertSql(SqlDatabase db, List<Object> parameters, UUID metricId, UUID recordId, UUID typeId) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            vendor.appendIdentifier(insertBuilder, RECORDRECORDMETRIC_TABLE);
            insertBuilder.append(" (");
            LinkedHashMap<String, Object> cols = new LinkedHashMap<String, Object>();
            cols.put("id", recordId);
            cols.put("typeId", typeId);
            cols.put("metricId", metricId);
            for (Map.Entry<String, Object> entry : cols.entrySet()) {
                vendor.appendIdentifier(insertBuilder, entry.getKey());
                insertBuilder.append(", ");
            }
            insertBuilder.setLength(insertBuilder.length()-2);
            insertBuilder.append(") VALUES (");
            for (Map.Entry<String, Object> entry : cols.entrySet()) {
                vendor.appendBindValue(insertBuilder, entry.getValue(), parameters);
                insertBuilder.append(", ");
            }
            insertBuilder.setLength(insertBuilder.length()-2);
            insertBuilder.append(")");
            return insertBuilder.toString();
        }

        private static byte[] toBytes(long eventDate, double cumulativeAmount, double amount) {

            Long cumulativeAmountLong = (long) (cumulativeAmount * AMOUNT_DECIMAL_SHIFT);
            Long amountLong = (long) (amount * AMOUNT_DECIMAL_SHIFT);
            Integer eventDateInt = (int) (eventDate / DATE_DECIMAL_SHIFT);

            int size, offset = 0;
            byte[] bytes = new byte[DATE_BYTE_SIZE+AMOUNT_BYTE_SIZE+AMOUNT_BYTE_SIZE];

            // first 4 bytes: timestamp
            size = DATE_BYTE_SIZE;
            for (int i = 0; i < size; ++i) {
                bytes[i+offset] = (byte) (eventDateInt >> (size - i - 1 << 3));
            }
            offset += size;

            // second 8 bytes: cumulativeAmount
            size = AMOUNT_BYTE_SIZE;
            for (int i = 0; i < size; ++i) {
                bytes[i+offset] = (byte) (cumulativeAmountLong >> (size - i - 1 << 3));
            }
            offset += size;

            // last 8 bytes: amount
            size = AMOUNT_BYTE_SIZE;
            for (int i = 0; i < 8; ++i) {
                bytes[i+offset] = (byte) (amountLong >> (size - i - 1 << 3));
            }

            return bytes;
        }

        private static long timestampFromBytes(byte[] bytes) {
            long timestamp = 0;

            for (int i = 0; i < DATE_BYTE_SIZE; ++i) {
                timestamp = (timestamp << 8) | (bytes[i] & 0xff);
            }

            return timestamp * DATE_DECIMAL_SHIFT;
        }

        private static double amountFromBytes(byte[] bytes, int position) {
            long amountLong = 0;

            int offset = DATE_BYTE_SIZE + ((position-1)*AMOUNT_BYTE_SIZE);

            for (int i = 0; i < AMOUNT_BYTE_SIZE; ++i) {
                amountLong = (amountLong << 8) | (bytes[i+offset] & 0xff);
            }

            return (double) amountLong / AMOUNT_DECIMAL_SHIFT;
        }

        private static String getDimensionInsertRowSql(SqlDatabase db, List<Object> parameters, UUID metricId, UUID recordId, String dimensionsSymbol, Dimension dimension, Object value, String table) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            vendor.appendIdentifier(insertBuilder, table);
            insertBuilder.append(" (");
            LinkedHashMap<String, Object> cols = new LinkedHashMap<String, Object>();
            cols.put(RECORDMETRIC_METRICID_FIELD, metricId);
            cols.put("dimensionsSymbolId", db.getSymbolId(dimensionsSymbol));
            cols.put("symbolId", db.getSymbolId(dimension.getSymbol()));
            cols.put("value", value);
            for (Map.Entry<String, Object> entry : cols.entrySet()) {
                vendor.appendIdentifier(insertBuilder, entry.getKey());
                insertBuilder.append(", ");
            }
            insertBuilder.setLength(insertBuilder.length()-2);
            insertBuilder.append(") VALUES (");
            for (Map.Entry<String, Object> entry : cols.entrySet()) {
                vendor.appendBindValue(insertBuilder, entry.getValue(), parameters);
                insertBuilder.append(", ");
            }
            insertBuilder.setLength(insertBuilder.length()-2);
            insertBuilder.append(")");
            return insertBuilder.toString();
        }

        private static String getUpdateSql(SqlDatabase db, List<Object> parameters, UUID metricId, String actionSymbol, double amount, long updateDate, long eventDate, boolean increment, boolean updateFuture) {
            StringBuilder updateBuilder = new StringBuilder("UPDATE ");
            SqlVendor vendor = db.getVendor();
            vendor.appendIdentifier(updateBuilder, RECORDMETRIC_TABLE);
            updateBuilder.append(" SET ");

            vendor.appendIdentifier(updateBuilder, RECORDMETRIC_DATA_FIELD);
            updateBuilder.append(" = ");
            updateBuilder.append(" UNHEX(");
                updateBuilder.append("CONCAT(");
                    // timestamp
                    appendHexEncodeExistingTimestampSql(updateBuilder, vendor, RECORDMETRIC_DATA_FIELD);
                    updateBuilder.append(',');
                    // cumulativeAmount and amount
                    if (increment) {
                        appendHexEncodeIncrementAmountSql(updateBuilder, parameters, vendor, RECORDMETRIC_DATA_FIELD, CUMULATIVEAMOUNT_POSITION, amount);
                        updateBuilder.append(',');
                        if (updateFuture) {
                            updateBuilder.append("IF (");
                                vendor.appendIdentifier(updateBuilder, RECORDMETRIC_DATA_FIELD);
                                updateBuilder.append(" LIKE ");
                                    updateBuilder.append(" CONCAT(");
                                        appendBinEncodeTimestampSql(updateBuilder, parameters, vendor, eventDate, null);
                                    updateBuilder.append(", '%')");
                                    updateBuilder.append(","); // if it's the exact date, then update the amount
                                    appendHexEncodeIncrementAmountSql(updateBuilder, parameters, vendor, RECORDMETRIC_DATA_FIELD, AMOUNT_POSITION, amount);
                                    updateBuilder.append(","); // if it's a date in the future, leave the date alone
                                    appendHexEncodeIncrementAmountSql(updateBuilder, parameters, vendor, RECORDMETRIC_DATA_FIELD, AMOUNT_POSITION, 0);
                            updateBuilder.append(")");
                        } else {
                            appendHexEncodeIncrementAmountSql(updateBuilder, parameters, vendor, RECORDMETRIC_DATA_FIELD, CUMULATIVEAMOUNT_POSITION, amount);
                        }
                    } else {
                        appendHexEncodeSetAmountSql(updateBuilder, parameters, vendor, amount);
                        updateBuilder.append(',');
                        appendHexEncodeSetAmountSql(updateBuilder, parameters, vendor, amount);
                    }
                updateBuilder.append(" )");
            updateBuilder.append(" )");

            updateBuilder.append(", ");
            vendor.appendIdentifier(updateBuilder, "updateDate");
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, updateDate, parameters);
            updateBuilder.append(" WHERE ");
            vendor.appendIdentifier(updateBuilder, RECORDMETRIC_METRICID_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, metricId, parameters);
            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, "actionSymbolId");
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, db.getSymbolId(actionSymbol), parameters);
            updateBuilder.append(" AND ");

            vendor.appendIdentifier(updateBuilder, RECORDMETRIC_DATA_FIELD);
            if (updateFuture) {
                // Note that this is a >= : we are updating the cumulativeAmount for every date AFTER this date, too, while leaving their amounts alone.
                updateBuilder.append(" >= ");
                appendBinEncodeTimestampSql(updateBuilder, parameters, vendor, eventDate, '0');
            } else {
                updateBuilder.append(" LIKE ");
                updateBuilder.append(" CONCAT(");
                appendBinEncodeTimestampSql(updateBuilder, parameters, vendor, eventDate, null);
                updateBuilder.append(", '%')");
            }

            return updateBuilder.toString();
        }

        public static void appendHexEncodeTimestampSql(StringBuilder str, List<Object> parameters, SqlVendor vendor, long timestamp, Character rpadChar) {
            if (rpadChar != null) {
                str.append("RPAD(");
            }
            str.append("LPAD(");
                str.append("HEX(");
                    if (parameters == null) {
                        vendor.appendValue(str, (int) (timestamp / DATE_DECIMAL_SHIFT));
                    } else {
                        vendor.appendBindValue(str, (int) (timestamp / DATE_DECIMAL_SHIFT), parameters);
                    }
                str.append(")");
            str.append(", "+(DATE_BYTE_SIZE*2)+", '0')");
            if (rpadChar != null) {
                str.append(",");
                vendor.appendValue(str, DATE_BYTE_SIZE*2+AMOUNT_BYTE_SIZE*2+AMOUNT_BYTE_SIZE*2);
                str.append(", '");
                str.append(rpadChar);
                str.append("')");
            }
        }

        public static void appendBinEncodeTimestampSql(StringBuilder str, List<Object> parameters, SqlVendor vendor, long timestamp, Character rpadHexChar) {
            str.append("UNHEX(");
            appendHexEncodeTimestampSql(str, parameters, vendor, timestamp, rpadHexChar);
            str.append(")");
        }

        private static void appendHexEncodeExistingTimestampSql(StringBuilder str, SqlVendor vendor, String columnIdentifier) {
            // columnName is "data" or "max(`data`)"
            str.append("HEX(");
                str.append("SUBSTR(");
                    str.append(columnIdentifier);
                    str.append(",");
                    vendor.appendValue(str, 1);
                    str.append(",");
                    vendor.appendValue(str, DATE_BYTE_SIZE);
                str.append(")");
            str.append(")");
        }

        private static void appendHexEncodeSetAmountSql(StringBuilder str, List<Object> parameters, SqlVendor vendor, double amount) {
            str.append("LPAD(");
                str.append("HEX(");
                    vendor.appendBindValue(str, (int) (amount * AMOUNT_DECIMAL_SHIFT), parameters);
                str.append(" )");
            str.append(", "+(AMOUNT_BYTE_SIZE*2)+", '0')");
        }

        private static void appendHexEncodeIncrementAmountSql(StringBuilder str, List<Object> parameters, SqlVendor vendor, String columnName, int position, double amount) {
            // position is 1 or 2
            // columnName is "data" unless it is aliased
            str.append("LPAD(");
                str.append("HEX(");
                    // conv(hex(substr(data, 1+4, 8)), 16, 10)
                    appendSelectAmountSql(str, vendor, columnName, position);
                    str.append("+");
                    vendor.appendBindValue(str, (long)(amount * AMOUNT_DECIMAL_SHIFT), parameters);
                str.append(" )");
            str.append(", "+(AMOUNT_BYTE_SIZE*2)+", '0')");
        }

        private static String getDeleteDimensionSql(SqlDatabase db, UUID recordId, UUID typeId, String table) {
            SqlVendor vendor = db.getVendor();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ");
            vendor.appendIdentifier(sqlBuilder, table);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, RECORDMETRIC_METRICID_FIELD);
            sqlBuilder.append(" IN (");
            sqlBuilder.append(" SELECT ");
            vendor.appendIdentifier(sqlBuilder, RECORDMETRIC_METRICID_FIELD);
            sqlBuilder.append(" FROM ");
            vendor.appendIdentifier(sqlBuilder, RECORDRECORDMETRIC_TABLE);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, "typeId");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, typeId);
            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, "id");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, recordId);
            sqlBuilder.append(") ");
            return sqlBuilder.toString();
        }

        private static String getDeleteRecordMetricSql(SqlDatabase db, UUID recordId) {
            SqlVendor vendor = db.getVendor();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ");
            vendor.appendIdentifier(sqlBuilder, RECORDMETRIC_TABLE);
            vendor.appendIdentifier(sqlBuilder, RECORDMETRIC_METRICID_FIELD);
            sqlBuilder.append(" IN (");
            sqlBuilder.append(" SELECT ");
            vendor.appendIdentifier(sqlBuilder, RECORDMETRIC_METRICID_FIELD);
            sqlBuilder.append(" FROM ");
            vendor.appendIdentifier(sqlBuilder, RECORDRECORDMETRIC_TABLE);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, "id");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, recordId);
            sqlBuilder.append(") ");
            /*
            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, "actionSymbolId");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, actionSymbolId);
            */
            return sqlBuilder.toString();
        }

        private static String getDeleteRecordMetricRecordSql(SqlDatabase db, UUID recordId, UUID typeId) {
            SqlVendor vendor = db.getVendor();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ");
            vendor.appendIdentifier(sqlBuilder, RECORDRECORDMETRIC_TABLE);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, "id");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, recordId);
            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, "typeId");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, typeId);
            return sqlBuilder.toString();
        }

        private static String getSelectMetricByRecordIdSql(SqlDatabase db, String actionSymbol, UUID recordId, UUID typeId, Long startTimestamp, Long endTimestamp) {
            SqlVendor vendor = db.getVendor();
            StringBuilder selectBuilder = new StringBuilder();
            StringBuilder fromBuilder = new StringBuilder();
            StringBuilder whereBuilder = new StringBuilder();
            StringBuilder groupByBuilder = new StringBuilder();

            boolean selectMinData = false;
            if (startTimestamp != null) {
                selectMinData = true;
            }

            selectBuilder.append("SELECT ");

            selectBuilder.append("MAX( ");
            vendor.appendIdentifier(selectBuilder, "data");
            selectBuilder.append(") ");
            vendor.appendIdentifier(selectBuilder, "maxData");
            if (selectMinData) {
                selectBuilder.append(", MIN( ");
                vendor.appendIdentifier(selectBuilder, "data");
                selectBuilder.append(") ");
                vendor.appendIdentifier(selectBuilder, "minData");
            }

            fromBuilder.append(" FROM ");
            vendor.appendIdentifier(fromBuilder, RECORDRECORDMETRIC_TABLE);
            fromBuilder.append(" ");
            vendor.appendIdentifier(fromBuilder, "rcr");

            fromBuilder.append(" JOIN ");
            vendor.appendIdentifier(fromBuilder, RECORDMETRIC_TABLE);
            fromBuilder.append(" ");
            vendor.appendIdentifier(fromBuilder, "cr");
            fromBuilder.append(" ON (");
            vendor.appendIdentifier(fromBuilder, "rcr");
            fromBuilder.append(".");
            vendor.appendIdentifier(fromBuilder, "metricId");
            fromBuilder.append(" = ");
            vendor.appendIdentifier(fromBuilder, "cr");
            fromBuilder.append(".");
            vendor.appendIdentifier(fromBuilder, "metricId");
            fromBuilder.append(") ");

            whereBuilder.append(" WHERE ");
            vendor.appendIdentifier(whereBuilder, "rcr");
            whereBuilder.append(".");
            vendor.appendIdentifier(whereBuilder, "typeId");
            whereBuilder.append(" = ");
            vendor.appendValue(whereBuilder, typeId);
            whereBuilder.append(" AND ");
            vendor.appendIdentifier(whereBuilder, "rcr");
            whereBuilder.append(".");
            vendor.appendIdentifier(whereBuilder, "id");
            whereBuilder.append(" = ");
            vendor.appendValue(whereBuilder, recordId);
            whereBuilder.append(" AND ");
            vendor.appendIdentifier(whereBuilder, "cr");
            whereBuilder.append(".");
            vendor.appendIdentifier(whereBuilder, "actionSymbolId");
            whereBuilder.append(" = ");
            vendor.appendValue(whereBuilder, db.getSymbolId(actionSymbol));

            groupByBuilder.append(" GROUP BY ");
            vendor.appendIdentifier(groupByBuilder, "cr");
            groupByBuilder.append(".");
            vendor.appendIdentifier(groupByBuilder, "metricId");

            // Now wrap it up for the sum()

            String innerSql = selectBuilder.toString() +
                " " + fromBuilder.toString() +
                " " + whereBuilder.toString() +
                " " + groupByBuilder.toString();

            selectBuilder = new StringBuilder();
            fromBuilder = new StringBuilder();
            whereBuilder = new StringBuilder();
            groupByBuilder = new StringBuilder();

            selectBuilder.append("SELECT ");

            selectBuilder.append("ROUND(SUM(");

            // Always select maxData.cumulativeAmount
            appendSelectAmountSql(selectBuilder, vendor, "maxData", CUMULATIVEAMOUNT_POSITION);

            if (selectMinData) {
                selectBuilder.append(" - ");
                appendSelectAmountSql(selectBuilder, vendor, "minData", CUMULATIVEAMOUNT_POSITION);
                selectBuilder.append(" + ");
                appendSelectAmountSql(selectBuilder, vendor, "minData", AMOUNT_POSITION);
            }

            selectBuilder.append(") / ");
            vendor.appendValue(selectBuilder, AMOUNT_DECIMAL_SHIFT);
            selectBuilder.append(",");
            vendor.appendValue(selectBuilder, AMOUNT_DECIMAL_PLACES);
            selectBuilder.append(")");

            fromBuilder.append(" FROM (");
            fromBuilder.append(innerSql);
            fromBuilder.append(") ");
            vendor.appendIdentifier(fromBuilder, "x");

            return selectBuilder.toString() +
                " " + fromBuilder.toString() +
                " " + whereBuilder.toString() +
                " " + groupByBuilder.toString();
        }

        private static String getMetricIdsByDimensionsSelectSql(SqlDatabase db, MetricQuery query, boolean preciseMatch) {
            SqlVendor vendor = db.getVendor();
            StringBuilder selectBuilder = new StringBuilder();
            StringBuilder fromBuilder = new StringBuilder();
            StringBuilder whereBuilder = new StringBuilder();
            StringBuilder groupByBuilder = new StringBuilder();
            StringBuilder orderByBuilder = new StringBuilder();

            int i = 0;
            int count = 1;
            String alias;
            selectBuilder.append("SELECT ");
            vendor.appendIdentifier(selectBuilder, "cr0");
            selectBuilder.append(".");
            vendor.appendIdentifier(selectBuilder, RECORDMETRIC_METRICID_FIELD);

            for (String table : Static.getIndexTables(query.getDimensions(),
                    query.getGroupByDimensions())) {
                alias = "cr" + i;
                if (i == 0) {
                    fromBuilder.append(" \nFROM ");
                    vendor.appendIdentifier(fromBuilder, table);
                    fromBuilder.append(" ");
                    vendor.appendIdentifier(fromBuilder, alias);
                    whereBuilder.append(" \nWHERE ");
                    if (preciseMatch) {
                        vendor.appendIdentifier(whereBuilder, alias);
                        whereBuilder.append(".");
                        vendor.appendIdentifier(whereBuilder, "dimensionsSymbolId");
                        whereBuilder.append(" = ");
                        whereBuilder.append(db.getSymbolId(query.getSymbol()));
                    } else {
                        whereBuilder.append("1 = 1");
                    }
                    if (query.getRecordIdForInsert() != null) {
                        fromBuilder.append(" JOIN ");
                        vendor.appendIdentifier(fromBuilder, RECORDRECORDMETRIC_TABLE);
                        fromBuilder.append(" ON (");
                        vendor.appendIdentifier(fromBuilder, alias);
                        fromBuilder.append(".");
                        vendor.appendIdentifier(fromBuilder, "metricId");
                        fromBuilder.append(" = ");
                        vendor.appendIdentifier(fromBuilder, RECORDRECORDMETRIC_TABLE);
                        fromBuilder.append(".");
                        vendor.appendIdentifier(fromBuilder, "metricId");
                        fromBuilder.append(") ");

                        whereBuilder.append(" AND ");
                        vendor.appendIdentifier(whereBuilder, RECORDRECORDMETRIC_TABLE);
                        whereBuilder.append(".");
                        vendor.appendIdentifier(whereBuilder, "id");
                        whereBuilder.append(" = ");
                        vendor.appendValue(whereBuilder, query.getRecordIdForInsert());

                        whereBuilder.append(" AND ");
                        vendor.appendIdentifier(whereBuilder, RECORDRECORDMETRIC_TABLE);
                        whereBuilder.append(".");
                        vendor.appendIdentifier(whereBuilder, "typeId");
                        whereBuilder.append(" = ");
                        vendor.appendValue(whereBuilder, query.getRecord().getState().getTypeId());
                    }
                } else {
                    fromBuilder.append(" \nJOIN ");
                    vendor.appendIdentifier(fromBuilder, table);
                    fromBuilder.append(" ");
                    vendor.appendIdentifier(fromBuilder, alias);
                    fromBuilder.append(" ON (");
                    vendor.appendIdentifier(fromBuilder, "cr0");
                    fromBuilder.append(".");
                    vendor.appendIdentifier(fromBuilder, "dimensionsSymbolId");
                    fromBuilder.append(" = ");
                    vendor.appendIdentifier(fromBuilder, alias);
                    fromBuilder.append(".");
                    vendor.appendIdentifier(fromBuilder, "dimensionsSymbolId");
                    fromBuilder.append(" AND ");
                    vendor.appendIdentifier(fromBuilder, "cr0");
                    fromBuilder.append(".");
                    vendor.appendIdentifier(fromBuilder, RECORDMETRIC_METRICID_FIELD);
                    fromBuilder.append(" = ");
                    vendor.appendIdentifier(fromBuilder, alias);
                    fromBuilder.append(".");
                    vendor.appendIdentifier(fromBuilder, RECORDMETRIC_METRICID_FIELD);
                    fromBuilder.append(")");
                }

                int numFilters = 0;
                // append to where statement
                whereBuilder.append(" \nAND (");
                for (Dimension dimension : Static.getDimensionsByIndexTable(
                        table, query.getDimensions())) {
                    Set<Object> values = dimension.getValues();
                    whereBuilder.append("(");
                    vendor.appendIdentifier(whereBuilder, alias);
                    whereBuilder.append(".");
                    vendor.appendIdentifier(whereBuilder, "symbolId");
                    whereBuilder.append(" = ");
                    whereBuilder.append(db.getSymbolId(dimension.getSymbol()));
                    whereBuilder.append(" AND ");
                    vendor.appendIdentifier(whereBuilder, alias);
                    whereBuilder.append(".");
                    vendor.appendIdentifier(whereBuilder, "value");
                    whereBuilder.append(" IN (");
                    for (Object v : values) {
                        vendor.appendValue(whereBuilder, v);
                        whereBuilder.append(',');
                        ++numFilters;
                    }
                    whereBuilder.setLength(whereBuilder.length() - 1);
                    whereBuilder.append("))");
                    whereBuilder.append(" \n  OR "); // 7 chars below
                }
                whereBuilder.setLength(whereBuilder.length() - 7);
                whereBuilder.append(") ");
                count = count * numFilters;
                ++i;
            }
            groupByBuilder.append("\nGROUP BY ");
            vendor.appendIdentifier(groupByBuilder, "cr0");
            groupByBuilder.append(".");
            vendor.appendIdentifier(groupByBuilder, RECORDMETRIC_METRICID_FIELD);
            //orderByBuilder.append("\nORDER BY ");
            //vendor.appendIdentifier(orderByBuilder, "cr0");
            //orderByBuilder.append(".");
            //vendor.appendIdentifier(orderByBuilder, RECORDMETRIC_METRICID_FIELD);
            groupByBuilder.append(" HAVING COUNT(*) = ");
            groupByBuilder.append(count);

            return selectBuilder.toString() +
                " " + fromBuilder.toString() +
                " " + whereBuilder.toString() +
                " " + groupByBuilder.toString() +
                " " + orderByBuilder.toString();
        }

        private static String getSelectMetricSql(SqlDatabase db, MetricQuery query) {
            SqlVendor vendor = db.getVendor();
            boolean selectMinData = false;

            StringBuilder selectBuilder = new StringBuilder();
            StringBuilder fromBuilder = new StringBuilder();

            selectBuilder.append("SELECT ");
            selectBuilder.append("ROUND(SUM(");

            // Always select maxData.cumulativeAmount
            appendSelectAmountSql(selectBuilder, vendor, "maxData", CUMULATIVEAMOUNT_POSITION);

            if (query.getStartTimestamp() != null) {
                // maxData.cumulativeAmount - minData.cumulativeAmount + minData.amount
                selectMinData = true;
                selectBuilder.append(" - ");
                appendSelectAmountSql(selectBuilder, vendor, "minData", CUMULATIVEAMOUNT_POSITION);
                selectBuilder.append(" + ");
                appendSelectAmountSql(selectBuilder, vendor, "minData", AMOUNT_POSITION);
            }

            selectBuilder.append(") / ");
            vendor.appendValue(selectBuilder, AMOUNT_DECIMAL_SHIFT);
            selectBuilder.append(",");
            vendor.appendValue(selectBuilder, AMOUNT_DECIMAL_PLACES);
            selectBuilder.append(")");
            fromBuilder.append(" FROM (");
            fromBuilder.append(getSelectMetricIdDataSql(db, query, selectMinData));
            fromBuilder.append(") x2");

            return selectBuilder.toString() + fromBuilder.toString();
        }

        private static String getSelectMetricIdDataSql(SqlDatabase db, MetricQuery query, boolean selectMinData) {
            StringBuilder selectBuilder = new StringBuilder();
            StringBuilder fromBuilder = new StringBuilder();
            StringBuilder whereBuilder = new StringBuilder();
            SqlVendor vendor = db.getVendor();

            selectBuilder.append("SELECT ");

            selectBuilder.append("MAX(");
            vendor.appendIdentifier(selectBuilder, "cr");
            selectBuilder.append(".");
            vendor.appendIdentifier(selectBuilder, RECORDMETRIC_DATA_FIELD);
            selectBuilder.append(") ");
            vendor.appendIdentifier(selectBuilder, "maxData");

            if (selectMinData) {
                selectBuilder.append(", ");
                selectBuilder.append("MIN(");
                vendor.appendIdentifier(selectBuilder, "cr");
                selectBuilder.append(".");
                vendor.appendIdentifier(selectBuilder, RECORDMETRIC_DATA_FIELD);
                selectBuilder.append(") ");
                vendor.appendIdentifier(selectBuilder, "minData");
            }

            fromBuilder.append(" \nFROM ");
            vendor.appendIdentifier(fromBuilder, RECORDMETRIC_TABLE);
            fromBuilder.append(" ");
            vendor.appendIdentifier(fromBuilder, "cr");
            whereBuilder.append(" \nWHERE ");

            vendor.appendIdentifier(whereBuilder, "cr");
            whereBuilder.append(".");
            vendor.appendIdentifier(whereBuilder, "actionSymbolId");
            whereBuilder.append(" = ");
            vendor.appendValue(whereBuilder, db.getSymbolId(query.getActionSymbol()));

            // for dates
            StringBuilder dateDataBuilder = new StringBuilder();
            vendor.appendIdentifier(dateDataBuilder, "cr");
            dateDataBuilder.append(".");
            vendor.appendIdentifier(dateDataBuilder, RECORDMETRIC_DATA_FIELD);

            if (query.getStartTimestamp() != null) {
                whereBuilder.append(" AND ");
                whereBuilder.append(dateDataBuilder);
                whereBuilder.append(" > ");
                appendBinEncodeTimestampSql(whereBuilder, null, vendor, query.getStartTimestamp(), 'F');
            }

            if (query.getEndTimestamp() != null) {
                whereBuilder.append(" AND ");
                whereBuilder.append(dateDataBuilder);
                whereBuilder.append(" <= ");
                appendBinEncodeTimestampSql(whereBuilder, null, vendor, query.getEndTimestamp(), 'F');
            }

            fromBuilder.append(" JOIN (");
            fromBuilder.append(getMetricIdsByDimensionsSelectSql(db, query, false));
            fromBuilder.append(") x");
            fromBuilder.append(" ON (");
            vendor.appendIdentifier(fromBuilder, "x");
            fromBuilder.append(".");
            vendor.appendIdentifier(fromBuilder, "metricId");
            fromBuilder.append(" = ");
            vendor.appendIdentifier(fromBuilder, "cr");
            fromBuilder.append(".");
            vendor.appendIdentifier(fromBuilder, "metricId");
            fromBuilder.append(")");

            return selectBuilder.toString() + fromBuilder.toString() + whereBuilder.toString();
        }

        private static String getSelectMetricIdSql(SqlDatabase db, MetricQuery query) {
            return getMetricIdsByDimensionsSelectSql(db, query, true);
        }

        // methods that actually touch the database

        // RECORDMETRIC INSERT/UPDATE/DELETE
        private static void doInserts(SqlDatabase db, UUID metricId, UUID recordId, UUID typeId, String actionSymbol, String dimensionsSymbol, DimensionSet dimensions, double amount, long updateDate, long eventDate) throws SQLException {
            Connection connection = db.openConnection();
            try {
                List<List<Object>> parametersList = new ArrayList<List<Object>>();
                List<String> sqls = getInsertSqls(db, parametersList, metricId, recordId, typeId, actionSymbol, dimensionsSymbol, dimensions, amount, updateDate, eventDate);
                Iterator<List<Object>> parametersIterator = parametersList.iterator();
                for (String sql : sqls) {
                    SqlDatabase.Static.executeUpdateWithList(connection, sql, parametersIterator.next());
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        private static void doIncrementUpdateOrInsert(SqlDatabase db, UUID metricId, UUID recordId, UUID typeId, String actionSymbol, String dimensionsSymbol, double incrementAmount, long updateDate, long eventDate) throws SQLException {
            Connection connection = db.openConnection();
            try {
                List<Object> parameters = new ArrayList<Object>();

                // First, find the max eventDate. Under normal circumstances, this will either be null (INSERT), before our eventDate (INSERT) or equal to our eventDate (UPDATE).
                byte[] data = getDataByMetricId(db, metricId, actionSymbol, null, null);
                String sql;

                if (data == null || timestampFromBytes(data) < eventDate) {
                    // No data for this eventDate; insert.
                    double previousCumulativeAmount = amountFromBytes(data, CUMULATIVEAMOUNT_POSITION);
                    parameters = new ArrayList<Object>();
                    sql = getRecordMetricInsertSql(db, parameters, metricId, recordId, typeId, actionSymbol, dimensionsSymbol, incrementAmount, previousCumulativeAmount+incrementAmount, updateDate, eventDate);
                } else if (timestampFromBytes(data) == eventDate) {
                    // There is data for this eventDate; update it.
                    sql = getUpdateSql(db, parameters, metricId, actionSymbol, incrementAmount, updateDate, eventDate, true, false);
                } else { // if (timestampFromBytes(data) > eventDate)
                    // We are updating a row in the past, so we need to tell updateSql to update the cumulativeAmount for all rows in the future.
                    sql = getUpdateSql(db, parameters, metricId, actionSymbol, incrementAmount, updateDate, eventDate, true, true);
                }
                SqlDatabase.Static.executeUpdateWithList( connection, sql, parameters);

            } finally {
                db.closeConnection(connection);
            }
        }

        private static void doSetUpdateOrInsert(SqlDatabase db, UUID metricId, UUID recordId, UUID typeId, String actionSymbol, String dimensionsSymbol, double amount, long updateDate, long eventDate) throws SQLException {
            Connection connection = db.openConnection();
            if (eventDate != 0L) {
                throw new RuntimeException("RecordMetric.Static.doSetUpdateOrInsert() can only be used if EventDatePrecision is NONE; eventDate is " + eventDate + ", should be 0L.");
            }
            try {
                List<Object> parameters = new ArrayList<Object>();
                String sql = getUpdateSql(db, parameters, metricId, actionSymbol, amount, updateDate, eventDate, false, false);
                int rowsAffected = SqlDatabase.Static.executeUpdateWithList( connection, sql, parameters);
                if (rowsAffected == 0) {
                    parameters = new ArrayList<Object>();
                    sql = getRecordMetricInsertSql(db, parameters, metricId, recordId, typeId, actionSymbol, dimensionsSymbol, amount, amount, updateDate, eventDate);
                    SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doMetricDelete(SqlDatabase db, UUID recordId, UUID typeId, DimensionSet dimensions) throws SQLException {
            if (recordId == null) return; // TODO need to handle this.
            Connection connection = db.openConnection();
            List<Object> parameters = new ArrayList<Object>();
            try {
                Set<String> tables = new HashSet<String>();
                for (Dimension dimension : dimensions) {
                    tables.add(dimension.getIndexTable());
                }
                // This needs to be executed BEFORE DeleteRecordMetricSql
                for (String table : tables) {
                    String sql = getDeleteDimensionSql(db, recordId, typeId, table);
                    SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                }
                String sql = getDeleteRecordMetricSql(db, recordId);
                SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                if (recordId != null) {
                    sql = getDeleteRecordMetricRecordSql(db, typeId, recordId);
                    SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        // RECORDMETRIC SELECT
        public static Double getMetricByDimensions(SqlDatabase db, MetricQuery query) throws SQLException {
            String sql = null;
            sql = getSelectMetricSql(db, query);
            Connection connection = db.openReadConnection();
            Double metric = 0.0;
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                if (result.next()) {
                    metric = result.getDouble(1);
                }
                result.close();
                statement.close();
                return metric;
            } finally {
                db.closeConnection(connection);
            }
        }

        private static Double getMetricByMetricId(SqlDatabase db, UUID metricId, String actionSymbol, Long minEventDate, Long maxEventDate) throws SQLException {
            if (minEventDate == null) {
                byte[] data = getDataByMetricId(db, metricId, actionSymbol, minEventDate, maxEventDate);
                if (data == null) return null;
                return amountFromBytes(data, CUMULATIVEAMOUNT_POSITION);
            } else {
                List<byte[]> datas = getMinMaxDataByMetricId(db, metricId, actionSymbol, minEventDate, maxEventDate);
                if (datas.size() == 0) return null;
                if (datas.get(0) == null) return null;
                double maxCumulativeAmount = amountFromBytes(datas.get(0), CUMULATIVEAMOUNT_POSITION);
                double minCumulativeAmount = amountFromBytes(datas.get(1), CUMULATIVEAMOUNT_POSITION);
                double minAmount = amountFromBytes(datas.get(1), AMOUNT_POSITION);
                return maxCumulativeAmount - minCumulativeAmount + minAmount;
            }
        }

        public static Double getMetricByRecordId(SqlDatabase db, UUID recordId, UUID typeId, String actionSymbol, Long minEventDate, Long maxEventDate) throws SQLException {
            String sql = null;
            sql = getSelectMetricByRecordIdSql(db, actionSymbol, recordId, typeId, minEventDate, maxEventDate);
            Connection connection = db.openReadConnection();
            Double metric = 0.0;
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                if (result.next()) {
                    metric = result.getDouble(1);
                }
                result.close();
                statement.close();
                return metric;
            } finally {
                db.closeConnection(connection);
            }
        }

        private static Long getMaxEventDateByMetricId(SqlDatabase db, UUID metricId, String actionSymbol, Long minEventDate, Long maxEventDate) throws SQLException {
            byte[] data = getDataByMetricId(db, metricId, actionSymbol, minEventDate, maxEventDate);
            if (data == null) return null;
            return timestampFromBytes(data);
        }

        private static byte[] getDataByMetricId(SqlDatabase db, UUID metricId, String actionSymbol, Long minEventDate, Long maxEventDate) throws SQLException {
            String sql = Static.getDataByMetricIdSql(db, metricId, actionSymbol, minEventDate, maxEventDate, false);
            byte[] data = null;
            Connection connection = db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                if (result.next()) {
                    data = result.getBytes(1);
                }
            } finally {
                db.closeConnection(connection);
            }
            return data;
        }

        private static List<byte[]> getMinMaxDataByMetricId(SqlDatabase db, UUID metricId, String actionSymbol, Long minEventDate, Long maxEventDate) throws SQLException {
            List<byte[]> datas = new ArrayList<byte[]>();
            String sql = Static.getDataByMetricIdSql(db, metricId, actionSymbol, minEventDate, maxEventDate, true);
            Connection connection = db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                if (result.next()) {
                    datas.add(result.getBytes(1));
                    datas.add(result.getBytes(2));
                }
            } finally {
                db.closeConnection(connection);
            }
            return datas;
        }

        private static UUID getMetricIdByDimensions(SqlDatabase db, MetricQuery query) throws SQLException {
            UUID metricId = null;
            // find the metricId, it might be null
            String sql = Static.getSelectMetricIdSql(db, query);
            Connection connection = db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                if (result.next()) {
                    metricId = UuidUtils.fromBytes(result.getBytes(1));
                }
                result.close();
                statement.close();
            } finally {
                db.closeConnection(connection);
            }
            return metricId;
        }

    }

    // MODIFICATIONS 

    @Record.FieldInternalNamePrefix("metrics.")
    public static class MetricFieldData extends Modification<ObjectField> {

        private transient MetricEventDateProcessor eventDateProcessor;

        private boolean dimension;
        private boolean metricValue;
        private boolean eventDateField;
        private boolean includeSelfDimension;
        private String eventDateProcessorClassName;
        private Set<String> dimensions = new HashSet<String>();
        private String eventDateFieldName;
        private String recordIdJoinTableName;
        private String recordIdJoinColumnName;

        public boolean isDimension() {
            return dimension;
        }

        public void setDimension(boolean dimension) {
            this.dimension = dimension;
        }

        public boolean isMetricValue() {
            return metricValue;
        }

        public void setMetricValue(boolean metricValue) {
            this.metricValue = metricValue;
        }

        public boolean isEventDateField() {
            return eventDateField;
        }

        public void setEventDateField(boolean eventDateField) {
            this.eventDateField = eventDateField;
        }

        public boolean isIncludeSelfDimension() {
            return includeSelfDimension;
        }

        public void setIncludeSelfDimension(boolean includeSelfDimension) {
            this.includeSelfDimension = includeSelfDimension;
        }

        @SuppressWarnings("unchecked")
        public MetricEventDateProcessor getEventDateProcessor() {
            if (eventDateProcessor == null) {
                if (eventDateProcessorClassName == null) {
                    return null;
                } else {
                    try {
                        Class<MetricEventDateProcessor> cls = (Class<MetricEventDateProcessor>) Class.forName(eventDateProcessorClassName);
                        eventDateProcessor = cls.newInstance();
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    } catch (InstantiationException e) {
                        throw new RuntimeException(e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            return eventDateProcessor;
        }

        public void setEventDateProcessorClass(Class<? extends MetricEventDateProcessor> eventDateProcessorClass) {
            this.eventDateProcessorClassName = eventDateProcessorClass.getName();
        }

        public Set<String> getDimensions() {
            return dimensions;
        }

        public void setDimensions(Set<String> dimensions) {
            this.dimensions = dimensions;
        }

        public String getEventDateFieldName() {
            return eventDateFieldName;
        }

        public void setEventDateFieldName(String eventDateFieldName) {
            this.eventDateFieldName = eventDateFieldName;
        }

        public String getRecordIdJoinTableName() {
            return recordIdJoinTableName;
        }

        public void setRecordIdJoinTableName(String recordIdJoinTableName) {
            this.recordIdJoinTableName = recordIdJoinTableName;
        }

        public String getRecordIdJoinColumnName() {
            return recordIdJoinColumnName;
        }

        public void setRecordIdJoinColumnName(String recordIdJoinColumnName) {
            this.recordIdJoinColumnName = recordIdJoinColumnName;
        }

    }

    public static class MetricAction extends Modification<Object> {

        //private static final Logger LOGGER = LoggerFactory.getLogger(MetricAction.class);

        private transient final Map<String, RecordMetric> recordMetrics = new HashMap<String, RecordMetric>();
        private transient final Map<String, ObjectField> eventDateFields = new HashMap<String, ObjectField>();
        private transient final Map<String, ObjectField> metricFields = new HashMap<String, ObjectField>();
        private transient Date oldEventDateValue;
        private transient Set<Integer> dimensionHashCodes;

        private ObjectField findMetricField(String internalName) {
            ObjectType recordType = ObjectType.getInstance(this.getOriginalObject().getClass());
            if (internalName == null) {
                for (ObjectField objectField : recordType.getFields()) {
                    if (objectField.as(MetricFieldData.class).isMetricValue()) {
                        return objectField;
                    }
                }
            } else {
                ObjectField objectField = recordType.getField(internalName);
                if (objectField != null && objectField.as(MetricFieldData.class).isMetricValue()) {
                    return objectField;
                }
            }
            throw new RuntimeException("At least one numeric field must be marked as @MetricValue");
        }

        private ObjectField getMetricField(String internalName) {
            if (!metricFields.containsKey(internalName)) {
                metricFields.put(internalName, findMetricField(internalName));
            }
            return metricFields.get(internalName);
        }

        private ObjectField getEventDateField(String metricFieldInternalName) {
            if (! eventDateFields.containsKey(metricFieldInternalName)) {
                ObjectType recordType = ObjectType.getInstance(this.getOriginalObject().getClass());
                ObjectField metricField = getMetricField(metricFieldInternalName);
                String eventDateFieldName = metricField.as(MetricFieldData.class).getEventDateFieldName();
                if (eventDateFieldName != null) {
                    ObjectField eventDateField = recordType.getField(eventDateFieldName);
                    if (eventDateField == null) {
                        throw new RuntimeException("Invalid eventDate field : " + eventDateFieldName);
                    }
                    if (eventDateField.as(MetricFieldData.class).isEventDateField()) {
                        eventDateFields.put(metricFieldInternalName, eventDateField);
                    } else {
                        throw new RuntimeException("The field " + eventDateFieldName + " is not annotated as @EventDate.");
                    }
                } else {
                    eventDateFields.put(metricFieldInternalName, null);
                }
            }
            return eventDateFields.get(metricFieldInternalName);
        }

        private Set<ObjectField> getDimensions(String metricFieldInternalName) {
            // Checking each field for @Dimension annotation
            Set<ObjectField> dimensions = new HashSet<ObjectField>();
            ObjectField metricField = getMetricField(metricFieldInternalName);
            ObjectType objectType = ObjectType.getInstance(getState().getType().getObjectClass());
            for (String dimensionFieldName : metricField.as(MetricFieldData.class).getDimensions()) {
                if (objectType.getField(dimensionFieldName) == null) {
                    throw new RuntimeException("Invalid dimension field : " + dimensionFieldName);
                }
                dimensions.add(objectType.getField(dimensionFieldName));
            }
            return dimensions;
        }

        private boolean dimensionValuesHaveChanged(String metricFieldInternalName) {
            Set<Integer> newDimensionHashCodes = new HashSet<Integer>();
            for (ObjectField field : this.getDimensions(metricFieldInternalName)) {
                if (this.getState().getByPath(field.getInternalName()) != null) {
                    newDimensionHashCodes.add(this.getState().getByPath(field.getInternalName()).hashCode());
                }
            }
            if (dimensionHashCodes == null || ! newDimensionHashCodes.equals(dimensionHashCodes)) {
                dimensionHashCodes = newDimensionHashCodes;
                return true;
            } else {
                return false;
            }
        }

        public void incrementMetric(String metricFieldInternalName, double c) {
            try {
                getRecordMetric(metricFieldInternalName).incrementMetric(c);
            } catch (SQLException e) {
                throw new DatabaseException(getRecordMetric(metricFieldInternalName).getDatabase(), "Error in RecordMetric.incrementMetric() : " + e.getLocalizedMessage());
            }
        }

        public void setMetric(String metricFieldInternalName, double c) {
            try {
                getRecordMetric(metricFieldInternalName).setMetric(c);
            } catch (SQLException e) {
                throw new DatabaseException(getRecordMetric(metricFieldInternalName).getDatabase(), "Error in RecordMetric.setMetric() : " + e.getLocalizedMessage());
            }
        }

        public void deleteMetrics() {
            try {
                getRecordMetric(null).deleteMetrics();
            } catch (SQLException e) {
                throw new DatabaseException(getRecordMetric(null).getDatabase(), "Error in RecordMetric.deleteMetrics() : " + e.getLocalizedMessage());
            }
        }

        public double getMetric(String metricFieldInternalName) {
            try {
                RecordMetric cr = getRecordMetric(metricFieldInternalName);
                cr.setQueryDateRange(null, null);
                return getRecordMetric(metricFieldInternalName).getMetric();
            } catch (SQLException e) {
                throw new DatabaseException(getRecordMetric(metricFieldInternalName).getDatabase(), "Error in RecordMetric.getMetric() : " + e.getLocalizedMessage());
            }
        }

        public double getMetricByRecordId(String metricFieldInternalName) {
            try {
                RecordMetric cr = getRecordMetric(metricFieldInternalName);
                cr.setQueryDateRange(null, null);
                return getRecordMetric(metricFieldInternalName).getMetricByRecordId();
            } catch (SQLException e) {
                throw new DatabaseException(getRecordMetric(metricFieldInternalName).getDatabase(), "Error in RecordMetric.getMetricByRecordId() : " + e.getLocalizedMessage());
            }
        }

        public double getMetricSinceDate(String metricFieldInternalName, Long startTimestamp) {
            return getMetricOverDateRange(metricFieldInternalName, startTimestamp, null);
        }

        public double getMetricAsOfDate(String metricFieldInternalName, Long endTimestamp) {
            return getMetricOverDateRange(metricFieldInternalName, null, endTimestamp);
        }

        public double getMetricOverDateRange(String metricFieldInternalName, Long startTimestamp, Long endTimestamp) {
            try {
                RecordMetric cr = getRecordMetric(metricFieldInternalName);
                if (cr.getEventDateProcessor().equals(MetricEventDateProcessor.None.class)) {
                    throw new RuntimeException("Date range does not apply - no MetricEventDateProcessor");
                }
                cr.setQueryDateRange(startTimestamp, endTimestamp);
                return getRecordMetric(metricFieldInternalName).getMetric();
            } catch (SQLException e) {
                throw new DatabaseException(getRecordMetric(metricFieldInternalName).getDatabase(), "Error in RecordMetric.getMetric() : " + e.getLocalizedMessage());
            }
        }

        public RecordMetric getRecordMetric(String metricFieldInternalName) {
            // if metricFieldInternalName is null, it will return the *first* @MetricValue in the type

            if (dimensionValuesHaveChanged(metricFieldInternalName)) {
                if (recordMetrics.containsKey(metricFieldInternalName)) {
                    recordMetrics.remove(metricFieldInternalName);
                }
            }

            ObjectField eventDateField = getEventDateField(metricFieldInternalName);
            if (! recordMetrics.containsKey(metricFieldInternalName)) {
                ObjectField metricField = getMetricField(metricFieldInternalName);
                RecordMetric recordMetric = new RecordMetric(this, metricField.getUniqueName(), this.getDimensions(metricFieldInternalName));
                if (eventDateField != null) {
                    recordMetric.setEventDateProcessor(eventDateField.as(MetricFieldData.class).getEventDateProcessor());
                } else {
                    recordMetric.setEventDateProcessor(null);
                }
                if (metricField.as(MetricFieldData.class).isIncludeSelfDimension()) {
                    recordMetric.setIncludeSelfDimension(true);
                }
                recordMetrics.put(metricFieldInternalName, recordMetric);
            }

            if (eventDateField != null) {
                Object eventDateValue = getState().getByPath(eventDateField.getInternalName());
                if (eventDateValue != null && eventDateValue instanceof Date) {
                    if (! ((Date) eventDateValue).equals(oldEventDateValue)) {
                        recordMetrics.get(metricFieldInternalName).setEventDate(((Date) eventDateValue).getTime());
                        oldEventDateValue = (Date)((Date) eventDateValue).clone();
                    }
                }
            }

            return recordMetrics.get(metricFieldInternalName);
        }

    }

}

class MetricQuery {
    private final String symbol;
    private final String actionSymbol;
    private final DimensionSet dimensions;
    private final Record record;
    private Long startTimestamp;
    private Long endTimestamp;
    private DimensionSet groupByDimensions;
    private String[] orderByDimensions;
    public boolean includeSelfDimension;

    public MetricQuery(String symbol, String actionSymbol, DimensionSet dimensions) {
        this.symbol = symbol;
        this.actionSymbol = actionSymbol;
        this.dimensions = dimensions;
        this.record = null;
    }

    public MetricQuery(String symbol, String actionSymbol, Record record,
            DimensionSet dimensions) {
        this.symbol = symbol;
        this.actionSymbol = actionSymbol;
        this.dimensions = dimensions;
        this.record = record;
    }

    public UUID getRecordIdForInsert() {
        if (isIncludeSelfDimension()) {
            return this.record.getId();
        } else {
            return null;
        }
    }

    public boolean isIncludeSelfDimension() {
        return includeSelfDimension;
    }

    public void setIncludeSelfDimension(boolean includeSelfDimension) {
        this.includeSelfDimension = includeSelfDimension;
    }

    public void setOrderByDimensions(String[] orderByDimensions) {
        this.orderByDimensions = orderByDimensions;
    }

    public void setGroupByDimensions(DimensionSet groupByDimensions) {
        this.groupByDimensions = groupByDimensions;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getActionSymbol() {
        return actionSymbol;
    }

    public DimensionSet getDimensions() {
        return dimensions;
    }

    public Record getRecord() {
        return record;
    }

    public Long getStartTimestamp() {
        return startTimestamp;
    }

    public Long getEndTimestamp() {
        return endTimestamp;
    }

    public void setDateRange(Long startTimestamp, Long endTimestamp) {
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    public DimensionSet getGroupByDimensions() {
        return groupByDimensions;
    }

    public String[] getOrderByDimensions() {
        return orderByDimensions;
    }

    public String toString() {
        return "action: " + getActionSymbol() + " recordId: " + getRecordIdForInsert() + " date range: " + startTimestamp + " - " + endTimestamp + " dimensions: " + dimensions;
    }
}

class Dimension implements Comparable<Dimension> {

    private final ObjectField objectField;
    private Set<Object> values = new HashSet<Object>();

    public Dimension(ObjectField objectField) {
        this.objectField = objectField;
    }

    public String getSymbol() {
        return getKey();
    }

    public String getKey() {
        return objectField.getUniqueName();
    }

    public ObjectField getObjectField() {
        return objectField;
    }

    public Set<Object> getValues() {
        return values;
    }

    public void addValue(UUID value) {
        this.values.add(value);
    }

    public void addValue(String value) {
        this.values.add(value);
    }

    public void addValue(Number value) {
        this.values.add(value);
    }

    public void addValue(Object value) {
        this.values.add(value.toString());
    }

    public String getIndexTable () {
        return RecordMetric.Static.getIndexTable(this.getObjectField());
    }

    public String toString() {
        StringBuilder str = new StringBuilder(getSymbol());
        if (values.size() > 1) {
            str.append("[");
            str.append(values.size());
            str.append("]");
        }
        return str.toString();
    }

    @Override
    public int compareTo(Dimension arg0) {
        return this.getSymbol().compareTo(arg0.getSymbol());
    }

}

class DimensionSet extends LinkedHashSet<Dimension> {
    private static final long serialVersionUID = 1L;

    public DimensionSet(Set<Dimension> dimensions) {
        super(dimensions);
    }

    public DimensionSet() {
        super();
    }

    public Set<String> keySet() {
        LinkedHashSet<String> keys = new LinkedHashSet<String>();
        for (Dimension d : this) {
            keys.add(d.getKey());
        }
        return keys;
    }

    public static DimensionSet createDimensionSet(Set<ObjectField> dimensions, Record record) {
        LinkedHashSet<Dimension> dimensionSet = new LinkedHashSet<Dimension>();
        for (ObjectField field : dimensions) {
            LinkedHashSet<Object> values = new LinkedHashSet<Object>();
            Object value = record.getState().get(field.getInternalName());
            if (value == null) continue;
            if (value instanceof Set) {
                if (((Set<?>)value).size() == 0) continue;
                values.addAll((Set<?>)value);
            } else {
                values.add(value);
            }
            Dimension dim = new Dimension(field);
            for (Object val : values) {
                if (val instanceof UUID) {
                    dim.addValue((UUID) val);
                } else if (value instanceof Number) {
                    dim.addValue((Number) val);
                } else {
                    dim.addValue(val.toString());
                }
            }
            dimensionSet.add(dim);
        }
        return new DimensionSet(dimensionSet);
    }

    public String getSymbol() {
        StringBuilder symbolBuilder = new StringBuilder();
        // if there is ever a prefix, put it here.
        //StringBuilder symbolBuilder = new StringBuilder(this.objectClass.getName());
        //symbolBuilder.append("/");

        boolean usedThisPrefix = false;
        String thisPrefix = "";
        for (Dimension d : getSortedDimensions()) {
            String dimSymbol = d.getSymbol();
            String prefix = dimSymbol.split("/")[0];
            if (! prefix.equals(thisPrefix)) {
                usedThisPrefix = false;
                thisPrefix = prefix;
            }
            if (!usedThisPrefix) {
                symbolBuilder.append(thisPrefix);
                symbolBuilder.append("/");
                usedThisPrefix = true;
            }
            if (dimSymbol.indexOf('/') > -1) {
                dimSymbol = dimSymbol.split("/")[1];
            }

            symbolBuilder.append(dimSymbol);
            if (d.getValues().size() > 1) {
                symbolBuilder.append("[");
                symbolBuilder.append(d.getValues().size());
                symbolBuilder.append("]");
            }
            symbolBuilder.append(',');
        }
        if (symbolBuilder.length() > 0) {
            symbolBuilder.setLength(symbolBuilder.length()-1);
        }
        symbolBuilder.append("#metric");
        return symbolBuilder.toString();
    }

    public String toString() {
        StringBuilder str = new StringBuilder(getSymbol());
        str.append(": ");
        for (Dimension dimension : this) {
            str.append(dimension.toString());
            str.append("=");
            str.append(dimension.getValues().toString());
            str.append(",");
        }
        str.setLength(str.length()-1);
        return str.toString();
    }

    private List<Dimension> getSortedDimensions() {
        ArrayList<Dimension> dims = new ArrayList<Dimension>(size());
        for (Dimension d : this) {
            dims.add(d);
        }
        Collections.sort(dims);
        return dims;
    }

}

