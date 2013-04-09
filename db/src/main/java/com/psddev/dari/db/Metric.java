package com.psddev.dari.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

class Metric {

    //private static final Logger LOGGER = LoggerFactory.getLogger(Metric.class);

    public static final String METRIC_TABLE = "Metric";
    public static final String METRIC_ID_FIELD = "id";
    public static final String METRIC_TYPE_FIELD = "typeId";
    public static final String METRIC_SYMBOL_FIELD = "symbolId";
    public static final String METRIC_CREATEDATE_FIELD = "createDate";
    public static final String METRIC_UPDATEDATE_FIELD = "updateDate";
    public static final String METRIC_DATA_FIELD = "data";

    public static final int AMOUNT_DECIMAL_PLACES = 6;
    public static final long AMOUNT_DECIMAL_SHIFT = (long) Math.pow(10, AMOUNT_DECIMAL_PLACES);
    public static final long DATE_DECIMAL_SHIFT = 60000L;
    public static final int CUMULATIVEAMOUNT_POSITION = 1;
    public static final int AMOUNT_POSITION = 2;

    private static final int QUERY_TIMEOUT = 3;
    private static final int DATE_BYTE_SIZE = 4;
    private static final int AMOUNT_BYTE_SIZE = 8;

    private final SqlDatabase db;
    private final MetricQuery query;
    private final Record record;

    private MetricEventDateProcessor eventDateProcessor;

    private Long updateDate;
    private Long eventDate;

    public Metric(SqlDatabase database, Record record, String actionSymbol) {
        this.db = database;
        this.query = new MetricQuery(actionSymbol, record);
        this.record = record;
    }

    public Metric(Record record, String actionSymbol) {
        this(Database.Static.getFirst(SqlDatabase.class), record, actionSymbol);
    }

    public void setEventDateProcessor(MetricEventDateProcessor processor) {
        this.eventDateProcessor = processor;
    }

    private Record getRecord() {
        return record;
    }

    public MetricEventDateProcessor getEventDateProcessor() {
        if (eventDateProcessor == null) {
            eventDateProcessor = new MetricEventDateProcessor.Hourly();
        }
        return eventDateProcessor;
    }

    public SqlDatabase getDatabase() {
        return db;
    }

    private MetricQuery getQuery() {
        return query;
    }

    public void setUpdateDate(long timestampMillis) {
        this.updateDate = timestampMillis;
    }

    public long getUpdateDate() {
        if (updateDate == null) {
            updateDate = System.currentTimeMillis();
        }
        return updateDate;
    }

    // This method should strip the minutes and seconds off of a timestamp, or otherwise process it
    public void setEventDate(long timestampMillis) {
        this.eventDate = getEventDateProcessor().process(timestampMillis);
    }

    public long getEventDate() {
        if (eventDate == null) {
            setEventDate((System.currentTimeMillis()));
        }
        return eventDate;
    }

    public void setQueryDateRange(Long startTimestamp, Long endTimestamp) {
        getQuery().setDateRange(startTimestamp, endTimestamp);
    }

    public Double getMetric() throws SQLException {
        return Static.getMetricById(getDatabase(), getRecord().getId(), getRecord().getState().getTypeId(), getQuery().getSymbol(), getQuery().getStartTimestamp(), getQuery().getEndTimestamp());
    }

    public void incrementMetric(Double amount) throws SQLException {
        // find the metricId, it might be null
        if (amount == 0) return; // This actually causes some problems if it's not here
        Static.doIncrementUpdateOrInsert(getDatabase(), getRecord().getId(), getRecord().getState().getTypeId(), getQuery().getSymbol(), amount, getUpdateDate(), getEventDate());
    }

    public void setMetric(Double amount) throws SQLException {
        // This only works if we're not tracking eventDate
        // TODO: better to just call the processor and see if the timestamp is 0
        if (! getEventDateProcessor().equals(MetricEventDateProcessor.None.class)) {
            throw new RuntimeException("Metric.setMetric() can only be used if EventDateProcessor is None");
        }
        Static.doSetUpdateOrInsert(getDatabase(), getRecord().getId(), getRecord().getState().getTypeId(), getQuery().getSymbol(), amount, getUpdateDate(), getEventDate());
    }

    public void deleteMetrics() throws SQLException {
        Static.doMetricDelete(getDatabase(), getRecord().getId(), getRecord().getState().getTypeId(), getQuery().getSymbol());
    }

    /** {@link Metric} utility methods. */
    public static final class Static {

        private Static() {
        }

        // Methods that generate SQL statements

        private static String getDataByIdSql(SqlDatabase db, UUID id, UUID typeId, String actionSymbol, Long minEventDate, Long maxEventDate, boolean selectMinData) {
            StringBuilder sqlBuilder = new StringBuilder();
            SqlVendor vendor = db.getVendor();

            sqlBuilder.append("SELECT ");

            sqlBuilder.append("MAX(");
            vendor.appendIdentifier(sqlBuilder, METRIC_DATA_FIELD);
            sqlBuilder.append(") ");
            vendor.appendIdentifier(sqlBuilder, "maxData");

            if (selectMinData) {
                sqlBuilder.append(", MIN(");
                vendor.appendIdentifier(sqlBuilder, METRIC_DATA_FIELD);
                sqlBuilder.append(") ");
                vendor.appendIdentifier(sqlBuilder, "minData");
            }

            sqlBuilder.append(" FROM ");
            vendor.appendIdentifier(sqlBuilder, METRIC_TABLE);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, METRIC_ID_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, id);

            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_SYMBOL_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, db.getSymbolId(actionSymbol));

            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_TYPE_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, typeId);

            if (maxEventDate != null) {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, METRIC_DATA_FIELD);
                sqlBuilder.append(" <= ");
                appendBinEncodeTimestampSql(sqlBuilder, null, vendor, maxEventDate, 'F');
            }

            if (minEventDate != null) {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, METRIC_DATA_FIELD);
                sqlBuilder.append(" >= ");
                appendBinEncodeTimestampSql(sqlBuilder, null, vendor, minEventDate, '0');
            }

            return sqlBuilder.toString();
        }

        private static String getUpdateSql(SqlDatabase db, List<Object> parameters, UUID id, UUID typeId, String symbol, double amount, long updateDate, long eventDate, boolean increment, boolean updateFuture) {
            StringBuilder updateBuilder = new StringBuilder("UPDATE ");
            SqlVendor vendor = db.getVendor();
            vendor.appendIdentifier(updateBuilder, METRIC_TABLE);
            updateBuilder.append(" SET ");

            vendor.appendIdentifier(updateBuilder, METRIC_DATA_FIELD);
            updateBuilder.append(" = ");
            updateBuilder.append(" UNHEX(");
                updateBuilder.append("CONCAT(");
                    // timestamp
                    appendHexEncodeExistingTimestampSql(updateBuilder, vendor, METRIC_DATA_FIELD);
                    updateBuilder.append(',');
                    // cumulativeAmount and amount
                    if (increment) {
                        appendHexEncodeIncrementAmountSql(updateBuilder, parameters, vendor, METRIC_DATA_FIELD, CUMULATIVEAMOUNT_POSITION, amount);
                        updateBuilder.append(',');
                        if (updateFuture) {
                            updateBuilder.append("IF (");
                                vendor.appendIdentifier(updateBuilder, METRIC_DATA_FIELD);
                                updateBuilder.append(" LIKE ");
                                    updateBuilder.append(" CONCAT(");
                                        appendBinEncodeTimestampSql(updateBuilder, parameters, vendor, eventDate, null);
                                    updateBuilder.append(", '%')");
                                    updateBuilder.append(","); // if it's the exact date, then update the amount
                                    appendHexEncodeIncrementAmountSql(updateBuilder, parameters, vendor, METRIC_DATA_FIELD, AMOUNT_POSITION, amount);
                                    updateBuilder.append(","); // if it's a date in the future, leave the date alone
                                    appendHexEncodeIncrementAmountSql(updateBuilder, parameters, vendor, METRIC_DATA_FIELD, AMOUNT_POSITION, 0);
                            updateBuilder.append(")");
                        } else {
                            appendHexEncodeIncrementAmountSql(updateBuilder, parameters, vendor, METRIC_DATA_FIELD, CUMULATIVEAMOUNT_POSITION, amount);
                        }
                    } else {
                        appendHexEncodeSetAmountSql(updateBuilder, parameters, vendor, amount);
                        updateBuilder.append(',');
                        appendHexEncodeSetAmountSql(updateBuilder, parameters, vendor, amount);
                    }
                updateBuilder.append(" )");
            updateBuilder.append(" )");

            updateBuilder.append(", ");
            vendor.appendIdentifier(updateBuilder, METRIC_UPDATEDATE_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, updateDate, parameters);
            updateBuilder.append(" WHERE ");
            vendor.appendIdentifier(updateBuilder, METRIC_ID_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, id, parameters);
            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_TYPE_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, typeId, parameters);
            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_SYMBOL_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, db.getSymbolId(symbol), parameters);
            updateBuilder.append(" AND ");

            vendor.appendIdentifier(updateBuilder, METRIC_DATA_FIELD);
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

        private static String getMetricInsertSql(SqlDatabase db, List<Object> parameters, UUID id, UUID typeId, String symbol, double amount, double cumulativeAmount, long createDate, long eventDate) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            vendor.appendIdentifier(insertBuilder, METRIC_TABLE);
            insertBuilder.append(" (");
            LinkedHashMap<String, Object> cols = new LinkedHashMap<String, Object>();
            cols.put(METRIC_ID_FIELD, id);
            cols.put(METRIC_TYPE_FIELD, typeId);
            cols.put(METRIC_SYMBOL_FIELD, db.getSymbolId(symbol));
            cols.put(METRIC_CREATEDATE_FIELD, createDate);
            cols.put(METRIC_UPDATEDATE_FIELD, createDate);
            cols.put(METRIC_DATA_FIELD, toBytes(eventDate, cumulativeAmount, amount));
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

        private static String getDeleteMetricSql(SqlDatabase db, UUID id, UUID typeId, String symbol) {
            SqlVendor vendor = db.getVendor();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ");
            vendor.appendIdentifier(sqlBuilder, METRIC_TABLE);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, METRIC_SYMBOL_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, db.getSymbolId(symbol));
            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_ID_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, id);
            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_TYPE_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, typeId);
            return sqlBuilder.toString();
        }

        // Methods that generate complicated bits of SQL

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

        // methods that convert bytes into values and back again

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

        private static double amountFromBytes(byte[] bytes, int position) {
            long amountLong = 0;

            int offset = DATE_BYTE_SIZE + ((position-1)*AMOUNT_BYTE_SIZE);

            for (int i = 0; i < AMOUNT_BYTE_SIZE; ++i) {
                amountLong = (amountLong << 8) | (bytes[i+offset] & 0xff);
            }

            return (double) amountLong / AMOUNT_DECIMAL_SHIFT;
        }

        private static long timestampFromBytes(byte[] bytes) {
            long timestamp = 0;

            for (int i = 0; i < DATE_BYTE_SIZE; ++i) {
                timestamp = (timestamp << 8) | (bytes[i] & 0xff);
            }

            return timestamp * DATE_DECIMAL_SHIFT;
        }

        /*
        private static double minMaxDataToAmount(byte[] minData, byte[] maxData) {
            double amount;
            double maxCumAmt = amountFromBytes(maxData, CUMULATIVEAMOUNT_POSITION);
            double minCumAmt = amountFromBytes(minData, CUMULATIVEAMOUNT_POSITION);
            double minAmt = amountFromBytes(minData, AMOUNT_POSITION);
            amount = (maxCumAmt - (minCumAmt - minAmt));
            return amount;
        }
        */

        // methods that actually touch the database

        private static void doIncrementUpdateOrInsert(SqlDatabase db, UUID id, UUID typeId, String symbol, double incrementAmount, long updateDate, long eventDate) throws SQLException {
            Connection connection = db.openConnection();
            try {
                List<Object> parameters = new ArrayList<Object>();

                // First, find the max eventDate. Under normal circumstances, this will either be null (INSERT), before our eventDate (INSERT) or equal to our eventDate (UPDATE).
                byte[] data = getDataById(db, id, typeId, symbol, null, null);
                String sql;

                if (data == null || timestampFromBytes(data) < eventDate) {
                    // No data for this eventDate; insert.
                    double previousCumulativeAmount = 0.0d;
                    if (data != null) {
                        previousCumulativeAmount = amountFromBytes(data, CUMULATIVEAMOUNT_POSITION);
                    }
                    parameters = new ArrayList<Object>();
                    sql = getMetricInsertSql(db, parameters, id, typeId, symbol, incrementAmount, previousCumulativeAmount+incrementAmount, updateDate, eventDate);
                } else if (timestampFromBytes(data) == eventDate) {
                    // There is data for this eventDate; update it.
                    sql = getUpdateSql(db, parameters, id, typeId, symbol, incrementAmount, updateDate, eventDate, true, false);
                } else { // if (timestampFromBytes(data) > eventDate)
                    // We are updating a row in the past, so we need to tell updateSql to update the cumulativeAmount for all rows in the future.
                    sql = getUpdateSql(db, parameters, id, typeId, symbol, incrementAmount, updateDate, eventDate, true, true);
                }
                SqlDatabase.Static.executeUpdateWithList( connection, sql, parameters);

            } finally {
                db.closeConnection(connection);
            }
        }

        private static void doSetUpdateOrInsert(SqlDatabase db, UUID id, UUID typeId, String symbol, double amount, long updateDate, long eventDate) throws SQLException {
            Connection connection = db.openConnection();
            if (eventDate != 0L) {
                throw new RuntimeException("Metric.Static.doSetUpdateOrInsert() can only be used if EventDatePrecision is NONE; eventDate is " + eventDate + ", should be 0L.");
            }
            try {
                List<Object> parameters = new ArrayList<Object>();
                String sql = getUpdateSql(db, parameters, id, typeId, symbol, amount, updateDate, eventDate, false, false);
                int rowsAffected = SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                if (rowsAffected == 0) {
                    parameters = new ArrayList<Object>();
                    sql = getMetricInsertSql(db, parameters, id, typeId, symbol, amount, amount, updateDate, eventDate);
                    SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doMetricDelete(SqlDatabase db, UUID id, UUID typeId, String symbol) throws SQLException {
            Connection connection = db.openConnection();
            List<Object> parameters = new ArrayList<Object>();
            try {
                String sql = getDeleteMetricSql(db, id, typeId, symbol);
                SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
            } finally {
                db.closeConnection(connection);
            }
        }

        // METRIC SELECT
        private static Double getMetricById(SqlDatabase db, UUID id, UUID typeId, String symbol, Long minEventDate, Long maxEventDate) throws SQLException {
            if (minEventDate == null) {
                byte[] data = getDataById(db, id, typeId, symbol, minEventDate, maxEventDate);
                if (data == null) return null;
                return amountFromBytes(data, CUMULATIVEAMOUNT_POSITION);
            } else {
                List<byte[]> datas = getMinMaxDataById(db, id, typeId, symbol, minEventDate, maxEventDate);
                if (datas.size() == 0) return null;
                if (datas.get(0) == null) return null;
                double maxCumulativeAmount = amountFromBytes(datas.get(0), CUMULATIVEAMOUNT_POSITION);
                double minCumulativeAmount = amountFromBytes(datas.get(1), CUMULATIVEAMOUNT_POSITION);
                double minAmount = amountFromBytes(datas.get(1), AMOUNT_POSITION);
                return maxCumulativeAmount - (minCumulativeAmount - minAmount);
            }
        }

        /*
        private static Long getMaxEventDateById(SqlDatabase db, UUID id, UUID typeId, String symbol, Long minEventDate, Long maxEventDate) throws SQLException {
            byte[] data = getDataById(db, id, typeId, symbol, minEventDate, maxEventDate);
            if (data == null) return null;
            return timestampFromBytes(data);
        }
        */

        private static byte[] getDataById(SqlDatabase db, UUID id, UUID typeId, String symbol, Long minEventDate, Long maxEventDate) throws SQLException {
            String sql = Static.getDataByIdSql(db, id, typeId, symbol, minEventDate, maxEventDate, false);
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

        private static List<byte[]> getMinMaxDataById(SqlDatabase db, UUID id, UUID typeId, String symbol, Long minEventDate, Long maxEventDate) throws SQLException {
            List<byte[]> datas = new ArrayList<byte[]>();
            String sql = Static.getDataByIdSql(db, id, typeId, symbol, minEventDate, maxEventDate, true);
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

    }

    // MODIFICATIONS

    @Record.FieldInternalNamePrefix("metrics.")
    public static class FieldData extends Modification<ObjectField> {

        private transient MetricEventDateProcessor eventDateProcessor;

        private boolean metricValue;
        private boolean eventDateField;
        private String eventDateProcessorClassName;
        private String eventDateFieldName;

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

        public String getEventDateFieldName() {
            return eventDateFieldName;
        }

        public void setEventDateFieldName(String eventDateFieldName) {
            this.eventDateFieldName = eventDateFieldName;
        }

    }
}
