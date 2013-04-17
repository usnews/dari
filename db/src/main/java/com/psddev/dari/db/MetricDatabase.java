package com.psddev.dari.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.psddev.dari.util.UuidUtils;

import org.joda.time.DateTime;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

class MetricDatabase {

    //private static final Logger LOGGER = LoggerFactory.getLogger(MetricDatabase.class);

    public static final String METRIC_TABLE = "Metric";
    public static final String METRIC_ID_FIELD = "id";
    public static final String METRIC_TYPE_FIELD = "typeId";
    public static final String METRIC_SYMBOL_FIELD = "symbolId";
    public static final String METRIC_DIMENSION_FIELD = "dimensionId";
    public static final String METRIC_DIMENSION_TABLE = "MetricDimension";
    public static final String METRIC_DIMENSION_VALUE_FIELD = "value";
    public static final String METRIC_DATA_FIELD = "data";

    private static final int AMOUNT_DECIMAL_PLACES = 6;
    private static final long AMOUNT_DECIMAL_SHIFT = (long) Math.pow(10, AMOUNT_DECIMAL_PLACES);
    private static final long DATE_DECIMAL_SHIFT = 60000L;
    private static final int CUMULATIVEAMOUNT_POSITION = 1;
    private static final int AMOUNT_POSITION = 2;

    private static final int QUERY_TIMEOUT = 3;
    private static final int DATE_BYTE_SIZE = 4;
    private static final int AMOUNT_BYTE_SIZE = 8;

    private static final int DIMENSION_CACHE_SIZE = 1000;

    private final SqlDatabase db;
    private final MetricQuery query;
    private final State state;

    private static final transient Cache<String, UUID> dimensionCache = CacheBuilder.newBuilder().maximumSize(DIMENSION_CACHE_SIZE).build();

    private MetricInterval eventDateProcessor;

    private Long eventDate;
    private boolean isImplicitEventDate;

    public MetricDatabase(SqlDatabase database, State state, String actionSymbol) {
        this.db = database;
        this.query = new MetricQuery(actionSymbol, state);
        this.state = state;
    }

    public MetricDatabase(State state, String actionSymbol) {
        this(Database.Static.getFirst(SqlDatabase.class), state, actionSymbol);
    }

    public void setEventDateProcessor(MetricInterval processor) {
        this.eventDateProcessor = processor;
    }

    private State getState() {
        return state;
    }

    public MetricInterval getEventDateProcessor() {
        if (eventDateProcessor == null) {
            eventDateProcessor = new MetricInterval.Hourly();
        }
        return eventDateProcessor;
    }

    public SqlDatabase getDatabase() {
        return db;
    }

    private MetricQuery getQuery() {
        return query;
    }

    // This method should strip the minutes and seconds off of a timestamp, or otherwise process it
    public void setEventDate(DateTime eventDate) {
        if (eventDate == null) {
            eventDate = new DateTime();
            isImplicitEventDate = true;
        } else {
            isImplicitEventDate = false;
        }
        this.eventDate = getEventDateProcessor().process(eventDate);
    }

    public void setEventDateMillis(Long timestampMillis) {
        setEventDate((timestampMillis == null ? null : new DateTime(timestampMillis)));
    }

    public long getEventDate() {
        if (eventDate == null) {
            setEventDateMillis(null);
        }
        return eventDate;
    }

    public void setQueryDateRange(DateTime startTimestamp, DateTime endTimestamp) {
        setQueryTimestampRange((startTimestamp == null ? null : startTimestamp.getMillis()), (endTimestamp == null ? null : endTimestamp.getMillis()));
    }

    public void setQueryTimestampRange(Long startTimestamp, Long endTimestamp) {
        getQuery().setDateRange(startTimestamp, endTimestamp);
    }

    public Double getMetric(String dimensionValue) throws SQLException {
        return Static.getMetricByIdAndDimension(getDatabase(), getState().getId(), getState().getTypeId(), getQuery().getSymbol(), getDimensionId(dimensionValue), getQuery().getStartTimestamp(), getQuery().getEndTimestamp());
    }

    public Double getMetricSum() throws SQLException {
        return Static.getMetricSumById(getDatabase(), getState().getId(), getState().getTypeId(), getQuery().getSymbol(), getQuery().getStartTimestamp(), getQuery().getEndTimestamp());
    }

    public Map<String, Double> getMetricValues() throws SQLException {
        return Static.getMetricDimensionsById(getDatabase(), getState().getId(), getState().getTypeId(), getQuery().getSymbol(), getQuery().getStartTimestamp(), getQuery().getEndTimestamp());
    }

    public Map<DateTime, Double> getMetricTimeline(String dimensionValue, MetricInterval metricInterval) throws SQLException {
        if (metricInterval == null) {
            metricInterval = getEventDateProcessor();
        }
        return Static.getMetricTimelineByIdAndDimension(getDatabase(), getState().getId(), getState().getTypeId(), getQuery().getSymbol(), getDimensionId(dimensionValue), getQuery().getStartTimestamp(), getQuery().getEndTimestamp(), metricInterval);
    }

    public Map<DateTime, Double> getMetricSumTimeline(MetricInterval metricInterval) throws SQLException {
        if (metricInterval == null) {
            metricInterval = getEventDateProcessor();
        }
        return Static.getMetricSumTimelineById(getDatabase(), getState().getId(), getState().getTypeId(), getQuery().getSymbol(), getQuery().getStartTimestamp(), getQuery().getEndTimestamp(), metricInterval);
    }

    public void incrementMetric(String dimensionValue, Double amount) throws SQLException {
        // find the metricId, it might be null
        if (amount == 0) return; // This actually causes some problems if it's not here
        Static.doIncrementUpdateOrInsert(getDatabase(), getState().getId(), getState().getTypeId(), getQuery().getSymbol(), getDimensionId(dimensionValue), amount, getEventDate(), isImplicitEventDate);
    }

    public void setMetric(String dimensionValue, Double amount) throws SQLException {
        // This only works if we're not tracking eventDate
        if (getEventDate() != 0) {
            throw new RuntimeException("MetricDatabase.setMetric() can only be used if EventDateProcessor is None");
        }
        Static.doSetUpdateOrInsert(getDatabase(), getState().getId(), getState().getTypeId(), getQuery().getSymbol(), getDimensionId(dimensionValue), amount, getEventDate());
    }

    public void deleteMetric() throws SQLException {
        Static.doMetricDelete(getDatabase(), getState().getId(), getState().getTypeId(), getQuery().getSymbol());
    }

    public static UUID getDimensionIdByValue(String dimensionValue) {
        if (dimensionValue == null || "".equals(dimensionValue)) {
            return UuidUtils.ZERO_UUID;
        }
        UUID dimensionId = dimensionCache.getIfPresent(dimensionValue);
        if (dimensionId == null) {
            try {
                SqlDatabase db = Database.Static.getFirst(SqlDatabase.class);
                dimensionId = Static.getDimensionIdByValue(db, dimensionValue);
                if (dimensionId == null) {
                    dimensionId = UuidUtils.createSequentialUuid();
                    Static.doInsertDimensionValue(db, dimensionId, dimensionValue);
                }
                dimensionCache.put(dimensionValue, dimensionId);
            } catch (SQLException e) {
                throw new DatabaseException(Database.Static.getFirst(SqlDatabase.class), "Error in MetricDatabase.getDimensionIdByValue() : " + e.getLocalizedMessage());
            }
        }
        return dimensionId;
    }

    private UUID getDimensionId(String dimensionValue) throws SQLException {
        if (dimensionValue == null || "".equals(dimensionValue)) {
            return UuidUtils.ZERO_UUID;
        }
        UUID dimensionId = dimensionCache.getIfPresent(dimensionValue);
        if (dimensionId == null) {
            dimensionId = Static.getDimensionIdByValue(db, dimensionValue);
            if (dimensionId == null) {
                dimensionId = UuidUtils.createSequentialUuid();
                Static.doInsertDimensionValue(db, dimensionId, dimensionValue);
            }
            dimensionCache.put(dimensionValue, dimensionId);
        }
        return dimensionId;
    }

    /** {@link MetricDatabase} utility methods. */
    public static final class Static {

        private Static() {
        }

        // Methods that generate SQL statements

        private static String getDataSql(SqlDatabase db, UUID id, UUID typeId, String symbol, UUID dimensionId, Long minEventDate, Long maxEventDate, boolean selectMinData) {
            return getDataSql(db, id, typeId, symbol, dimensionId, minEventDate, maxEventDate, selectMinData, null, null);
        }

        private static String getDataSql(SqlDatabase db, UUID id, UUID typeId, String symbol, UUID dimensionId, Long minEventDate, Long maxEventDate, boolean selectMinData, String extraSelectSql, String extraGroupBySql) {
            StringBuilder sqlBuilder = new StringBuilder();
            SqlVendor vendor = db.getVendor();

            sqlBuilder.append("SELECT ");

            if (dimensionId == null) {
                vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
                sqlBuilder.append(", ");
            }

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

            if (extraSelectSql != null && ! "".equals(extraSelectSql)) {
                sqlBuilder.append(", ");
                sqlBuilder.append(extraSelectSql);
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
            vendor.appendValue(sqlBuilder, db.getSymbolId(symbol));

            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_TYPE_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, typeId);

            if (dimensionId != null) {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
                sqlBuilder.append(" = ");
                vendor.appendValue(sqlBuilder, dimensionId);
            }

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

            if (dimensionId == null) {
                sqlBuilder.append(" GROUP BY ");
                vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
                if (extraGroupBySql != null && ! "".equals(extraGroupBySql)) {
                    sqlBuilder.append(", ");
                    sqlBuilder.append(extraGroupBySql);
                }
            } else if (extraGroupBySql != null && ! "".equals(extraGroupBySql)) {
                sqlBuilder.append(" GROUP BY ");
                sqlBuilder.append(extraGroupBySql);
            }

            return sqlBuilder.toString();
        }

        private static String getSumSql(SqlDatabase db, UUID id, UUID typeId, String symbol, Long minEventDate, Long maxEventDate) {
            StringBuilder sqlBuilder = new StringBuilder();
            SqlVendor vendor = db.getVendor();
            String innerSql = getDataSql(db, id, typeId, symbol, null, minEventDate, maxEventDate, true);

            sqlBuilder.append("SELECT ");
            appendSelectCalculatedAmountSql(sqlBuilder, vendor, "minData", "maxData", true);
            sqlBuilder.append(" FROM (");
            sqlBuilder.append(innerSql);
            sqlBuilder.append(") x");

            return sqlBuilder.toString();
        }

        private static String getDimensionsSql(SqlDatabase db, UUID id, UUID typeId, String symbol, Long minEventDate, Long maxEventDate) {
            StringBuilder sqlBuilder = new StringBuilder();
            SqlVendor vendor = db.getVendor();
            String innerSql = getDataSql(db, id, typeId, symbol, null, minEventDate, maxEventDate, true);

            sqlBuilder.append("SELECT ");
            vendor.appendIdentifier(sqlBuilder, "d");
            sqlBuilder.append(".");
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_VALUE_FIELD);
            sqlBuilder.append(", ");
            appendSelectCalculatedAmountSql(sqlBuilder, vendor, "minData", "maxData", true);
            sqlBuilder.append(" FROM (");
            sqlBuilder.append(innerSql);
            sqlBuilder.append(") x ");
            sqlBuilder.append(" JOIN ");
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_TABLE);
            sqlBuilder.append(" ");
            vendor.appendIdentifier(sqlBuilder, "d");
            sqlBuilder.append(" ON (");
            vendor.appendIdentifier(sqlBuilder, "x");
            sqlBuilder.append(".");
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendIdentifier(sqlBuilder, "d");
            sqlBuilder.append(".");
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
            sqlBuilder.append(")");
            sqlBuilder.append(" GROUP BY ");
            vendor.appendIdentifier(sqlBuilder, "d");
            sqlBuilder.append(".");
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_VALUE_FIELD);

            return sqlBuilder.toString();
        }

        private static String getTimelineSql(SqlDatabase db, UUID id, UUID typeId, String symbol, UUID dimensionId, Long minEventDate, Long maxEventDate, MetricInterval metricInterval) {
            
            SqlVendor vendor = db.getVendor();
            String dateFormatString = metricInterval.getSqlDateFormat(vendor);

            StringBuilder extraSelectSqlBuilder = new StringBuilder("MIN(");
            appendSelectTimestampSql(extraSelectSqlBuilder, vendor, "data");
            extraSelectSqlBuilder.append(") * ");
            vendor.appendValue(extraSelectSqlBuilder, DATE_DECIMAL_SHIFT);
            extraSelectSqlBuilder.append(" ");
            vendor.appendIdentifier(extraSelectSqlBuilder, "eventDate");

            StringBuilder extraGroupBySqlBuilder = new StringBuilder("DATE_FORMAT(FROM_UNIXTIME(");
            appendSelectTimestampSql(extraGroupBySqlBuilder, vendor, "data");
            extraGroupBySqlBuilder.append("*");
            vendor.appendValue(extraGroupBySqlBuilder, (DATE_DECIMAL_SHIFT/1000L));
            extraGroupBySqlBuilder.append("),");
            vendor.appendValue(extraGroupBySqlBuilder, dateFormatString);
            extraGroupBySqlBuilder.append(")");

            String sql = getDataSql(db, id, typeId, symbol, dimensionId, minEventDate, maxEventDate, true, extraSelectSqlBuilder.toString(), extraGroupBySqlBuilder.toString());

            return sql;
        }

        private static String getSumTimelineSql(SqlDatabase db, UUID id, UUID typeId, String symbol, Long minEventDate, Long maxEventDate, MetricInterval metricInterval) {
            
            StringBuilder sqlBuilder = new StringBuilder();
            SqlVendor vendor = db.getVendor();

            String innerSql = getTimelineSql(db, id, typeId, symbol, null, minEventDate, maxEventDate, metricInterval);

            sqlBuilder.append("SELECT ");
            appendSelectCalculatedAmountSql(sqlBuilder, vendor, "minData", "maxData", true);
            sqlBuilder.append(", ");
            vendor.appendIdentifier(sqlBuilder, "eventDate");
            sqlBuilder.append(" FROM (");
            sqlBuilder.append(innerSql);
            sqlBuilder.append(") x");
            sqlBuilder.append(" GROUP BY ");
            vendor.appendIdentifier(sqlBuilder, "eventDate");

            return sqlBuilder.toString();
        }

        private static String getUpdateSql(SqlDatabase db, List<Object> parameters, UUID id, UUID typeId, String symbol, UUID dimensionId, double amount, long eventDate, boolean increment, boolean updateFuture) {
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
                            appendHexEncodeIncrementAmountSql(updateBuilder, parameters, vendor, METRIC_DATA_FIELD, AMOUNT_POSITION, amount);
                        }
                    } else {
                        appendHexEncodeSetAmountSql(updateBuilder, parameters, vendor, amount);
                        updateBuilder.append(',');
                        appendHexEncodeSetAmountSql(updateBuilder, parameters, vendor, amount);
                    }
                updateBuilder.append(" )");
            updateBuilder.append(" )");

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
            vendor.appendIdentifier(updateBuilder, METRIC_DIMENSION_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, dimensionId, parameters);

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

        private static String getMetricInsertSql(SqlDatabase db, List<Object> parameters, UUID id, UUID typeId, String symbol, UUID dimensionId, double amount, double cumulativeAmount, long eventDate) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            vendor.appendIdentifier(insertBuilder, METRIC_TABLE);
            insertBuilder.append(" (");
            LinkedHashMap<String, Object> cols = new LinkedHashMap<String, Object>();
            cols.put(METRIC_ID_FIELD, id);
            cols.put(METRIC_TYPE_FIELD, typeId);
            cols.put(METRIC_SYMBOL_FIELD, db.getSymbolId(symbol));
            cols.put(METRIC_DIMENSION_FIELD, dimensionId);
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

        private static String getDimensionIdByValueSql(SqlDatabase db, String dimensionValue) {
            SqlVendor vendor = db.getVendor();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT ");
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
            sqlBuilder.append(" FROM ");
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_TABLE);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_VALUE_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, dimensionValue);
            return sqlBuilder.toString();
        }

        private static String getInsertDimensionValueSql(SqlDatabase db, List<Object> parameters, UUID dimensionId, String dimensionValue) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            vendor.appendIdentifier(insertBuilder, METRIC_DIMENSION_TABLE);
            insertBuilder.append(" (");
            vendor.appendIdentifier(insertBuilder, METRIC_DIMENSION_FIELD);
            insertBuilder.append(", ");
            vendor.appendIdentifier(insertBuilder, METRIC_DIMENSION_VALUE_FIELD);
            insertBuilder.append(") VALUES (");
            vendor.appendBindValue(insertBuilder, dimensionId, parameters);
            insertBuilder.append(", ");
            vendor.appendBindValue(insertBuilder, dimensionValue, parameters);
            insertBuilder.append(")");
            return insertBuilder.toString();
        }

        // Methods that generate complicated bits of SQL
        // TODO: all of these are MySQL only.

        public static void appendSelectCalculatedAmountSql(StringBuilder str, SqlVendor vendor, String minDataColumnIdentifier, String maxDataColumnIdentifier, boolean includeSum) {

            str.append("ROUND(");
            if (includeSum) str.append("SUM");
            str.append("(");
            appendSelectAmountSql(str, vendor, maxDataColumnIdentifier, CUMULATIVEAMOUNT_POSITION);
            str.append(" - (");
            appendSelectAmountSql(str, vendor, minDataColumnIdentifier, CUMULATIVEAMOUNT_POSITION);
            str.append(" - ");
            appendSelectAmountSql(str, vendor, minDataColumnIdentifier, AMOUNT_POSITION);
            str.append(") ");

            str.append(")");
            str.append(" / ");
            vendor.appendValue(str, AMOUNT_DECIMAL_SHIFT);
            str.append(",");
            vendor.appendValue(str, AMOUNT_DECIMAL_PLACES);
            str.append(") ");

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

        public static void appendSelectTimestampSql(StringBuilder str, SqlVendor vendor, String columnIdentifier) {
            // This does NOT shift the decimal place or round to 6 places. Do it yourself AFTER any other arithmetic.
            // position is 1 or 2
            // columnIdentifier is "`data`" or "MAX(`data`)" - already escaped
            str.append("CONV(");
                str.append("HEX(");
                    str.append("SUBSTR(");
                        str.append(columnIdentifier);
                        str.append(",");
                        vendor.appendValue(str, 1);
                        str.append(",");
                        vendor.appendValue(str, DATE_BYTE_SIZE);
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

        private static void doIncrementUpdateOrInsert(SqlDatabase db, UUID id, UUID typeId, String symbol, UUID dimensionId, double incrementAmount, long eventDate, boolean isImplicitEventDate) throws SQLException {
            Connection connection = db.openConnection();
            try {
                List<Object> parameters = new ArrayList<Object>();

                if (isImplicitEventDate) {
                    // If they have not passed in an eventDate, we can assume a couple of things:
                    // 1) The event date is the CURRENT date
                    // 2) There is NOT any FUTURE data
                    // 3) We CANNOT assume the row exists

                    // Try to do an update. This is the best case scenario, and does not require any reads.
                    String sql = getUpdateSql(db, parameters, id, typeId, symbol, dimensionId, incrementAmount, eventDate, true, false);
                    int rowsAffected = SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                    if (0 == rowsAffected) {
                        // There is no data for the current date. Now we have to read 
                        // the previous cumulative amount so we can insert a new row.
                        byte[] data = getDataByIdAndDimension(db, id, typeId, symbol, dimensionId, null, null);
                        double previousCumulativeAmount = 0.0d;
                        if (data != null) {
                            previousCumulativeAmount = amountFromBytes(data, CUMULATIVEAMOUNT_POSITION);
                        }
                        parameters = new ArrayList<Object>();
                        sql = getMetricInsertSql(db, parameters, id, typeId, symbol, dimensionId, incrementAmount, previousCumulativeAmount+incrementAmount, eventDate);
                        SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                    }

                } else {

                    // First, find the max eventDate. Under normal circumstances, this will either be null (INSERT), before our eventDate (INSERT) or equal to our eventDate (UPDATE).
                    byte[] data = getDataByIdAndDimension(db, id, typeId, symbol, dimensionId, null, null);

                    if (data == null || timestampFromBytes(data) < eventDate) {
                        // No data for this eventDate; insert.
                        double previousCumulativeAmount = 0.0d;
                        if (data != null) {
                            previousCumulativeAmount = amountFromBytes(data, CUMULATIVEAMOUNT_POSITION);
                        }
                        String sql = getMetricInsertSql(db, parameters, id, typeId, symbol, dimensionId, incrementAmount, previousCumulativeAmount+incrementAmount, eventDate);
                        SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                    } else if (timestampFromBytes(data) == eventDate) {
                        // There is data for this eventDate; update it.
                        String sql = getUpdateSql(db, parameters, id, typeId, symbol, dimensionId, incrementAmount, eventDate, true, false);
                        SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                    } else { // if (timestampFromBytes(data) > eventDate)
                        // The max(eventDate) in the table is greater than our
                        // event date. If there exists a row in the past, UPDATE it
                        // or if not, INSERT. Either way we will be updating future
                        // data, so just INSERT with a value of 0 if necessary, then 
                        // UPDATE all rows.
                        byte[] oldData = getDataByIdAndDimension(db, id, typeId, symbol, dimensionId, null, eventDate);
                        if (oldData == null || timestampFromBytes(oldData) < eventDate) {
                            double previousCumulativeAmount = 0.0d;
                            if (oldData != null) {
                                previousCumulativeAmount = amountFromBytes(oldData, CUMULATIVEAMOUNT_POSITION);
                            }
                            List<Object> parameters2 = new ArrayList<Object>();
                            String sql2 = getMetricInsertSql(db, parameters2, id, typeId, symbol, dimensionId, 0, previousCumulativeAmount, eventDate);
                            SqlDatabase.Static.executeUpdateWithList( connection, sql2, parameters2);
                        }
                        String sql = getUpdateSql(db, parameters, id, typeId, symbol, dimensionId, incrementAmount, eventDate, true, true);
                        SqlDatabase.Static.executeUpdateWithList( connection, sql, parameters);
                    }
                }

            } finally {
                db.closeConnection(connection);
            }
        }

        private static void doSetUpdateOrInsert(SqlDatabase db, UUID id, UUID typeId, String symbol, UUID dimensionId, double amount, long eventDate) throws SQLException {
            Connection connection = db.openConnection();
            if (eventDate != 0L) {
                throw new RuntimeException("MetricDatabase.Static.doSetUpdateOrInsert() can only be used if EventDatePrecision is NONE; eventDate is " + eventDate + ", should be 0L.");
            }
            try {
                List<Object> parameters = new ArrayList<Object>();
                String sql = getUpdateSql(db, parameters, id, typeId, symbol, dimensionId, amount, eventDate, false, false);
                int rowsAffected = SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                if (rowsAffected == 0) {
                    parameters = new ArrayList<Object>();
                    sql = getMetricInsertSql(db, parameters, id, typeId, symbol, dimensionId, amount, amount, eventDate);
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

        static void doInsertDimensionValue(SqlDatabase db, UUID dimensionId, String dimensionValue) throws SQLException {
            Connection connection = db.openConnection();
            List<Object> parameters = new ArrayList<Object>();
            try {
                String sql = getInsertDimensionValueSql(db, parameters, dimensionId, dimensionValue);
                SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
            } finally {
                db.closeConnection(connection);
            }
        }

        // METRIC SELECT
        private static Double getMetricSumById(SqlDatabase db, UUID id, UUID typeId, String symbol, Long minEventDate, Long maxEventDate) throws SQLException {
            String sql = getSumSql(db, id, typeId, symbol, minEventDate, maxEventDate);
            Double amount = null;
            Connection connection = db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                if (result.next()) {
                    amount = result.getDouble(1);
                }
            } finally {
                db.closeConnection(connection);
            }
            return amount;
        }

        private static Map<String, Double> getMetricDimensionsById(SqlDatabase db, UUID id, UUID typeId, String symbol, Long minEventDate, Long maxEventDate) throws SQLException {
            String sql = getDimensionsSql(db, id, typeId, symbol, minEventDate, maxEventDate);
            Map<String, Double> values = new HashMap<String, Double>();
            Connection connection = db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                while (result.next()) {
                    values.put(result.getString(1), result.getDouble(2));
                }
            } finally {
                db.closeConnection(connection);
            }
            return values;
        }

        private static Double getMetricByIdAndDimension(SqlDatabase db, UUID id, UUID typeId, String symbol, UUID dimensionId, Long minEventDate, Long maxEventDate) throws SQLException {
            if (minEventDate == null) {
                byte[] data = getDataByIdAndDimension(db, id, typeId, symbol, dimensionId, minEventDate, maxEventDate);
                if (data == null) return null;
                return amountFromBytes(data, CUMULATIVEAMOUNT_POSITION);
            } else {
                List<byte[]> datas = getMinMaxDataByIdAndDimension(db, id, typeId, symbol, dimensionId, minEventDate, maxEventDate);
                if (datas.size() == 0) return null;
                if (datas.get(0) == null) return null;
                double maxCumulativeAmount = amountFromBytes(datas.get(0), CUMULATIVEAMOUNT_POSITION);
                double minCumulativeAmount = amountFromBytes(datas.get(1), CUMULATIVEAMOUNT_POSITION);
                double minAmount = amountFromBytes(datas.get(1), AMOUNT_POSITION);
                return maxCumulativeAmount - (minCumulativeAmount - minAmount);
            }
        }

        private static Map<DateTime, Double> getMetricTimelineByIdAndDimension(SqlDatabase db, UUID id, UUID typeId, String symbol, UUID dimensionId, Long minEventDate, Long maxEventDate, MetricInterval metricInterval) throws SQLException {
            String sql = getTimelineSql(db, id, typeId, symbol, dimensionId, minEventDate, maxEventDate, metricInterval);
            Map<DateTime, Double> values = new LinkedHashMap<DateTime, Double>();
            Connection connection = db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                while (result.next()) {
                    byte[] maxData = result.getBytes(1);
                    byte[] minData = result.getBytes(2);
                    long timestamp = result.getLong(3);
                    timestamp = metricInterval.process(new DateTime(timestamp));
                    double maxCumulativeAmount = amountFromBytes(maxData, CUMULATIVEAMOUNT_POSITION);
                    double minCumulativeAmount = amountFromBytes(minData, CUMULATIVEAMOUNT_POSITION);
                    double minAmount = amountFromBytes(minData, AMOUNT_POSITION);
                    double intervalAmount = maxCumulativeAmount - (minCumulativeAmount - minAmount);
                    values.put(new DateTime(timestamp), intervalAmount);
                }
            } finally {
                db.closeConnection(connection);
            }
            return values;
        }

        private static Map<DateTime, Double> getMetricSumTimelineById(SqlDatabase db, UUID id, UUID typeId, String symbol, Long minEventDate, Long maxEventDate, MetricInterval metricInterval) throws SQLException {
            String sql = getSumTimelineSql(db, id, typeId, symbol, minEventDate, maxEventDate, metricInterval);
            Map<DateTime, Double> values = new LinkedHashMap<DateTime, Double>();
            Connection connection = db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                while (result.next()) {
                    double intervalAmount = result.getLong(1);
                    long timestamp = result.getLong(2);
                    timestamp = metricInterval.process(new DateTime(timestamp));
                    values.put(new DateTime(timestamp), intervalAmount);
                }
            } finally {
                db.closeConnection(connection);
            }
            return values;
        }

        /*
        private static Long getMaxEventDateById(SqlDatabase db, UUID id, UUID typeId, String symbol, Long minEventDate, Long maxEventDate) throws SQLException {
            byte[] data = getDataById(db, id, typeId, symbol, minEventDate, maxEventDate);
            if (data == null) return null;
            return timestampFromBytes(data);
        }
        */

        private static byte[] getDataByIdAndDimension(SqlDatabase db, UUID id, UUID typeId, String symbol, UUID dimensionId, Long minEventDate, Long maxEventDate) throws SQLException {
            String sql = getDataSql(db, id, typeId, symbol, dimensionId, minEventDate, maxEventDate, false);
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

        private static List<byte[]> getMinMaxDataByIdAndDimension(SqlDatabase db, UUID id, UUID typeId, String symbol, UUID dimensionId, Long minEventDate, Long maxEventDate) throws SQLException {
            List<byte[]> datas = new ArrayList<byte[]>();
            String sql = getDataSql(db, id, typeId, symbol, dimensionId, minEventDate, maxEventDate, true);
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

        private static UUID getDimensionIdByValue(SqlDatabase db, String dimensionValue) throws SQLException {
            String sql = getDimensionIdByValueSql(db, dimensionValue);
            Connection connection = db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                if (result.next()) {
                   return UuidUtils.fromBytes(result.getBytes(1));
                }
            } finally {
                db.closeConnection(connection);
            }
            return null;
        }

    }

    // MODIFICATIONS

    @Record.FieldInternalNamePrefix("metrics.")
    public static class FieldData extends Modification<ObjectField> {

        private transient MetricInterval eventDateProcessor;

        private boolean metricValue;
        private String eventDateProcessorClassName;

        public boolean isMetricValue() {
            return metricValue;
        }

        public void setMetricValue(boolean metricValue) {
            this.metricValue = metricValue;
        }

        @SuppressWarnings("unchecked")
        public MetricInterval getEventDateProcessor() {
            if (eventDateProcessor == null) {
                if (eventDateProcessorClassName == null) {
                    return null;
                } else {
                    try {
                        Class<MetricInterval> cls = (Class<MetricInterval>) Class.forName(eventDateProcessorClassName);
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

        public void setEventDateProcessorClass(Class<? extends MetricInterval> eventDateProcessorClass) {
            this.eventDateProcessorClassName = eventDateProcessorClass.getName();
        }

    }
}
