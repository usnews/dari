package com.psddev.dari.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.UuidUtils;

public class CountRecord {

    private static final Logger LOGGER = LoggerFactory.getLogger(CountRecord.class);

    private static final String COUNTRECORD_TABLE = "CountRecord";
    private static final String COUNTRECORD_STRINGINDEX_TABLE = "CountRecordString";
    private static final String COUNTRECORD_DOUBLEINDEX_TABLE = "CountRecordDouble";
    private static final String COUNTRECORD_INTEGERINDEX_TABLE = "CountRecordInteger";
    private static final String COUNTRECORD_UUIDINDEX_TABLE = "CountRecordUuid";

    private final Map<String, Object> dimensions;
    private final String symbol;
    private final SqlDatabase db; /* TODO: get rid of this and move all this implementation specific stuff where it goes */

    private UUID id;

    private Long updateDate;
    private Long eventDate;
    private Boolean dimensionsSaved;

    private CountRecordQuery query;

    public CountRecord(Map<String, Object> dimensions) {
        this.dimensions = dimensions;
        this.symbol = this.getSymbol();
        this.db = Database.Static.getFirst(SqlDatabase.class);
        this.query = new CountRecordQuery(symbol, dimensions);
        //LOGGER.info("===== creating new instance of CountRecord: " + this.toString());
    }

    public String toString() {
        return "CountRecord: [" + this.getSymbol() + "] " + dimensions.toString();
    }

    public void setUpdateDate(long timestampSeconds) {
        this.updateDate = timestampSeconds;
    }

    public long getUpdateDate() {
        if (this.updateDate != null) {
            return this.updateDate;
        } else {
            return System.currentTimeMillis() / 1000L;
        }
    }

    // This method will strip the minutes and seconds off of a timestamp
    public void setEventDateHour(long timestampSeconds) {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.setTimeInMillis(timestampSeconds*1000);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        this.eventDate = c.getTimeInMillis()/1000L;
    }

    public long getEventDateHour() {
        if (this.eventDate == null) {
            setEventDateHour(System.currentTimeMillis() / 1000L);
        }
        return this.eventDate;
    }

    public void setQueryDateRange(long startTimestamp, long endTimestamp) {
        this.query.startTimestamp = startTimestamp;
        this.query.endTimestamp = endTimestamp;
    }

    public Integer getCount() throws SQLException {
        query.groupByDimensions = null;
        query.orderByDimensions = null;
        return Static.getCountByDimensions(db, query);
    }

    public Map<Map<String, Object>, Integer> getCountBy(Map<String, Object> groupByDimensions, String[] orderByDimensions) throws SQLException {
        query.groupByDimensions = groupByDimensions;
        query.orderByDimensions = orderByDimensions;
        return Static.getCountByDimensionsWithGroupBy(db, query);
    }

    public void adjustCount(Integer amount) throws SQLException {
        // find the ID, it might be null
        UUID id = getId();
        if (dimensionsSaved) {
            Static.doAdjustUpdateOrInsert(db, id, getSymbol(), dimensions, amount, getUpdateDate(), getEventDateHour());
        } else {
            Static.doInserts(db, id, getSymbol(), dimensions, amount, getUpdateDate(), getEventDateHour());
            dimensionsSaved = true;
        }
    }

    public void setCount(Integer amount) throws SQLException {
        // find the ID, it might be null
        UUID id = getId();
        if (dimensionsSaved) {
            Static.doSetUpdateOrInsert(db, id, getSymbol(), dimensions, amount, getUpdateDate(), getEventDateHour());
        } else {
            Static.doInserts(db, id, getSymbol(), dimensions, amount, getUpdateDate(), getEventDateHour());
            dimensionsSaved = true;
        }
    }

    public String getSymbol() {
        if (symbol != null) return symbol;
        StringBuilder symbolBuilder = new StringBuilder();
        //StringBuilder symbolBuilder = new StringBuilder(this.objectClass.getName());
        //symbolBuilder.append("/");

        Iterator<String> keysIterator = new TreeSet<String>(this.dimensions.keySet()).iterator();
        while (keysIterator.hasNext()) {
            String key = keysIterator.next();
            symbolBuilder.append(key);
            Object value = this.dimensions.get(key);
            if (value instanceof Set) {
                int numElements = ((Set<?>)value).size();
                if (numElements > 1) {
                    symbolBuilder.append("[");
                    symbolBuilder.append(numElements);
                    symbolBuilder.append("]");
                }
            }
            symbolBuilder.append(',');
        }
        symbolBuilder.setLength(symbolBuilder.length()-1);
        symbolBuilder.append("#count");
        return symbolBuilder.toString();
    }

    public UUID getId() throws SQLException {
        if (id == null) {
            id = Static.getIdByDimensions(db, query);
            if (id == null) {
                // create a new ID
                dimensionsSaved = false;
                id = UuidUtils.createSequentialUuid();
            } else {
                // this ID came from the DB
                dimensionsSaved = true;
            }
        }
        return id;
    }

    /** {@link CountRecord} utility methods. */
    private static final class Static {

        private Static() {
            
        }

        static String getSelectSql(SqlDatabase db, CountRecordQuery query, Boolean preciseMatch, Boolean selectAmount) {
            SqlVendor vendor = db.getVendor();
            StringBuilder selectBuilder = new StringBuilder("SELECT cr0.id");
            StringBuilder fromBuilder = new StringBuilder();
            StringBuilder whereBuilder = new StringBuilder();
            StringBuilder groupByBuilder = new StringBuilder();
            StringBuilder orderByBuilder = new StringBuilder();
            boolean joinCountRecordTable = false;
            int i = 0;
            int count = 1;
            String alias = "cr" + i;

            if (selectAmount)
                joinCountRecordTable = true;
            if (query.startTimestamp != null && query.endTimestamp != null)
                joinCountRecordTable = true;

            if (joinCountRecordTable) {
                if (selectAmount) {
                    selectBuilder.append(", ");
                    selectBuilder.append(alias);
                    selectBuilder.append(".");
                    selectBuilder.append("amount");
                }
                selectBuilder.append(", ");
                selectBuilder.append(alias);
                selectBuilder.append(".");
                selectBuilder.append("eventDate");

                fromBuilder.append(" \nFROM ");
                fromBuilder.append(COUNTRECORD_TABLE);
                fromBuilder.append(" ");
                fromBuilder.append(alias);
                whereBuilder.append(" \nWHERE ");
                if (preciseMatch) {
                    whereBuilder.append(alias);
                    whereBuilder.append(".");
                    whereBuilder.append("typeSymbolId");
                    whereBuilder.append(" = ");
                    whereBuilder.append(db.getSymbolId(query.symbol));
                } else {
                    whereBuilder.append("1 = 1");
                }
                if (query.startTimestamp != null && query.endTimestamp != null) {
                    whereBuilder.append(" AND ");
                    whereBuilder.append(alias);
                    whereBuilder.append(".");
                    whereBuilder.append("eventDate");
                    whereBuilder.append(" >= ");
                    vendor.appendValue(whereBuilder, query.startTimestamp);
                    whereBuilder.append(" AND ");
                    whereBuilder.append(alias);
                    whereBuilder.append(".");
                    whereBuilder.append("eventDate");
                    whereBuilder.append(" <= ");
                    vendor.appendValue(whereBuilder, query.endTimestamp);
                }
                ++i;
            }

            for (String table : Static.getIndexTables(query.dimensions, query.groupByDimensions)) {
                alias = "cr" + i;
                if (i == 0) {
                    fromBuilder.append(" \nFROM ");
                    fromBuilder.append(table);
                    fromBuilder.append(" ");
                    fromBuilder.append(alias);
                    whereBuilder.append(" \nWHERE ");
                    if (preciseMatch) {
                        whereBuilder.append(alias);
                        whereBuilder.append(".");
                        whereBuilder.append("typeSymbolId");
                        whereBuilder.append(" = ");
                        whereBuilder.append(db.getSymbolId(query.symbol));
                    } else {
                        whereBuilder.append("1 = 1");
                    }
                } else {
                    fromBuilder.append(" \nJOIN ");
                    fromBuilder.append(table);
                    fromBuilder.append(" ");
                    fromBuilder.append(alias);
                    fromBuilder.append(" ON (");
                    fromBuilder.append("cr0");
                    fromBuilder.append(".");
                    fromBuilder.append("typeSymbolId");
                    fromBuilder.append(" = ");
                    fromBuilder.append(alias);
                    fromBuilder.append(".");
                    fromBuilder.append("typeSymbolId");
                    fromBuilder.append(" AND ");
                    fromBuilder.append("cr0");
                    fromBuilder.append(".");
                    fromBuilder.append("id");
                    fromBuilder.append(" = ");
                    fromBuilder.append(alias);
                    fromBuilder.append(".");
                    fromBuilder.append("id");
                    fromBuilder.append(")");
                }

                int numFilters = 0;
                // append to where statement
                whereBuilder.append(" \nAND (");
                for (Map.Entry<String, Object> entry : Static.getDimensionsByIndexTable(table, query.dimensions).entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    whereBuilder.append("(");
                    whereBuilder.append(alias);
                    whereBuilder.append(".");
                    whereBuilder.append("symbolId");
                    whereBuilder.append(" = ");
                    whereBuilder.append(db.getSymbolId(getDimensionSymbol(key)));
                    whereBuilder.append(" AND ");
                    whereBuilder.append(alias);
                    whereBuilder.append(".");
                    whereBuilder.append("value");
                    whereBuilder.append(" IN (");
                    if (value instanceof Set) {
                        for (Object v : (Set<?>)value) {
                            vendor.appendValue(whereBuilder, v);
                            whereBuilder.append(',');
                            ++numFilters;
                        }
                        whereBuilder.setLength(whereBuilder.length()-1);
                    } else {
                        vendor.appendValue(whereBuilder, value);
                        ++numFilters;
                    }
                    whereBuilder.append("))");
                    whereBuilder.append(" \n  OR "); // 7 chars below
                }
                if (query.groupByDimensions != null) {
                    for (Map.Entry<String, Object> entry : getDimensionsByIndexTable(table, query.groupByDimensions).entrySet()) {
                        if (! query.dimensions.containsKey(entry.getKey())) {
                            whereBuilder.append("(");
                            whereBuilder.append(alias);
                            whereBuilder.append(".");
                            whereBuilder.append("symbolId");
                            whereBuilder.append(" = ");
                            whereBuilder.append(db.getSymbolId(getDimensionSymbol(entry.getKey())));
                            whereBuilder.append(")");
                            whereBuilder.append(" \n  OR "); // 7 chars below
                            ++numFilters;
                        }
                        selectBuilder.append(", MAX(IF(");
                        selectBuilder.append(alias);
                        selectBuilder.append(".symbolId = ");
                        selectBuilder.append(db.getSymbolId(getDimensionSymbol(entry.getKey())));
                        selectBuilder.append(", ");
                        selectBuilder.append(alias);
                        selectBuilder.append(".");
                        selectBuilder.append("value");
                        selectBuilder.append(", null)) AS ");
                        vendor.appendIdentifier(selectBuilder, entry.getKey());
                    }
                }
                whereBuilder.setLength(whereBuilder.length() - 7);
                whereBuilder.append(") ");
                count = count * numFilters;
                ++i;
            }

            groupByBuilder.append("\nGROUP BY ");
            groupByBuilder.append("cr0.id");
            orderByBuilder.append("\nORDER BY ");
            orderByBuilder.append("cr0.id");
            if (joinCountRecordTable) {
                orderByBuilder.append(", cr0.eventDate DESC");
                groupByBuilder.append(", cr0.eventDate");
                if (selectAmount) {
                    groupByBuilder.append(", cr0.amount");
                }
            }
            groupByBuilder.append(" HAVING COUNT(*) = ");
            groupByBuilder.append(count);
            return selectBuilder.toString() + " " + fromBuilder.toString() + " " + whereBuilder.toString() + " " + groupByBuilder.toString() + orderByBuilder.toString();
        }

        static String getSelectCountGroupBySql(SqlDatabase db, CountRecordQuery query) {
            StringBuilder selectBuilder = new StringBuilder();
            StringBuilder fromBuilder = new StringBuilder();
            StringBuilder groupBuilder = new StringBuilder();
            StringBuilder orderBuilder = new StringBuilder();
            selectBuilder.append("SELECT SUM(x.amount) AS amount");
            fromBuilder.append(" FROM (");
            fromBuilder.append(getSelectSql(db, query, false, true));
            fromBuilder.append(") x");
            // handle additional dimensions
            if (query.groupByDimensions != null) {
                groupBuilder.append(" GROUP BY ");
                for (String key : query.groupByDimensions.keySet()) {
                    // select additional dimensions
                    selectBuilder.append(", x.");
                    selectBuilder.append(key);
                    // group by additional dimensions
                    groupBuilder.append("x.");
                    groupBuilder.append(key);
                    groupBuilder.append(", ");
                }
                if (groupBuilder.toString().equals(" GROUP BY ")) {
                    groupBuilder.setLength(0);
                } else {
                    groupBuilder.setLength(groupBuilder.length() - 2);
                }
                if (query.orderByDimensions != null && groupBuilder.length() > 0) {
                    orderBuilder.append(" ORDER BY ");
                    for (String key : query.orderByDimensions) {
                        if (query.groupByDimensions.containsKey(key)) {
                            // order by additional dimensions
                            orderBuilder.append("x.");
                            orderBuilder.append(key);
                            orderBuilder.append(", ");
                        }
                    }
                    if (orderBuilder.toString().equals(" ORDER BY ")) {
                        orderBuilder.setLength(0);
                    } else {
                        orderBuilder.setLength(orderBuilder.length() - 2);
                    }
                }
            }
            return selectBuilder.toString() + fromBuilder.toString() + groupBuilder.toString() + orderBuilder.toString();
        }

        static String getSelectPreciseIdSql(SqlDatabase db, CountRecordQuery query) {
            return getSelectSql(db, query, true, false);
        }

        static String getSelectPreciseCountSql(SqlDatabase db, CountRecordQuery query) {
            return getSelectSql(db, query, true, true);
        }

        static String getSelectCountSql(SqlDatabase db, CountRecordQuery query) {
            return "SELECT SUM(x.amount) AS amount FROM (" + getSelectSql(db, query, false, true) + ") x";
        }

        static String getDimensionSymbol(String keyName) {
            /*
            StringBuilder symbolBuilder = new StringBuilder();
            //symbolBuilder.append("/");
            symbolBuilder.append(keyName.toLowerCase());
            return symbolBuilder.toString();
            */
            return keyName.toLowerCase();
        }

        static List<String> getInsertSqls(SqlDatabase db, UUID id, String symbol, Map<String, Object> dimensions, int amount, long createDate, long eventDateHour) {
            ArrayList<String> sqls = new ArrayList<String>();
            // insert countrecord
            sqls.add(getCountRecordInsertSql(db, id, symbol, amount, createDate, eventDateHour));
            // insert indexes
            for (Map.Entry<String, Object> entry : dimensions.entrySet()) {
                String key = entry.getKey();
                Object dimensionValue = entry.getValue();
                String table;
                TreeSet<Object> values = new TreeSet<Object>();
                if (dimensionValue instanceof Set) {
                    values.addAll((Set<?>)dimensionValue);
                } else {
                    values.add(dimensionValue);
                }
                table = Static.getIndexTable(values);
                for (Object value:values) {
                    if (value instanceof UUID) {
                        sqls.add(getDimensionInsertSql(db, id, symbol, key, (UUID) value, table));
                    } else if (value instanceof Double ) {
                        sqls.add(getDimensionInsertSql(db, id, symbol, key, (Double) value, table));
                    } else if (value instanceof Integer ) {
                        sqls.add(getDimensionInsertSql(db, id, symbol, key, (Integer) value, table));
                    } else {
                        sqls.add(getDimensionInsertSql(db, id, symbol, key, value.toString(), table));
                    }
                }
            }
            return sqls;
        }

        static String getCountRecordInsertSql(SqlDatabase db, UUID id, String symbol, int amount, long createDate, long eventDateHour) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            insertBuilder.append(COUNTRECORD_TABLE);
            insertBuilder.append(" (");
            insertBuilder.append("id, typeSymbolId, amount, createDate, updateDate, eventDate");
            insertBuilder.append(") VALUES (");
            vendor.appendValue(insertBuilder, id);
            insertBuilder.append(", ");
            insertBuilder.append(db.getSymbolId(symbol));
            insertBuilder.append(", ");
            insertBuilder.append(amount);
            insertBuilder.append(", ");
            insertBuilder.append(createDate);
            insertBuilder.append(", ");
            insertBuilder.append(createDate);
            insertBuilder.append(", ");
            insertBuilder.append(eventDateHour);
            insertBuilder.append(")");
            return insertBuilder.toString();
        }

        static String getDimensionInsertSql(SqlDatabase db, UUID id, String symbol, String key, Object value, String table) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            insertBuilder.append(table);
            insertBuilder.append(" (");
            insertBuilder.append("id, typeSymbolId, symbolId, value");
            insertBuilder.append(") VALUES (");
            vendor.appendValue(insertBuilder, id);
            insertBuilder.append(", ");
            insertBuilder.append(db.getSymbolId(symbol));
            insertBuilder.append(", ");
            insertBuilder.append(db.getSymbolId(Static.getDimensionSymbol(key)));
            insertBuilder.append(", ");
            vendor.appendValue(insertBuilder, value);
            insertBuilder.append(")");
            return insertBuilder.toString();
        }

        static String getUpdateSql(SqlDatabase db, UUID id, int amount, long updateDate, long eventDateHour, Boolean adjust) {
            StringBuilder updateBuilder = new StringBuilder("UPDATE ");
            SqlVendor vendor = db.getVendor();
            updateBuilder.append(COUNTRECORD_TABLE);
            if (adjust) {
                updateBuilder.append(" SET amount = amount + ");
            } else {
                updateBuilder.append(" SET amount = ");
            }
            updateBuilder.append(amount);
            updateBuilder.append(", updateDate = ");
            updateBuilder.append(updateDate);
            updateBuilder.append(" WHERE ");
            updateBuilder.append("id");
            updateBuilder.append(" = ");
            vendor.appendValue(updateBuilder, id);
            updateBuilder.append(" AND ");
            updateBuilder.append("eventDate");
            updateBuilder.append(" = ");
            vendor.appendValue(updateBuilder, eventDateHour);
            return updateBuilder.toString();
        }

        static String getIndexTable (Object value) {
            if (value instanceof Set) {
                // for the purpose of determining the index table, 
                // just check the type of the first element of the set
                value = ((Set<?>)value).iterator().next();
            }
            if (value instanceof UUID) {
                return COUNTRECORD_UUIDINDEX_TABLE;
            } else if (value instanceof Double ) {
                return COUNTRECORD_DOUBLEINDEX_TABLE;
            } else if (value instanceof Integer ) {
                return COUNTRECORD_INTEGERINDEX_TABLE;
            } else {
                return COUNTRECORD_STRINGINDEX_TABLE;
            }
        }

        static Set<String> getIndexTables(Map<String, Object> dimensions, Map<String, Object> additionalDimensions) {
            LinkedHashSet<String> tables = new LinkedHashSet<String>();
            for (Map.Entry<String, Object> entry : dimensions.entrySet()) {
                tables.add(getIndexTable(entry.getValue()));
            }
            if (additionalDimensions != null) {
                for (Map.Entry<String, Object> entry : additionalDimensions.entrySet()) {
                    tables.add(getIndexTable(entry.getValue()));
                }
            }
            return tables;
        }

        static Map<String, Object> getDimensionsByIndexTable(String table, Map<String, Object> dimensions) {
            HashMap<String, Object> dims = new HashMap<String, Object>();
            for (Map.Entry<String, Object> entry : dimensions.entrySet()) {
                if (table == getIndexTable(entry.getValue())) {
                    dims.put(entry.getKey(), entry.getValue());
                }
            }
            return dims;
        }

        static ResultSet doSelectSql(Connection connection, String sql) throws SQLException {
            ResultSet result = null;
            Statement statement;
            try {
                statement = connection.createStatement();
                result = statement.executeQuery(sql);
            } catch (SQLException ex) {
                LOGGER.error("SqlException: " + ex);
                throw ex;
            }
            return result;
        }

        static void doInserts(SqlDatabase db, UUID id, String symbol, Map<String, Object> dimensions, int amount, long updateDate, long eventDateHour) throws SQLException {
            Connection connection = db.openConnection();
            try {
                for (String sql : getInsertSqls(db, id, symbol, dimensions, amount, updateDate, eventDateHour)) {
                    SqlDatabase.Static.executeUpdateWithArray(connection, sql);
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doSetUpdateOrInsert(SqlDatabase db, UUID id, String symbol, Map<String, Object> dimensions, int amount, long updateDate, long eventDateHour) throws SQLException {
            Connection connection = db.openConnection();
            try {
                String sql = getUpdateSql(db, id, amount, updateDate, eventDateHour, false);
                int rowsAffected = SqlDatabase.Static.executeUpdateWithArray(connection, sql);
                if (rowsAffected == 0) {
                    sql = Static.getCountRecordInsertSql(db, id, symbol, amount, updateDate, eventDateHour);
                    SqlDatabase.Static.executeUpdateWithArray(connection, sql);
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doAdjustUpdateOrInsert(SqlDatabase db, UUID id, String symbol, Map<String, Object> dimensions, int adjustAmount, long updateDate, long eventDateHour) throws SQLException {
            Connection connection = db.openConnection();
            try {
                String sql = getUpdateSql(db, id, adjustAmount, updateDate, eventDateHour, true);
                int rowsAffected = SqlDatabase.Static.executeUpdateWithArray(connection, sql);
                if (rowsAffected == 0) {
                    sql = getCountRecordInsertSql(db, id, symbol, adjustAmount, updateDate, eventDateHour);
                    SqlDatabase.Static.executeUpdateWithArray(connection, sql);
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        static Integer getCountByDimensions(SqlDatabase db, CountRecordQuery query) throws SQLException {
            String sql = getSelectCountSql(db, query);
            Connection connection = db.openConnection();
            Integer count = 0;
            try {
                ResultSet result = doSelectSql(connection, sql);
                if (result.next()) {
                    count = result.getInt(1);
                }
                result.close();
                return count;
            } finally {
                db.closeConnection(connection);
            }
        }

        static Map<Map<String, Object>, Integer> getCountByDimensionsWithGroupBy(SqlDatabase db, CountRecordQuery query) throws SQLException {
            String sql = getSelectCountGroupBySql(db, query);
            Connection connection = db.openConnection();
            LinkedHashMap<Map<String, Object>, Integer> results = new LinkedHashMap<Map<String, Object>, Integer>();
            try {
                ResultSet result = doSelectSql(connection, sql);
                ResultSetMetaData meta = result.getMetaData();
                int numColumns = meta.getColumnCount();
                while (result.next()) {
                    HashMap<String, Object> dims = new HashMap<String, Object>();
                    Integer count = result.getInt(1);
                    for (int i = 2; i <= numColumns; i++) {
                        String columnName = meta.getColumnLabel(i);
                        dims.put(columnName, result.getObject(i));
                    }
                    results.put(dims, count);
                }
                result.close();
                return results;
            } finally {
                db.closeConnection(connection);
            }
        }

        static UUID getIdByDimensions(SqlDatabase db, CountRecordQuery query) throws SQLException {
            UUID id = null;
            // find the ID, it might be null
            String sql = Static.getSelectPreciseIdSql(db, query);
            Connection connection = db.openConnection();
            try {
                ResultSet result = doSelectSql(connection, sql);
                if (result.next()) {
                    id = UuidUtils.fromBytes(result.getBytes(1));
                    //LOGGER.info(this.id.toString());
                }
                result.close();
            } finally {
                db.closeConnection(connection);
            }
            return id;
        }

    }

}

class CountRecordQuery {
    public final String symbol;
    public final Map<String, Object> dimensions;
    public Long startTimestamp;
    public Long endTimestamp;
    public Map<String, Object> groupByDimensions;
    public String[] orderByDimensions;

    public CountRecordQuery(String symbol, Map<String, Object> dimensions) {
        this.symbol = symbol;
        this.dimensions = dimensions;
    }
}
