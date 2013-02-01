package com.psddev.dari.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.UuidUtils;

public class CountRecord {

    private static final Logger LOGGER = LoggerFactory.getLogger(CountRecord.class);

    static final String COUNTRECORD_TABLE = "CountRecord";
    static final String COUNTRECORD_STRINGINDEX_TABLE = "CountRecordString";
    static final String COUNTRECORD_DOUBLEINDEX_TABLE = "CountRecordDouble";
    static final String COUNTRECORD_INTEGERINDEX_TABLE = "CountRecordInteger";
    static final String COUNTRECORD_UUIDINDEX_TABLE = "CountRecordUuid";

    private final DimensionSet dimensions;
    private final String typeSymbol;
    private final SqlDatabase db; /* TODO: get rid of this and move all this implementation specific stuff where it goes */
    private final CountRecordQuery query;

    private UUID id;

    private Long updateDate;
    private Long eventDate;
    private Boolean dimensionsSaved;

    public CountRecord(String actionSymbol, Map<String, Object> dimensions) {
        this.dimensions = DimensionSet.createDimensionSet(dimensions);
        this.typeSymbol = this.getTypeSymbol(); // requires this.dimensions
        this.db = Database.Static.getFirst(SqlDatabase.class);
        this.query = new CountRecordQuery(typeSymbol, actionSymbol, this.dimensions);
        //LOGGER.info("===== creating new instance of CountRecord: " + this.toString());
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
        return Static.getCountByDimensions(db, query);
    }

    public Map<String, Integer> getCountBy(Map<String, Object> groupByDimensions, String[] orderByDimensions) throws SQLException {
        query.groupByDimensions = DimensionSet.createDimensionSet(groupByDimensions);
        query.orderByDimensions = orderByDimensions;
        return Static.getCountByDimensionsWithGroupBy(db, query);
    }

    public void adjustCount(Integer amount) throws SQLException {
        // find the ID, it might be null
        UUID id = getId();
        if (dimensionsSaved) {
            Static.doAdjustUpdateOrInsert(db, id, getActionSymbol(), getTypeSymbol(), amount, getUpdateDate(), getEventDateHour());
        } else {
            Static.doInserts(db, id, getActionSymbol(), getTypeSymbol(), dimensions, amount, getUpdateDate(), getEventDateHour());
            dimensionsSaved = true;
        }
    }

    public void setCount(Integer amount) throws SQLException {
        // find the ID, it might be null
        UUID id = getId();
        if (dimensionsSaved) {
            Static.doSetUpdateOrInsert(db, id, getActionSymbol(), getTypeSymbol(), amount, getUpdateDate(), getEventDateHour());
        } else {
            Static.doInserts(db, id, getActionSymbol(), getTypeSymbol(), dimensions, amount, getUpdateDate(), getEventDateHour());
            dimensionsSaved = true;
        }
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

    public String getTypeSymbol() {
        if (this.typeSymbol != null) {
            return typeSymbol;
        } else {
            return this.dimensions.getSymbol();
        }
    }

    public String getActionSymbol() {
        return this.query.actionSymbol;
    }

    /** {@link CountRecord} utility methods. */
    private static final class Static {

        private Static() { }

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
                whereBuilder.append(alias);
                whereBuilder.append(".");
                whereBuilder.append("actionSymbolId");
                whereBuilder.append(" = ");
                whereBuilder.append(db.getSymbolId(query.actionSymbol));
                if (preciseMatch) {
                    whereBuilder.append(" AND ");
                    whereBuilder.append(alias);
                    whereBuilder.append(".");
                    whereBuilder.append("typeSymbolId");
                    whereBuilder.append(" = ");
                    whereBuilder.append(db.getSymbolId(query.symbol));
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
                for (Dimension dimension : Static.getDimensionsByIndexTable(table, query.dimensions)) {
                    Set<Object> values = dimension.getValues();
                    whereBuilder.append("(");
                    whereBuilder.append(alias);
                    whereBuilder.append(".");
                    whereBuilder.append("symbolId");
                    whereBuilder.append(" = ");
                    whereBuilder.append(db.getSymbolId(dimension.getSymbol()));
                    whereBuilder.append(" AND ");
                    whereBuilder.append(alias);
                    whereBuilder.append(".");
                    whereBuilder.append("value");
                    whereBuilder.append(" IN (");
                    for (Object v : values) {
                        vendor.appendValue(whereBuilder, v);
                        whereBuilder.append(',');
                        ++numFilters;
                    }
                    whereBuilder.setLength(whereBuilder.length()-1);
                    whereBuilder.append("))");
                    whereBuilder.append(" \n  OR "); // 7 chars below
                }
                if (query.groupByDimensions != null) {
                    for (Dimension dimension : getDimensionsByIndexTable(table, query.groupByDimensions)) {
                        if (! query.dimensions.keySet().contains(dimension.getKey())) {
                            whereBuilder.append("(");
                            whereBuilder.append(alias);
                            whereBuilder.append(".");
                            whereBuilder.append("symbolId");
                            whereBuilder.append(" = ");
                            whereBuilder.append(db.getSymbolId(dimension.getSymbol()));
                            whereBuilder.append(")");
                            whereBuilder.append(" \n  OR "); // 7 chars below
                            ++numFilters;
                        }
                        selectBuilder.append(", MAX(IF(");
                        selectBuilder.append(alias);
                        selectBuilder.append(".symbolId = ");
                        selectBuilder.append(db.getSymbolId(dimension.getSymbol()));
                        selectBuilder.append(", ");
                        selectBuilder.append(alias);
                        selectBuilder.append(".");
                        selectBuilder.append("value");
                        selectBuilder.append(", null)) AS ");
                        vendor.appendIdentifier(selectBuilder, dimension.getKey());
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
                        if (query.groupByDimensions.keySet().contains(key)) {
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

        static List<String> getInsertSqls(SqlDatabase db, UUID id, String actionSymbol, String typeSymbol, DimensionSet dimensions, int amount, long createDate, long eventDateHour) {
            ArrayList<String> sqls = new ArrayList<String>();
            // insert countrecord
            sqls.add(getCountRecordInsertSql(db, id, actionSymbol, typeSymbol, amount, createDate, eventDateHour));
            // insert indexes
            for (Dimension dimension : dimensions) {
                Set<Object> values = dimension.getValues();
                String table = dimension.getIndexTable();
                for (Object value:values) {
                    sqls.add(getDimensionInsertRowSql(db, id, typeSymbol, dimension, value, table));
                }
            }
            return sqls;
        }

        static String getCountRecordInsertSql(SqlDatabase db, UUID id, String actionSymbol, String typeSymbol, int amount, long createDate, long eventDateHour) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            insertBuilder.append(COUNTRECORD_TABLE);
            insertBuilder.append(" (");
            insertBuilder.append("id, actionSymbolId, typeSymbolId, amount, createDate, updateDate, eventDate");
            insertBuilder.append(") VALUES (");
            vendor.appendValue(insertBuilder, id);
            insertBuilder.append(", ");
            insertBuilder.append(db.getSymbolId(actionSymbol));
            insertBuilder.append(", ");
            insertBuilder.append(db.getSymbolId(typeSymbol));
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

        static String getDimensionInsertRowSql(SqlDatabase db, UUID id, String typeSymbol, Dimension dimension, Object value, String table) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            insertBuilder.append(table);
            insertBuilder.append(" (");
            insertBuilder.append("id, typeSymbolId, symbolId, value");
            insertBuilder.append(") VALUES (");
            vendor.appendValue(insertBuilder, id);
            insertBuilder.append(", ");
            insertBuilder.append(db.getSymbolId(typeSymbol));
            insertBuilder.append(", ");
            insertBuilder.append(db.getSymbolId(dimension.getSymbol()));
            insertBuilder.append(", ");
            vendor.appendValue(insertBuilder, value);
            insertBuilder.append(")");
            return insertBuilder.toString();
        }

        static String getUpdateSql(SqlDatabase db, UUID id, String actionSymbol, int amount, long updateDate, long eventDateHour, Boolean adjust) {
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
            updateBuilder.append("actionSymbolId");
            updateBuilder.append(" = ");
            vendor.appendValue(updateBuilder, db.getSymbolId(actionSymbol));
            updateBuilder.append(" AND ");
            updateBuilder.append("eventDate");
            updateBuilder.append(" = ");
            vendor.appendValue(updateBuilder, eventDateHour);
            return updateBuilder.toString();
        }

        static Set<String> getIndexTables(DimensionSet ... dimensionSets) {
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

        static DimensionSet getDimensionsByIndexTable(String table, DimensionSet dimensions) {
            //HashMap<String, Object> dims = new HashMap<String, Object>();
            DimensionSet dims = new DimensionSet();
            for (Dimension dimension: dimensions) {
                if (table == dimension.getIndexTable()) {
                    dims.add(dimension);
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

        static void doInserts(SqlDatabase db, UUID id, String actionSymbol, String typeSymbol, DimensionSet dimensions, int amount, long updateDate, long eventDateHour) throws SQLException {
            Connection connection = db.openConnection();
            try {
                for (String sql : getInsertSqls(db, id, actionSymbol, typeSymbol, dimensions, amount, updateDate, eventDateHour)) {
                    SqlDatabase.Static.executeUpdateWithArray(connection, sql);
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doSetUpdateOrInsert(SqlDatabase db, UUID id, String actionSymbol, String typeSymbol, int amount, long updateDate, long eventDateHour) throws SQLException {
            Connection connection = db.openConnection();
            try {
                String sql = getUpdateSql(db, id, actionSymbol, amount, updateDate, eventDateHour, false);
                int rowsAffected = SqlDatabase.Static.executeUpdateWithArray(connection, sql);
                if (rowsAffected == 0) {
                    sql = Static.getCountRecordInsertSql(db, id, actionSymbol, typeSymbol, amount, updateDate, eventDateHour);
                    SqlDatabase.Static.executeUpdateWithArray(connection, sql);
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doAdjustUpdateOrInsert(SqlDatabase db, UUID id, String actionSymbol, String typeSymbol, int adjustAmount, long updateDate, long eventDateHour) throws SQLException {
            Connection connection = db.openConnection();
            try {
                String sql = getUpdateSql(db, id, actionSymbol, adjustAmount, updateDate, eventDateHour, true);
                int rowsAffected = SqlDatabase.Static.executeUpdateWithArray(connection, sql);
                if (rowsAffected == 0) {
                    sql = getCountRecordInsertSql(db, id, actionSymbol, typeSymbol, adjustAmount, updateDate, eventDateHour);
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

        static Map<String, Integer> getCountByDimensionsWithGroupBy(SqlDatabase db, CountRecordQuery query) throws SQLException {
            String sql = getSelectCountGroupBySql(db, query);
            Connection connection = db.openConnection();
            LinkedHashMap<String, Integer> results = new LinkedHashMap<String, Integer>();
            try {
                ResultSet result = doSelectSql(connection, sql);
                ResultSetMetaData meta = result.getMetaData();
                int numColumns = meta.getColumnCount();
                while (result.next()) {
                    HashMap<String, Object> dims = new HashMap<String, Object>();
                    Integer count = result.getInt(1);
                    for (int i = 2; i <= numColumns; i++) {
                        String key = meta.getColumnLabel(i);
                        Object value = result.getObject(i);
                        dims.put(key, value);
                    }
                    // TODO: obviously this is ridiculous
                    results.put(DimensionSet.createDimensionSet(dims).toString(), count);
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
    public final String actionSymbol;
    public final DimensionSet dimensions;
    public Long startTimestamp;
    public Long endTimestamp;
    public DimensionSet groupByDimensions;
    public String[] orderByDimensions;

    public CountRecordQuery(String symbol, String actionSymbol, DimensionSet dimensions) {
        this.symbol = symbol;
        this.actionSymbol = actionSymbol;
        this.dimensions = dimensions;
    }
}

class Dimension implements Comparable<Dimension> {

    private final String key;
    private Set<Object> values = new HashSet<Object>();
    private Class<?> type;

    public Dimension(String key) {
        this.key = key;
    }

    public String getSymbol() {
        return key.toLowerCase();
    }

    public String getKey() {
        return key;
    }

    public Set<Object> getValues() {
        return values;
    }

    public void addValue(UUID value) {
        this.type = UUID.class;
        this.values.add(value);
    }

    public void addValue(String value) {
        this.type = String.class;
        this.values.add(value);
    }

    public void addValue(Integer value) {
        this.type = Integer.class;
        this.values.add(value);
    }

    public void addValue(Double value) {
        this.type = Double.class;
        this.values.add(value);
    }

    public void addValue(Object value) {
        this.type = String.class;
        this.values.add(value.toString());
    }

    public Class<?> getType() {
        return type;
    }

    public String getIndexTable () {
        if (getType() == UUID.class) {
            return CountRecord.COUNTRECORD_UUIDINDEX_TABLE;
        } else if (getType() == Double.class) {
            return CountRecord.COUNTRECORD_DOUBLEINDEX_TABLE;
        } else if (getType() == Integer.class ) {
            return CountRecord.COUNTRECORD_INTEGERINDEX_TABLE;
        } else {
            return CountRecord.COUNTRECORD_STRINGINDEX_TABLE;
        }
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

    public static DimensionSet createDimensionSet(Map<String, Object> dimensions) {
        LinkedHashSet<Dimension> dimensionSet = new LinkedHashSet<Dimension>();
        for (Map.Entry<String, Object> entry : dimensions.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            LinkedHashSet<Object> values = new LinkedHashSet<Object>();
            if (value instanceof Set) {
                values.addAll((Set<?>)value);
            } else {
                values.add(value);
            }
            Dimension dim = new Dimension(key);
            for (Object val : values) {
                if (val instanceof UUID) {
                    dim.addValue((UUID) val);
                } else if (value instanceof Double) {
                    dim.addValue((Double) val);
                } else if (value instanceof Integer) {
                    dim.addValue((Integer) val);
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

        for (Dimension d : getSortedDimensions()) {
            symbolBuilder.append(d.getSymbol());
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
        symbolBuilder.append("#count");
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

