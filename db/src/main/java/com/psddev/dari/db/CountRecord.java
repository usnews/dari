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

public class CountRecord {

    //private static final Logger LOGGER = LoggerFactory.getLogger(CountRecord.class);

    static final String COUNTRECORD_TABLE = "CountRecord";
    static final String COUNTRECORD_STRINGINDEX_TABLE = "CountRecordString";
    static final String COUNTRECORD_DOUBLEINDEX_TABLE = "CountRecordDouble";
    static final String COUNTRECORD_INTEGERINDEX_TABLE = "CountRecordInteger";
    static final String COUNTRECORD_UUIDINDEX_TABLE = "CountRecordUuid";
    static final int QUERY_TIMEOUT = 3;

    private final DimensionSet dimensions;
    private final String typeSymbol;
    private SqlDatabase db; /* TODO: get rid of this and move all this implementation specific stuff where it goes */
    private final CountRecordQuery query;

    private UUID id;
    private UUID summaryRecordId;

    private Long updateDate;
    private Long eventDate;
    private Boolean dimensionsSaved;
    private ObjectField countField;

    public CountRecord(SqlDatabase database, String actionSymbol, Map<String, Object> dimensions) {
        this.dimensions = DimensionSet.createDimensionSet(dimensions);
        this.typeSymbol = this.getTypeSymbol(); // requires this.dimensions
        this.db = database; 
        this.query = new CountRecordQuery(typeSymbol, actionSymbol, this.dimensions);
    }

    public CountRecord(String actionSymbol, Map<String, Object> dimensions) {
        this(Database.Static.getFirst(SqlDatabase.class), actionSymbol, dimensions);
    }

    public void setSummaryField(ObjectField countField, UUID summaryRecordId) {
        this.countField = countField;
        this.summaryRecordId = summaryRecordId;
    }

    public SqlDatabase getDatabase() {
        return db;
    }

    public CountRecordQuery getQuery() {
        return query;
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
        getQuery().setDateRange(startTimestamp, endTimestamp);
    }

    public Integer getCount() throws SQLException {
        return Static.getCountByDimensions(getDatabase(), getQuery());
    }

    public Map<String, Integer> getCountBy(Map<String, Object> groupByDimensions, String[] orderByDimensions) throws SQLException {
        getQuery().setGroupByDimensions(DimensionSet.createDimensionSet(groupByDimensions));
        getQuery().setOrderByDimensions(orderByDimensions);
        return Static.getCountByDimensionsWithGroupBy(getDatabase(), getQuery());
    }

    public void incrementCount(Integer amount) throws SQLException {
        // find the ID, it might be null
        UUID id = getId();
        if (dimensionsSaved) {
            Static.doIncrementUpdateOrInsert(getDatabase(), id, getQuery().getActionSymbol(),
                    getTypeSymbol(), amount, getUpdateDate(),
                    getEventDateHour());
        } else {
            Static.doInserts(getDatabase(), id, getQuery().getActionSymbol(), getTypeSymbol(),
                    dimensions, amount, getUpdateDate(), getEventDateHour());
            dimensionsSaved = true;
        }
        if (isSummaryPossible()) {
            Static.doIncrementCountSummaryUpdateOrInsert(getDatabase(), amount, this.countField, this.summaryRecordId);
        }
    }

    /** This only needs to be executed if the summary has fallen out of sync due to data model change or some other operation */
    public void syncCountSummary() throws SQLException {
        if (isSummaryPossible()) {
            Static.doSetCountSummaryUpdateOrInsert(getDatabase(), getCount(), this.countField, this.summaryRecordId);
        }
    }

    private boolean isSummaryPossible() {
        SqlDatabase.FieldData fieldData = countField.as(SqlDatabase.FieldData.class);
        String indexTable = fieldData.getIndexTable();
        //Boolean isReadOnly = fieldData.isIndexTableReadOnly();
        Boolean isSource = fieldData.isIndexTableSource();
        if (indexTable != null &&
                /*isReadOnly &&*/
                isSource) {
            return true;
        } else {
            return false;
        }
    }

    public UUID getId() throws SQLException {
        if (id == null) {
            id = Static.getIdByDimensions(getDatabase(), getQuery());
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

    /** {@link CountRecord} utility methods. */
    private static final class Static {

        private Static() {
        }

        static String getSelectSql(SqlDatabase db, CountRecordQuery query,
                boolean preciseMatch, boolean selectAmount) {
            SqlVendor vendor = db.getVendor();
            StringBuilder selectBuilder = new StringBuilder("SELECT ");
            StringBuilder fromBuilder = new StringBuilder();
            StringBuilder whereBuilder = new StringBuilder();
            StringBuilder groupByBuilder = new StringBuilder();
            StringBuilder orderByBuilder = new StringBuilder();
            boolean joinCountRecordTable = false;
            int i = 0;
            int count = 1;
            String alias = "cr" + i;

            vendor.appendIdentifier(selectBuilder, alias);
            selectBuilder.append(".");
            vendor.appendIdentifier(selectBuilder, "id");

            if (selectAmount)
                joinCountRecordTable = true;

            if (joinCountRecordTable) {
                if (selectAmount) {
                    selectBuilder.append(", ");
                    vendor.appendIdentifier(selectBuilder, alias);
                    selectBuilder.append(".");
                    vendor.appendIdentifier(selectBuilder, "amount");
                }
                selectBuilder.append(", ");
                vendor.appendIdentifier(selectBuilder, alias);
                selectBuilder.append(".");
                vendor.appendIdentifier(selectBuilder, "eventDate");

                fromBuilder.append(" \nFROM ");
                vendor.appendIdentifier(fromBuilder, COUNTRECORD_TABLE);
                fromBuilder.append(" ");
                vendor.appendIdentifier(fromBuilder, alias);
                whereBuilder.append(" \nWHERE ");
                vendor.appendIdentifier(whereBuilder, alias);
                whereBuilder.append(".");
                vendor.appendIdentifier(whereBuilder, "actionSymbolId");
                whereBuilder.append(" = ");
                whereBuilder.append(db.getSymbolId(query.getActionSymbol()));
                if (preciseMatch) {
                    whereBuilder.append(" AND ");
                    vendor.appendIdentifier(whereBuilder, alias);
                    whereBuilder.append(".");
                    vendor.appendIdentifier(whereBuilder, "typeSymbolId");
                    whereBuilder.append(" = ");
                    whereBuilder.append(db.getSymbolId(query.getSymbol()));
                }
                if (query.getStartTimestamp() != null && query.getEndTimestamp() != null) {
                    whereBuilder.append(" AND ");
                    vendor.appendIdentifier(whereBuilder, alias);
                    whereBuilder.append(".");
                    vendor.appendIdentifier(whereBuilder, "eventDate");
                    whereBuilder.append(" >= ");
                    vendor.appendValue(whereBuilder, query.getStartTimestamp());
                    whereBuilder.append(" AND ");
                    vendor.appendIdentifier(whereBuilder, alias);
                    whereBuilder.append(".");
                    vendor.appendIdentifier(whereBuilder, "eventDate");
                    whereBuilder.append(" <= ");
                    vendor.appendValue(whereBuilder, query.getEndTimestamp());
                }
                ++i;
            }

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
                        vendor.appendIdentifier(whereBuilder, "typeSymbolId");
                        whereBuilder.append(" = ");
                        whereBuilder.append(db.getSymbolId(query.getSymbol()));
                    } else {
                        whereBuilder.append("1 = 1");
                    }
                } else {
                    fromBuilder.append(" \nJOIN ");
                    vendor.appendIdentifier(fromBuilder, table);
                    fromBuilder.append(" ");
                    vendor.appendIdentifier(fromBuilder, alias);
                    fromBuilder.append(" ON (");
                    vendor.appendIdentifier(fromBuilder, "cr0");
                    fromBuilder.append(".");
                    vendor.appendIdentifier(fromBuilder, "typeSymbolId");
                    fromBuilder.append(" = ");
                    vendor.appendIdentifier(fromBuilder, alias);
                    fromBuilder.append(".");
                    vendor.appendIdentifier(fromBuilder, "typeSymbolId");
                    fromBuilder.append(" AND ");
                    vendor.appendIdentifier(fromBuilder, "cr0");
                    fromBuilder.append(".");
                    vendor.appendIdentifier(fromBuilder, "id");
                    fromBuilder.append(" = ");
                    vendor.appendIdentifier(fromBuilder, alias);
                    fromBuilder.append(".");
                    vendor.appendIdentifier(fromBuilder, "id");
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
                if (query.getGroupByDimensions() != null) {
                    for (Dimension dimension : getDimensionsByIndexTable(table,
                            query.getGroupByDimensions())) {
                        if (!query.getDimensions().keySet().contains(
                                dimension.getKey())) {
                            whereBuilder.append("(");
                            vendor.appendIdentifier(whereBuilder, alias);
                            whereBuilder.append(".");
                            vendor.appendIdentifier(whereBuilder, "symbolId");
                            whereBuilder.append(" = ");
                            whereBuilder.append(db.getSymbolId(dimension
                                    .getSymbol()));
                            whereBuilder.append(")");
                            whereBuilder.append(" \n  OR "); // 7 chars below
                            ++numFilters;
                        }
                        selectBuilder.append(", MAX(IF(");
                        vendor.appendIdentifier(selectBuilder, alias);
                        selectBuilder.append(".");
                        vendor.appendIdentifier(selectBuilder, "symbolId");
                        selectBuilder.append(" = ");
                        selectBuilder.append(db.getSymbolId(dimension.getSymbol()));
                        selectBuilder.append(", ");
                        vendor.appendIdentifier(selectBuilder, alias);
                        selectBuilder.append(".");
                        vendor.appendIdentifier(selectBuilder, "value");
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
            vendor.appendIdentifier(groupByBuilder, "cr0");
            groupByBuilder.append(".");
            vendor.appendIdentifier(groupByBuilder, "id");
            orderByBuilder.append("\nORDER BY ");
            vendor.appendIdentifier(orderByBuilder, "cr0");
            orderByBuilder.append(".");
            vendor.appendIdentifier(orderByBuilder, "id");
            if (joinCountRecordTable) {
                orderByBuilder.append(", ");
                vendor.appendIdentifier(orderByBuilder, "cr0");
                orderByBuilder.append(".");
                vendor.appendIdentifier(orderByBuilder, "eventDate");
                orderByBuilder.append(" DESC");
                groupByBuilder.append(", ");
                vendor.appendIdentifier(groupByBuilder, "cr0");
                groupByBuilder.append(".");
                vendor.appendIdentifier(groupByBuilder, "eventDate");
                if (selectAmount) {
                    groupByBuilder.append(", ");
                    vendor.appendIdentifier(groupByBuilder, "cr0");
                    groupByBuilder.append(".");
                    vendor.appendIdentifier(groupByBuilder, "amount");
                }
            }
            groupByBuilder.append(" HAVING COUNT(*) = ");
            groupByBuilder.append(count);
            return selectBuilder.toString() + " " + fromBuilder.toString()
                    + " " + whereBuilder.toString() + " "
                    + groupByBuilder.toString() + orderByBuilder.toString();
        }

        static String getSelectCountGroupBySql(SqlDatabase db,
                CountRecordQuery query) {
            StringBuilder selectBuilder = new StringBuilder();
            StringBuilder fromBuilder = new StringBuilder();
            StringBuilder groupBuilder = new StringBuilder();
            StringBuilder orderBuilder = new StringBuilder();
            SqlVendor vendor = db.getVendor();
            selectBuilder.append("SELECT SUM(");
            vendor.appendIdentifier(selectBuilder, "x");
            selectBuilder.append(".");
            vendor.appendIdentifier(selectBuilder, "amount");
            selectBuilder.append(") AS ");
            vendor.appendIdentifier(selectBuilder, "amount");
            fromBuilder.append(" FROM (");
            fromBuilder.append(getSelectSql(db, query, false, true));
            fromBuilder.append(") x");
            // handle additional dimensions
            if (query.getGroupByDimensions() != null) {
                groupBuilder.append(" GROUP BY ");
                for (String key : query.getGroupByDimensions().keySet()) {
                    // select additional dimensions
                    selectBuilder.append(", ");
                    vendor.appendIdentifier(selectBuilder, "x");
                    selectBuilder.append(".");
                    vendor.appendIdentifier(selectBuilder, key);
                    // group by additional dimensions
                    vendor.appendIdentifier(groupBuilder, "x");
                    groupBuilder.append(".");
                    vendor.appendIdentifier(groupBuilder, key);
                    groupBuilder.append(", ");
                }
                if (groupBuilder.toString().equals(" GROUP BY ")) {
                    groupBuilder.setLength(0);
                } else {
                    groupBuilder.setLength(groupBuilder.length() - 2);
                }
                if (query.getOrderByDimensions() != null
                        && groupBuilder.length() > 0) {
                    orderBuilder.append(" ORDER BY ");
                    for (String key : query.getOrderByDimensions()) {
                        if (query.getGroupByDimensions().keySet().contains(key)) {
                            // order by additional dimensions
                            vendor.appendIdentifier(orderBuilder, "x");
                            orderBuilder.append(".");
                            vendor.appendIdentifier(orderBuilder, key);
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
            return selectBuilder.toString() + fromBuilder.toString()
                    + groupBuilder.toString() + orderBuilder.toString();
        }

        static String getSelectPreciseIdSql(SqlDatabase db,
                CountRecordQuery query) {
            return getSelectSql(db, query, true, false);
        }

        /*
        static String getSelectPreciseCountSql(SqlDatabase db,
                CountRecordQuery query) {
            return getSelectSql(db, query, true, true);
        }
        */

        static String getSelectCountSql(SqlDatabase db, CountRecordQuery query) {
            StringBuilder selectBuilder = new StringBuilder();
            StringBuilder fromBuilder = new StringBuilder();
            SqlVendor vendor = db.getVendor();
            selectBuilder.append("SELECT SUM(");
            vendor.appendIdentifier(selectBuilder, "x");
            selectBuilder.append(".");
            vendor.appendIdentifier(selectBuilder, "amount");
            selectBuilder.append(") AS ");
            vendor.appendIdentifier(selectBuilder, "amount");
            fromBuilder.append(" FROM (");
            fromBuilder.append(getSelectSql(db, query, false, true));
            fromBuilder.append(") x");
            return selectBuilder.toString() + fromBuilder.toString();
        }

        static List<String> getInsertSqls(SqlDatabase db, List<List<Object>> parametersList, UUID id,
                String actionSymbol, String typeSymbol,
                DimensionSet dimensions, int amount, long createDate,
                long eventDateHour) {
            ArrayList<String> sqls = new ArrayList<String>();
            // insert countrecord
            List<Object> parameters = new ArrayList<Object>();
            sqls.add(getCountRecordInsertSql(db, parameters, id, actionSymbol, typeSymbol,
                    amount, createDate, eventDateHour));
            parametersList.add(parameters);
            // insert indexes
            for (Dimension dimension : dimensions) {
                Set<Object> values = dimension.getValues();
                String table = dimension.getIndexTable();
                for (Object value : values) {
                    parameters = new ArrayList<Object>();
                    sqls.add(getDimensionInsertRowSql(db, parameters, id, typeSymbol,
                            dimension, value, table));
                    parametersList.add(parameters);
                }
            }
            return sqls;
        }

        static String getCountRecordInsertSql(SqlDatabase db, List<Object> parameters, UUID id,
                String actionSymbol, String typeSymbol, int amount,
                long createDate, long eventDateHour) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            vendor.appendIdentifier(insertBuilder, COUNTRECORD_TABLE);
            insertBuilder.append(" (");
            LinkedHashMap<String, Object> cols = new LinkedHashMap<String, Object>();
            cols.put("id", id);
            cols.put("actionSymbolId", db.getSymbolId(actionSymbol));
            cols.put("typeSymbolId", db.getSymbolId(typeSymbol));
            cols.put("amount", amount);
            cols.put("createDate", createDate);
            cols.put("updateDate", createDate);
            cols.put("eventDate", eventDateHour);
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

        static String getDimensionInsertRowSql(SqlDatabase db, List<Object> parameters, UUID id,
                String typeSymbol, Dimension dimension, Object value,
                String table) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            vendor.appendIdentifier(insertBuilder, table);
            insertBuilder.append(" (");
            LinkedHashMap<String, Object> cols = new LinkedHashMap<String, Object>();
            cols.put("id", id);
            cols.put("typeSymbolId", db.getSymbolId(typeSymbol));
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

        static String getUpdateSql(SqlDatabase db, List<Object> parameters, UUID id,
                String actionSymbol, int amount, long updateDate,
                long eventDateHour, boolean increment) {
            StringBuilder updateBuilder = new StringBuilder("UPDATE ");
            SqlVendor vendor = db.getVendor();
            vendor.appendIdentifier(updateBuilder, COUNTRECORD_TABLE);
            updateBuilder.append(" SET ");
            vendor.appendIdentifier(updateBuilder, "amount");
            updateBuilder.append(" = ");
            if (increment) {
                vendor.appendIdentifier(updateBuilder, "amount");
                updateBuilder.append(" + ");
            }
            vendor.appendBindValue(updateBuilder, amount, parameters);
            updateBuilder.append(", ");
            vendor.appendIdentifier(updateBuilder, "updateDate");
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, updateDate, parameters);
            updateBuilder.append(" WHERE ");
            vendor.appendIdentifier(updateBuilder, "id");
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, id, parameters);
            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, "actionSymbolId");
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, db.getSymbolId(actionSymbol), parameters);
            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, "eventDate");
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, eventDateHour, parameters);
            return updateBuilder.toString();
        }

        static Set<String> getIndexTables(DimensionSet... dimensionSets) {
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

        static DimensionSet getDimensionsByIndexTable(String table,
                DimensionSet dimensions) {
            //HashMap<String, Object> dims = new HashMap<String, Object>();
            DimensionSet dims = new DimensionSet();
            for (Dimension dimension : dimensions) {
                if (table.equals(dimension.getIndexTable())) {
                    dims.add(dimension);
                }
            }
            return dims;
        }

        static void doInserts(SqlDatabase db, UUID id, String actionSymbol,
                String typeSymbol, DimensionSet dimensions, int amount,
                long updateDate, long eventDateHour) throws SQLException {
            Connection connection = db.openConnection();
            try {
                List<List<Object>> parametersList = new ArrayList<List<Object>>();
                List<String> sqls = getInsertSqls(db, parametersList, id, actionSymbol,
                        typeSymbol, dimensions, amount, updateDate,
                        eventDateHour);
                Iterator<List<Object>> parametersIterator = parametersList.iterator();
                for (String sql : sqls) {
                    SqlDatabase.Static.executeUpdateWithList(connection, sql, parametersIterator.next());
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doIncrementUpdateOrInsert(SqlDatabase db, UUID id,
                String actionSymbol, String typeSymbol, int incrementAmount,
                long updateDate, long eventDateHour) throws SQLException {
            Connection connection = db.openConnection();
            try {
                List<Object> parameters = new ArrayList<Object>();
                String sql = getUpdateSql(db, parameters, id, actionSymbol,
                        incrementAmount, updateDate, eventDateHour, true);
                int rowsAffected = SqlDatabase.Static.executeUpdateWithList(
                        connection, sql, parameters);
                if (rowsAffected == 0) {
                    parameters = new ArrayList<Object>();
                    sql = getCountRecordInsertSql(db, parameters, id, actionSymbol,
                            typeSymbol, incrementAmount, updateDate,
                            eventDateHour);
                    SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        static String getUpdateSummarySql(SqlDatabase db, List<Object> parameters, int amount, UUID summaryRecordId, int summaryFieldSymbolId, String tableName, String valueColumnName, boolean increment) {
            /* TODO: this is going to have to change once countperformance is merged in - this table does not have typeId */
            SqlVendor vendor = db.getVendor();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("UPDATE ");
            vendor.appendIdentifier(sqlBuilder, tableName);
            sqlBuilder.append(" SET ");
            vendor.appendIdentifier(sqlBuilder, valueColumnName);
            sqlBuilder.append(" = ");
            if (increment) {
                vendor.appendIdentifier(sqlBuilder, valueColumnName);
                sqlBuilder.append(" + ");
            }
            vendor.appendBindValue(sqlBuilder, amount, parameters);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, "id");
            sqlBuilder.append(" = ");
            vendor.appendBindValue(sqlBuilder, summaryRecordId, parameters);
            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, "symbolId");
            sqlBuilder.append(" = ");
            vendor.appendBindValue(sqlBuilder, summaryFieldSymbolId, parameters);
            return sqlBuilder.toString();
        }

        static String getInsertSummarySql(SqlDatabase db, List<Object> parameters, int amount, UUID summaryRecordId, int summaryFieldSymbolId, String tableName, String valueColumnName) {
            /* TODO: this is going to have to change once countperformance is merged in - this table does not have typeId */
            SqlVendor vendor = db.getVendor();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("INSERT INTO ");
            vendor.appendIdentifier(sqlBuilder, tableName);
            sqlBuilder.append(" (");
            vendor.appendIdentifier(sqlBuilder, "id");
            sqlBuilder.append(", ");
            vendor.appendIdentifier(sqlBuilder, "symbolId");
            sqlBuilder.append(", ");
            vendor.appendIdentifier(sqlBuilder, valueColumnName);
            sqlBuilder.append(") VALUES (");
            vendor.appendBindValue(sqlBuilder, summaryRecordId, parameters);
            sqlBuilder.append(", ");
            vendor.appendBindValue(sqlBuilder, summaryFieldSymbolId, parameters);
            sqlBuilder.append(", ");
            vendor.appendBindValue(sqlBuilder, amount, parameters);
            sqlBuilder.append(")");
            return sqlBuilder.toString();
        }

        static void doCountSummaryUpdateOrInsert(SqlDatabase db, int amount, ObjectField countField, UUID summaryRecordId, boolean increment) throws SQLException {
            Connection connection = db.openConnection();
            StringBuilder symbolBuilder = new StringBuilder();
            symbolBuilder.append(countField.getJavaDeclaringClassName());
            symbolBuilder.append("/");
            symbolBuilder.append(countField.getInternalName());
            int summaryFieldSymbolId = db.getSymbolId(symbolBuilder.toString());
            String summaryTable = countField.as(SqlDatabase.FieldData.class).getIndexTable();
            String columnName = "value";
            if (countField.as(SqlDatabase.FieldData.class).isIndexTableSameColumnNames()) {
                columnName = countField.getInternalName();
                int dotAt = columnName.lastIndexOf(".");
                if (dotAt > -1) {
                    columnName = columnName.substring(dotAt+1);
                }
            }
            try {
                List<Object> parameters = new ArrayList<Object>();
                String sql = getUpdateSummarySql(db, parameters, amount, summaryRecordId, summaryFieldSymbolId, summaryTable, columnName, increment);
                int rowsAffected = SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                if (rowsAffected == 0) {
                    parameters = new ArrayList<Object>();
                    sql = getInsertSummarySql(db, parameters, amount, summaryRecordId, summaryFieldSymbolId, summaryTable, columnName);
                    SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                }
            } finally {
                db.closeConnection(connection);
            }
            
        }

        static void doIncrementCountSummaryUpdateOrInsert(SqlDatabase db, int amount, ObjectField countField, UUID summaryRecordId) throws SQLException {
            doCountSummaryUpdateOrInsert(db, amount, countField, summaryRecordId, true);
        }

        static void doSetCountSummaryUpdateOrInsert(SqlDatabase db, int amount, ObjectField countField, UUID summaryRecordId) throws SQLException {
            doCountSummaryUpdateOrInsert(db, amount, countField, summaryRecordId, false);
        }

        static Integer getCountByDimensions(SqlDatabase db,
                CountRecordQuery query) throws SQLException {
            String sql = getSelectCountSql(db, query);
            Connection connection = db.openReadConnection();
            Integer count = 0;
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                if (result.next()) {
                    count = result.getInt(1);
                }
                result.close();
                statement.close();
                return count;
            } finally {
                db.closeConnection(connection);
            }
        }

        static Map<String, Integer> getCountByDimensionsWithGroupBy(
                SqlDatabase db, CountRecordQuery query) throws SQLException {
            String sql = getSelectCountGroupBySql(db, query);
            Connection connection = db.openReadConnection();
            LinkedHashMap<String, Integer> results = new LinkedHashMap<String, Integer>();
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
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
                statement.close();
                return results;
            } finally {
                db.closeConnection(connection);
            }
        }

        static UUID getIdByDimensions(SqlDatabase db, CountRecordQuery query)
                throws SQLException {
            UUID id = null;
            // find the ID, it might be null
            String sql = Static.getSelectPreciseIdSql(db, query);
            Connection connection = db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                if (result.next()) {
                    id = UuidUtils.fromBytes(result.getBytes(1));
                    //LOGGER.info(this.id.toString());
                }
                result.close();
                statement.close();
            } finally {
                db.closeConnection(connection);
            }
            return id;
        }

    }

}

class CountRecordQuery {
    private final String symbol;
    private final String actionSymbol;
    private final DimensionSet dimensions;
    private Long startTimestamp;
    private Long endTimestamp;
    private DimensionSet groupByDimensions;
    private String[] orderByDimensions;

    public CountRecordQuery(String symbol, String actionSymbol, DimensionSet dimensions) {
        this.symbol = symbol;
        this.actionSymbol = actionSymbol;
        this.dimensions = dimensions;
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

    public Long getStartTimestamp() {
        return startTimestamp;
    }

    public Long getEndTimestamp() {
        return endTimestamp;
    }

    public void setDateRange(long startTimestamp, long endTimestamp) {
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    public DimensionSet getGroupByDimensions() {
        return groupByDimensions;
    }

    public String[] getOrderByDimensions() {
        return orderByDimensions;
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

