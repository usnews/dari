package com.psddev.dari.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
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
    static final String COUNTRECORD_NUMBERINDEX_TABLE = "CountRecordNumber";
    static final String COUNTRECORD_UUIDINDEX_TABLE = "CountRecordUuid";
    static final String COUNTRECORD_LOCATIONINDEX_TABLE = "CountRecordLocation";
    static final String COUNTRECORD_COUNTID_FIELD = "countId";
    static final int QUERY_TIMEOUT = 3;

    public static enum EventDatePrecision {
        HOUR,
        DAY,
        WEEK,
        WEEK_SUNDAY,
        WEEK_MONDAY,
        MONTH,
        YEAR,
        NONE
    }

    private final DimensionSet dimensions;
    private final String dimensionsSymbol;
    private final SqlDatabase db; 
    private final CountRecordQuery query;
    private final Record record;

    private EventDatePrecision eventDatePrecision = EventDatePrecision.NONE;
    private UUID countId;

    private Long updateDate;
    private Long eventDate;
    private Boolean dimensionsSaved;
    private ObjectField countField;

    public CountRecord(SqlDatabase database, Record record, String actionSymbol, Set<ObjectField> dimensions) {
        this.dimensions = DimensionSet.createDimensionSet(dimensions, record);
        this.dimensionsSymbol = this.getDimensionsSymbol(); // requires this.dimensions
        this.db = database; 
        this.query = new CountRecordQuery(dimensionsSymbol, actionSymbol, record, this.dimensions);
        this.record = record;
        //this.summaryRecordId = record.getId();
    }

    public CountRecord(Record record, String actionSymbol, Set<ObjectField> dimensions) {
        this(Database.Static.getFirst(SqlDatabase.class), record, actionSymbol, dimensions);
    }

    public void setSummaryField(ObjectField countField) {
        this.countField = countField;
    }

    public void setEventDatePrecision(EventDatePrecision precision) {
        this.eventDatePrecision = precision;
    }

    public Record getRecord() {
        return record;
    }

    public EventDatePrecision getEventDatePrecision() {
        return this.eventDatePrecision;
    }

    public SqlDatabase getDatabase() {
        return db;
    }

    public CountRecordQuery getQuery() {
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

    // This method will strip the minutes and seconds off of a timestamp
    public void setEventDate(long timestampMillis) {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.setTimeInMillis(timestampMillis);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MINUTE, 0);
        switch (getEventDatePrecision()) {
            case HOUR: // leave hour/day/month/year
                break;
            case DAY: // leave day/month/year, set hour to 0
                c.set(Calendar.HOUR_OF_DAY, 0);
                break;
            case WEEK_SUNDAY: // leave month/year, set day to sunday of this week
                c.set(Calendar.HOUR_OF_DAY, 0);
                while (c.get(Calendar.DAY_OF_WEEK) != Calendar.SUNDAY)
                  c.add(Calendar.DATE, -1);
                break;
            case WEEK: // same as WEEK_MONDAY
            case WEEK_MONDAY: // leave month/year, set day to monday of this week
                c.set(Calendar.HOUR_OF_DAY, 0);
                while (c.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY)
                  c.add(Calendar.DATE, -1);
                break;
            case MONTH: // leave month/year, set day to 1st day of this month
                c.set(Calendar.HOUR_OF_DAY, 0);
                c.set(Calendar.DAY_OF_MONTH, 1);
                break;
            case YEAR: // leave year, set month to 1st month of this year
                c.set(Calendar.HOUR_OF_DAY, 0);
                c.set(Calendar.DAY_OF_MONTH, 1);
                c.set(Calendar.MONTH, Calendar.JANUARY);
                break;
            case NONE: // not tracking dates at all - set to 0
                c.setTimeInMillis(0);
                break;
        }
        this.eventDate = (c.getTimeInMillis());
    }

    public long getEventDate() {
        if (this.eventDate == null) {
            setEventDate((System.currentTimeMillis()));
        }
        return this.eventDate;
    }

    public void setQueryDateRange(int startTimestamp, int endTimestamp) {
        getQuery().setDateRange(startTimestamp, endTimestamp);
    }

    public Integer getCount() throws SQLException {
        return Static.getCountByDimensions(getDatabase(), getQuery());
    }

    public void incrementCount(Integer amount) throws SQLException {
        // find the countId, it might be null
        UUID countId = getCountId();
        if (dimensionsSaved) {
            Static.doIncrementUpdateOrInsert(getDatabase(), countId, this.record.getId(), this.record.getState().getTypeId(), getQuery().getActionSymbol(),
                    getDimensionsSymbol(), amount, getUpdateDate(),
                    getEventDate());
        } else {
            Static.doInserts(getDatabase(), countId, this.record.getId(), this.record.getState().getTypeId(), getQuery().getActionSymbol(), getDimensionsSymbol(),
                    dimensions, amount, getUpdateDate(), getEventDate());
            dimensionsSaved = true;
        }
        if (isSummaryPossible()) {
            Static.doIncrementCountSummaryUpdateOrInsert(getDatabase(), amount, this.countField, this.record.getId());
        }
    }

    public void setCount(Integer amount) throws SQLException {
        // This only works if we're not tracking eventDate
        if (getEventDatePrecision() != EventDatePrecision.NONE) {
            throw new RuntimeException("CountRecord.setCount() can only be used if EventDatePrecision is NONE");
        }
        // find the countId, it might be null
        UUID countId = getCountId();
        if (dimensionsSaved) {
            Static.doSetUpdateOrInsert(getDatabase(), countId, this.record.getId(), this.record.getState().getTypeId(), getQuery().getActionSymbol(),
                    getDimensionsSymbol(), amount, getUpdateDate(),
                    getEventDate());
        } else {
            Static.doInserts(getDatabase(), countId, this.record.getId(), this.record.getState().getTypeId(), getQuery().getActionSymbol(), getDimensionsSymbol(),
                    dimensions, amount, getUpdateDate(), getEventDate());
            dimensionsSaved = true;
        }
        if (isSummaryPossible()) {
            Static.doIncrementCountSummaryUpdateOrInsert(getDatabase(), amount, this.countField, this.record.getId());
        }
    }

    /** This only needs to be executed if the summary has fallen out of sync due to data model change or some other operation */
    public void syncCountSummary() throws SQLException {
        if (isSummaryPossible()) {
            Static.doSetCountSummaryUpdateOrInsert(getDatabase(), getCount(), this.countField, this.record.getId());
        }
    }

    public void deleteCount() throws SQLException {
        Static.doCountDelete(getDatabase(), this.record.getId(), getQuery().getDimensions(), getQuery().getActionSymbol());
        if (isSummaryPossible()) {
            Static.doCountSummaryDelete(getDatabase(), this.record.getId(), this.countField);
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

    public UUID getCountId() throws SQLException {
        if (countId == null) {
            countId = Static.getCountIdByDimensions(getDatabase(), getQuery());
            if (countId == null) {
                // create a new countId
                dimensionsSaved = false;
                countId = UuidUtils.createSequentialUuid();
            } else {
                // this countId came from the DB
                dimensionsSaved = true;
            }
        }
        return countId;
    }

    public String getDimensionsSymbol() {
        if (this.dimensionsSymbol != null) {
            return dimensionsSymbol;
        } else {
            return this.dimensions.getSymbol();
        }
    }

    /** {@link CountRecord} utility methods. */
    public static final class Static {

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
            vendor.appendIdentifier(selectBuilder, COUNTRECORD_COUNTID_FIELD);

            if (selectAmount || preciseMatch || query.getDimensions().size() == 0)
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
                    vendor.appendIdentifier(whereBuilder, "dimensionsSymbolId");
                    whereBuilder.append(" = ");
                    vendor.appendValue(whereBuilder, db.getSymbolId(query.getSymbol()));
                }
                if (query.getRecord() != null) {
                    whereBuilder.append(" AND ");
                    vendor.appendIdentifier(whereBuilder, alias);
                    whereBuilder.append(".");
                    vendor.appendIdentifier(whereBuilder, "id");
                    whereBuilder.append(" = ");
                    vendor.appendValue(whereBuilder, query.getRecord().getId());
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
                        vendor.appendIdentifier(whereBuilder, "dimensionsSymbolId");
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
                    vendor.appendIdentifier(fromBuilder, "dimensionsSymbolId");
                    fromBuilder.append(" = ");
                    vendor.appendIdentifier(fromBuilder, alias);
                    fromBuilder.append(".");
                    vendor.appendIdentifier(fromBuilder, "dimensionsSymbolId");
                    fromBuilder.append(" AND ");
                    vendor.appendIdentifier(fromBuilder, "cr0");
                    fromBuilder.append(".");
                    vendor.appendIdentifier(fromBuilder, COUNTRECORD_COUNTID_FIELD);
                    fromBuilder.append(" = ");
                    vendor.appendIdentifier(fromBuilder, alias);
                    fromBuilder.append(".");
                    vendor.appendIdentifier(fromBuilder, COUNTRECORD_COUNTID_FIELD);
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
            vendor.appendIdentifier(groupByBuilder, COUNTRECORD_COUNTID_FIELD);
            orderByBuilder.append("\nORDER BY ");
            vendor.appendIdentifier(orderByBuilder, "cr0");
            orderByBuilder.append(".");
            vendor.appendIdentifier(orderByBuilder, COUNTRECORD_COUNTID_FIELD);
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

        static List<String> getInsertSqls(SqlDatabase db, List<List<Object>> parametersList, UUID countId,
                UUID recordId, UUID typeId, String actionSymbol, String dimensionsSymbol,
                DimensionSet dimensions, int amount, long createDate,
                long eventDate) {
            ArrayList<String> sqls = new ArrayList<String>();
            // insert countrecord
            List<Object> parameters = new ArrayList<Object>();
            sqls.add(getCountRecordInsertSql(db, parameters, countId, recordId, typeId, actionSymbol, dimensionsSymbol,
                    amount, createDate, eventDate));
            parametersList.add(parameters);
            // insert indexes
            for (Dimension dimension : dimensions) {
                Set<Object> values = dimension.getValues();
                String table = dimension.getIndexTable();
                for (Object value : values) {
                    parameters = new ArrayList<Object>();
                    sqls.add(getDimensionInsertRowSql(db, parameters, countId, recordId, dimensionsSymbol,
                            dimension, value, table));
                    parametersList.add(parameters);
                }
            }
            return sqls;
        }

        static String getCountRecordInsertSql(SqlDatabase db, List<Object> parameters, UUID countId,
                UUID recordId, UUID typeId, String actionSymbol, String dimensionsSymbol, int amount,
                long createDate, long eventDate) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            vendor.appendIdentifier(insertBuilder, COUNTRECORD_TABLE);
            insertBuilder.append(" (");
            LinkedHashMap<String, Object> cols = new LinkedHashMap<String, Object>();
            cols.put(COUNTRECORD_COUNTID_FIELD, countId);
            cols.put("id", recordId);
            cols.put("typeId", typeId);
            cols.put("actionSymbolId", db.getSymbolId(actionSymbol));
            cols.put("dimensionsSymbolId", db.getSymbolId(dimensionsSymbol));
            cols.put("amount", amount);
            cols.put("createDate", createDate);
            cols.put("updateDate", createDate);
            cols.put("eventDate", eventDate);
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

        static String getDimensionInsertRowSql(SqlDatabase db, List<Object> parameters, UUID countId,
                UUID recordId, String dimensionsSymbol, Dimension dimension, Object value,
                String table) {
            SqlVendor vendor = db.getVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            vendor.appendIdentifier(insertBuilder, table);
            insertBuilder.append(" (");
            LinkedHashMap<String, Object> cols = new LinkedHashMap<String, Object>();
            cols.put(COUNTRECORD_COUNTID_FIELD, countId);
            cols.put("id", recordId);
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

        static String getUpdateSql(SqlDatabase db, List<Object> parameters, UUID countId,
                String actionSymbol, int amount, long updateDate,
                long eventDate, boolean increment) {
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
            vendor.appendIdentifier(updateBuilder, COUNTRECORD_COUNTID_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, countId, parameters);
            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, "actionSymbolId");
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, db.getSymbolId(actionSymbol), parameters);
            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, "eventDate");
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, eventDate, parameters);
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

        static void doInserts(SqlDatabase db, UUID countId, UUID recordId, UUID typeId, String actionSymbol,
                String dimensionsSymbol, DimensionSet dimensions, int amount,
                long updateDate, long eventDate) throws SQLException {
            Connection connection = db.openConnection();
            try {
                List<List<Object>> parametersList = new ArrayList<List<Object>>();
                List<String> sqls = getInsertSqls(db, parametersList, countId, recordId, typeId, actionSymbol,
                        dimensionsSymbol, dimensions, amount, updateDate,
                        eventDate);
                Iterator<List<Object>> parametersIterator = parametersList.iterator();
                for (String sql : sqls) {
                    //LOGGER.info("===== [1] " + sql);
                    SqlDatabase.Static.executeUpdateWithList(connection, sql, parametersIterator.next());
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doIncrementUpdateOrInsert(SqlDatabase db, UUID countId, UUID recordId, UUID typeId, 
                String actionSymbol, String dimensionsSymbol, int incrementAmount,
                long updateDate, long eventDate) throws SQLException {
            Connection connection = db.openConnection();
            try {
                List<Object> parameters = new ArrayList<Object>();
                String sql = getUpdateSql(db, parameters, countId, actionSymbol,
                        incrementAmount, updateDate, eventDate, true);
                //LOGGER.info("===== [2] " + sql);
                int rowsAffected = SqlDatabase.Static.executeUpdateWithList(
                        connection, sql, parameters);
                if (rowsAffected == 0) {
                    parameters = new ArrayList<Object>();
                    sql = getCountRecordInsertSql(db, parameters, countId, recordId, typeId, actionSymbol,
                            dimensionsSymbol, incrementAmount, updateDate,
                            eventDate);
                    SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doSetUpdateOrInsert(SqlDatabase db, UUID countId, UUID recordId, UUID typeId, 
                String actionSymbol, String dimensionsSymbol, int amount,
                long updateDate, long eventDate) throws SQLException {
            Connection connection = db.openConnection();
            try {
                List<Object> parameters = new ArrayList<Object>();
                String sql = getUpdateSql(db, parameters, countId, actionSymbol,
                        amount, updateDate, eventDate, false);
                int rowsAffected = SqlDatabase.Static.executeUpdateWithList(
                        connection, sql, parameters);
                if (rowsAffected == 0) {
                    parameters = new ArrayList<Object>();
                    sql = getCountRecordInsertSql(db, parameters, countId, recordId, typeId, actionSymbol,
                            dimensionsSymbol, amount, updateDate,
                            eventDate);
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

        static String getDeleteDimensionSql(SqlDatabase db, UUID recordId, String table, int actionSymbolId) {
            SqlVendor vendor = db.getVendor();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ");
            vendor.appendIdentifier(sqlBuilder, table);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, "id");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, recordId);
            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, COUNTRECORD_COUNTID_FIELD);
            sqlBuilder.append(" IN (");
            sqlBuilder.append(" SELECT DISTINCT ");
            vendor.appendIdentifier(sqlBuilder, COUNTRECORD_COUNTID_FIELD);
            sqlBuilder.append(" FROM ");
            vendor.appendIdentifier(sqlBuilder, COUNTRECORD_TABLE);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, "id");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, recordId);
            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, "actionSymbolId");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, actionSymbolId);
            sqlBuilder.append(") ");
            return sqlBuilder.toString();
        }

        static String getDeleteCountRecordSql(SqlDatabase db, UUID recordId, int actionSymbolId) {
            SqlVendor vendor = db.getVendor();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ");
            vendor.appendIdentifier(sqlBuilder, COUNTRECORD_TABLE);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, "id");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, recordId);
            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, "actionSymbolId");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, actionSymbolId);
            return sqlBuilder.toString();
        }

        static String getDeleteSummarySql(SqlDatabase db, UUID recordId, String table, int summaryFieldSymbolId) {
            SqlVendor vendor = db.getVendor();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ");
            vendor.appendIdentifier(sqlBuilder, table);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, "id");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, recordId);
            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, "symbolId");
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, summaryFieldSymbolId);
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
                //LOGGER.info("===== [3] " + sql);
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

        static void doCountDelete(SqlDatabase db, UUID recordId, DimensionSet dimensions, String actionSymbol) throws SQLException {
            Connection connection = db.openConnection();
            List<Object> parameters = new ArrayList<Object>();
            int actionSymbolId = db.getSymbolId(actionSymbol);
            try {
                Set<String> tables = new HashSet<String>();
                for (Dimension dimension : dimensions) {
                    tables.add(dimension.getIndexTable());
                }
                // This needs to be executed BEFORE DeleteCountRecordSql
                for (String table : tables) {
                    String sql = getDeleteDimensionSql(db, recordId, table, actionSymbolId);
                    SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
                }
                String sql = getDeleteCountRecordSql(db, recordId, actionSymbolId);
                SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doCountSummaryDelete(SqlDatabase db, UUID recordId, ObjectField countField) throws SQLException {
            Connection connection = db.openConnection();
            List<Object> parameters = new ArrayList<Object>();
            StringBuilder symbolBuilder = new StringBuilder();
            symbolBuilder.append(countField.getJavaDeclaringClassName());
            symbolBuilder.append("/");
            symbolBuilder.append(countField.getInternalName());
            int summaryFieldSymbolId = db.getSymbolId(symbolBuilder.toString());
            try {
                String summaryTable = countField.as(SqlDatabase.FieldData.class).getIndexTable();
                String sql = getDeleteSummarySql(db, recordId, summaryTable, summaryFieldSymbolId);
                SqlDatabase.Static.executeUpdateWithList(connection, sql, parameters);
            } finally {
                db.closeConnection(connection);
            }
        }

        static Integer getCountByDimensions(SqlDatabase db, CountRecordQuery query) throws SQLException {
            String sql = getSelectCountSql(db, query);
            //LOGGER.info("===== [4] " + sql);
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

        static UUID getCountIdByDimensions(SqlDatabase db, CountRecordQuery query)
                throws SQLException {
            UUID countId = null;
            // find the countId, it might be null
            String sql = Static.getSelectPreciseIdSql(db, query);
            //LOGGER.info("===== [6] " + sql);
            Connection connection = db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                if (result.next()) {
                    countId = UuidUtils.fromBytes(result.getBytes(1));
                    //LOGGER.info(this.countId.toString());
                }
                result.close();
                statement.close();
            } finally {
                db.closeConnection(connection);
            }
            return countId;
        }

        public static String getIndexTable(ObjectField field) {
            String fieldType = field.getInternalItemType();
            if (fieldType.equals(ObjectField.UUID_TYPE)) {
                return CountRecord.COUNTRECORD_UUIDINDEX_TABLE;
            } else if (fieldType.equals(ObjectField.LOCATION_TYPE)) {
                return CountRecord.COUNTRECORD_LOCATIONINDEX_TABLE;
            } else if (fieldType.equals(ObjectField.NUMBER_TYPE) || 
                    fieldType.equals(ObjectField.DATE_TYPE)) {
                return CountRecord.COUNTRECORD_NUMBERINDEX_TABLE;
            } else {
                return CountRecord.COUNTRECORD_STRINGINDEX_TABLE;
            }
        }

    }

}

class CountRecordQuery {
    private final String symbol;
    private final String actionSymbol;
    private final DimensionSet dimensions;
    private final Record record;
    private Integer startTimestamp;
    private Integer endTimestamp;
    private DimensionSet groupByDimensions;
    private String[] orderByDimensions;

    public CountRecordQuery(String symbol, String actionSymbol, DimensionSet dimensions) {
        this.symbol = symbol;
        this.actionSymbol = actionSymbol;
        this.dimensions = dimensions;
        this.record = null;
    }

    public CountRecordQuery(String symbol, String actionSymbol, Record record,
            DimensionSet dimensions) {
        this.symbol = symbol;
        this.actionSymbol = actionSymbol;
        this.dimensions = dimensions;
        this.record = record;
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

    public Integer getStartTimestamp() {
        return startTimestamp;
    }

    public Integer getEndTimestamp() {
        return endTimestamp;
    }

    public void setDateRange(int startTimestamp, int endTimestamp) {
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
        return CountRecord.Static.getIndexTable(this.getObjectField());
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

