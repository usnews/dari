package com.psddev.dari.db;

// import com.psddev.dari.util.ObjectToIterable;
// import com.psddev.dari.util.ObjectUtils;
// import com.psddev.dari.util.PullThroughCache;
// import com.psddev.dari.util.StorageItem;
// import com.psddev.dari.util.StringUtils;
// import com.psddev.dari.util.TypeDefinition;
import com.psddev.dari.util.UuidUtils;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

// import java.lang.reflect.Field;
// import java.lang.reflect.Type;
// import java.lang.reflect.TypeVariable;
// import java.net.URI;
// import java.net.URL;
import java.util.ArrayList;
// import java.util.Collection;
// import java.util.Collections;
// import java.util.Date;
import java.util.HashMap;
// import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
//import java.util.HashSet;

import java.util.TreeSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountRecord {

    private static final Logger LOGGER = LoggerFactory.getLogger(CountRecord.class);

    private static final String COUNTRECORD_TABLE = "CountRecord";
    private static final String COUNTRECORD_STRINGINDEX_TABLE = "CountRecordString";
    private static final String COUNTRECORD_NUMBERINDEX_TABLE = "CountRecordNumber";
    private static final String COUNTRECORD_UUIDINDEX_TABLE = "CountRecordUuid";

    private final String counterType;
    private final Map<String, Object> dimensions;
    private final Class<?> objectClass;
    private final String symbol;
    private final SqlDatabase db; /* TODO: get rid of this and move all this implementation specific stuff where it goes */

    private UUID id;

    public CountRecord(Class<?> objectClass, String counterType, Map<String, Object> dimensions) {
        this.objectClass = objectClass;
        this.counterType = counterType;
        this.dimensions = dimensions;
        this.symbol = this.getSymbol();
        this.db = Database.Static.getFirst(SqlDatabase.class);
        //LOGGER.info("===== creating new instance of CountRecord: " + this.toString());
    }

    public String toString() {
        return "CountRecord: [" + this.getSymbol() + "] " + dimensions.toString();
    }

    public Integer getCount() throws SQLException {
        String sql = getSelectCountSql();
        Connection connection = db.openConnection();
        try {
            ResultSet result = selectSql(connection, sql);
            if (result.next()) {
                Integer count = result.getInt(1);
                return count;
            }
        } finally {
            db.closeConnection(connection);
        }
        return 0;
    }

    public Integer adjustCount(Integer amount) throws SQLException {
        // find the ID, it might be null
        String sql = getSelectPreciseCountSql();
        Connection connection = db.openConnection();
        int currentCount = 0;
        try {
            ResultSet result = selectSql(connection, sql);
            if (result.next()) {
                this.id = UuidUtils.fromBytes(result.getBytes(1));
                currentCount = result.getInt(2);
                //LOGGER.info(this.id.toString());
            }
        } finally {
            db.closeConnection(connection);
        }
        int newCount = currentCount + amount;
        if (this.id == null) {
            //LOGGER.info("INSERT!" + newCount);
            doInserts(newCount);
        } else {
            //LOGGER.info("UPDATE!" + newCount);
            doUpdate(newCount);
        }
        return newCount;
    }

    public Integer setCount(Integer amount) throws SQLException {
        // find the ID, it might be null
        if (this.id == null) {
            String sql = getSelectPreciseIdSql();
            Connection connection = db.openConnection();
            try {
                ResultSet result = selectSql(connection, sql);
                if (result.next()) {
                    this.id = UuidUtils.fromBytes(result.getBytes(1));
                    //LOGGER.info(this.id.toString());
                }
            } finally {
                db.closeConnection(connection);
            }
        }
        if (this.id == null) {
            //LOGGER.info("INSERT!" + amount);
            doInserts(amount);
        } else {
            //LOGGER.info("UPDATE!" + amount);
            doUpdate(amount);
        }
        return amount;
    }

    public String getSymbol() {
        if (symbol != null) return symbol;
        StringBuilder symbolBuilder = new StringBuilder(this.objectClass.getName());
        symbolBuilder.append("/");

        Iterator<String> keysIterator = new TreeSet<String>(this.dimensions.keySet()).iterator();
        symbolBuilder.append(keysIterator.next());
        while (keysIterator.hasNext()) {
            symbolBuilder.append(',');
            symbolBuilder.append(keysIterator.next());
        }
        symbolBuilder.append("#count:");
        symbolBuilder.append(this.counterType);
        return symbolBuilder.toString();
    }

    public String getDimensionSymbol(String keyName) {
        StringBuilder symbolBuilder = new StringBuilder(this.objectClass.getName());
        symbolBuilder.append("/");
        symbolBuilder.append(keyName.toLowerCase());
        return symbolBuilder.toString();
    }

    public UUID getId() {
        if (this.id == null) {
            this.id = newId();
        }
        return this.id;
    }

    private UUID newId() {
        return UuidUtils.createSequentialUuid();
    }

    private void doInserts(int amount) throws SQLException {
        Connection connection = db.openConnection();
        try {
            for (String sql : buildInsertSqls(amount)) {
                SqlDatabase.Static.executeUpdateWithArray(connection, sql);
            }
        } finally {
            db.closeConnection(connection);
        }
    }

    private void doUpdate(int amount) throws SQLException {
        Connection connection = db.openConnection();
        try {
            String sql = buildUpdateSql(amount);
            SqlDatabase.Static.executeUpdateWithArray(connection, sql);
        } finally {
            db.closeConnection(connection);
        }
    }

    private List<String> buildInsertSqls(int amount) {
        ArrayList<String> sqls = new ArrayList<String>();
        // insert countrecord
        sqls.add(buildInitialInsertSql(amount));
        // insert indexes
        for (Map.Entry<String, Object> entry : dimensions.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String table = getIndexTable(value);
            if (value instanceof UUID) {
                sqls.add(buildIndexInsertSql(key, (UUID) value, table));
            } else if (value instanceof Double ) {
                sqls.add(buildIndexInsertSql(key, (Double) value, table));
            } else if (value instanceof Integer ) {
                sqls.add(buildIndexInsertSql(key, ((Integer) value).doubleValue(), table));
            } else {
                sqls.add(buildIndexInsertSql(key, value.toString(), table));
            }
        }
        return sqls;
    }

    private String buildInitialInsertSql(int amount) {
        SqlVendor vendor = db.getVendor();
        StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
        insertBuilder.append(COUNTRECORD_TABLE);
        insertBuilder.append(" (");
        insertBuilder.append("id, typeSymbolId, amount");
        insertBuilder.append(") VALUES (");
        vendor.appendValue(insertBuilder, getId());
        insertBuilder.append(", ");
        insertBuilder.append(db.getSymbolId(this.getSymbol()));
        insertBuilder.append(", ");
        insertBuilder.append(amount);
        insertBuilder.append(")");
        return insertBuilder.toString();
    }

    private String buildIndexInsertSql(String key, Object value, String table) {
        SqlVendor vendor = db.getVendor();
        StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
        insertBuilder.append(table);
        insertBuilder.append(" (");
        insertBuilder.append("id, typeSymbolId, symbolId, value");
        insertBuilder.append(") VALUES (");
        vendor.appendValue(insertBuilder, getId());
        insertBuilder.append(", ");
        insertBuilder.append(db.getSymbolId(this.getSymbol()));
        insertBuilder.append(", ");
        insertBuilder.append(db.getSymbolId(this.getDimensionSymbol(key)));
        insertBuilder.append(", ");
        vendor.appendValue(insertBuilder, value);
        insertBuilder.append(")");
        return insertBuilder.toString();
    }

    private String buildUpdateSql(int amount) {
        StringBuilder updateBuilder = new StringBuilder("UPDATE ");
        SqlVendor vendor = db.getVendor();
        updateBuilder.append(COUNTRECORD_TABLE);
        updateBuilder.append(" SET amount = ");
        updateBuilder.append(amount);
        updateBuilder.append(" WHERE ");
        updateBuilder.append("id");
        updateBuilder.append(" = ");
        vendor.appendValue(updateBuilder, getId());
        return updateBuilder.toString();
    }

    private String getIndexTable (Object value) {
        if (value instanceof UUID) {
            return COUNTRECORD_UUIDINDEX_TABLE;
        } else if (value instanceof Double ) {
            return COUNTRECORD_NUMBERINDEX_TABLE;
        } else if (value instanceof Integer ) {
            return COUNTRECORD_NUMBERINDEX_TABLE;
        } else {
            return COUNTRECORD_STRINGINDEX_TABLE;
        }
    }

    private Set<String> getIndexTables() {
        LinkedHashSet<String> tables = new LinkedHashSet<String>();
        for (Map.Entry<String, Object> entry : dimensions.entrySet()) {
            tables.add(getIndexTable(entry.getValue()));
        }
        return tables;
    }

    private String getSelectPreciseIdSql() {
        return getSelectSql(true, false);
    }

    private String getSelectPreciseCountSql() {
        return getSelectSql(true, true);
    }

    private String getSelectCountSql() {
        return "SELECT SUM(x.amount) FROM (" + getSelectSql(false, true) + ") x";
    }

    private String getSelectSql(Boolean preciseMatch, Boolean selectAmount) {
        SqlVendor vendor = db.getVendor();
        StringBuilder selectBuilder = new StringBuilder("SELECT cr0.id");
        StringBuilder fromBuilder = new StringBuilder();
        StringBuilder whereBuilder = new StringBuilder();
        int i = 0;
        int count = 1;
        String alias = "cr" + i;

        if (selectAmount) {
            selectBuilder.append(", cr0.amount");
            fromBuilder.append(" FROM ");
            fromBuilder.append(COUNTRECORD_TABLE);
            fromBuilder.append(" ");
            fromBuilder.append(alias);
            whereBuilder.append(" WHERE ");
            if (preciseMatch) {
                whereBuilder.append(alias);
                whereBuilder.append(".");
                whereBuilder.append("typeSymbolId");
                whereBuilder.append(" = ");
                whereBuilder.append(db.getSymbolId(getSymbol()));
            } else {
                whereBuilder.append("1 = 1");
            }
            ++i;
        }

        for (String table : getIndexTables()) {
            alias = "cr" + i;
            if (i == 0) {
                fromBuilder.append(" FROM ");
                fromBuilder.append(table);
                fromBuilder.append(" ");
                fromBuilder.append(alias);
                whereBuilder.append(" WHERE ");
                if (preciseMatch) {
                    whereBuilder.append(alias);
                    whereBuilder.append(".");
                    whereBuilder.append("typeSymbolId");
                    whereBuilder.append(" = ");
                    whereBuilder.append(db.getSymbolId(getSymbol()));
                } else {
                    whereBuilder.append("1 = 1");
                }
            } else {
                fromBuilder.append(" JOIN ");
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
            whereBuilder.append(" AND (");
            for (Map.Entry<String, Object> entry : getDimensionsByIndexTable(table).entrySet()) {
                whereBuilder.append("(");
                whereBuilder.append(alias);
                whereBuilder.append(".");
                whereBuilder.append("symbolId");
                whereBuilder.append(" = ");
                whereBuilder.append(db.getSymbolId(getDimensionSymbol(entry.getKey())));
                whereBuilder.append(" AND ");
                whereBuilder.append(alias);
                whereBuilder.append(".");
                whereBuilder.append("value");
                whereBuilder.append(" = ");
                vendor.appendValue(whereBuilder, entry.getValue());
                whereBuilder.append(")");
                whereBuilder.append(" OR ");
                ++numFilters;
            }
            whereBuilder.setLength(whereBuilder.length() - 4);
            whereBuilder.append(") ");
            count = count * numFilters;
            ++i;
        }
        whereBuilder.append("GROUP BY ");
        whereBuilder.append("cr0.id");
        if (selectAmount) {
            whereBuilder.append(", cr0.amount");
        }
        whereBuilder.append(" HAVING COUNT(*) = ");
        whereBuilder.append(count);
        return selectBuilder.toString() + " " + fromBuilder.toString() + " " + whereBuilder.toString();
    }

    private Map<String, Object> getDimensionsByIndexTable(String table) {
        HashMap<String, Object> dims = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry : dimensions.entrySet()) {
            if (table == getIndexTable(entry.getValue())) {
                dims.put(entry.getKey(), entry.getValue());
            }
        }
        return dims;
    }

    private ResultSet selectSql(Connection connection, String sql) throws SQLException {
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

}
