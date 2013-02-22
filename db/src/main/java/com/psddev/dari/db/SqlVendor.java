package com.psddev.dari.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.UuidUtils;

public class SqlVendor {

    public enum ColumnType {
        BYTES_LONG,
        BYTES_SHORT,
        DOUBLE,
        INTEGER,
        POINT,
        SERIAL,
        UUID;
    }

    public static final String RECORD_TABLE_NAME = "Record";
    public static final String RECORD_UPDATE_TABLE_NAME = "RecordUpdate";
    public static final String SYMBOL_TABLE_NAME = "Symbol";

    public static final int MAX_BYTES_SHORT_LENGTH = 500;

    private SqlDatabase database;

    public SqlDatabase getDatabase() {
        return database;
    }

    public void setDatabase(SqlDatabase database) {
        this.database = database;
    }

    public Set<String> getTables(Connection connection) throws SQLException {
        Set<String> tableNames = new HashSet<String>();
        String catalog = connection.getCatalog();
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet result = null;

        try {
            result = meta.getTables(catalog, null, null, null);
            while (result.next()) {
                String name = result.getString("TABLE_NAME");
                if (name != null) {
                    tableNames.add(name);
                }
            }
        } finally {
            result.close();
        }

        return tableNames;
    }

    public boolean hasInRowIndex(Connection connection, String recordTable) throws SQLException {
        boolean newHasInRowIndex = false;
        String catalog = connection.getCatalog();
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet result = null;

        try {
            result = meta.getColumns(catalog, null, recordTable, null);
            while (result.next()) {
                String name = result.getString("COLUMN_NAME");
                if (name != null && name.equalsIgnoreCase(SqlDatabase.IN_ROW_INDEX_COLUMN)) {
                    newHasInRowIndex = true;
                    break;
                }
            }
        } finally {
            result.close();
        }

        return newHasInRowIndex;
    }

    public boolean supportsDistinctBlob() {
        return true;
    }

    public void appendIdentifier(StringBuilder builder, String identifier) {
        builder.append("\"");
        builder.append(identifier.replace("\"", "\"\""));
        builder.append("\"");
    }

    public void appendBindLocation(StringBuilder builder, Location location, List<Object> parameters) {
        builder.append("GeomFromText(?)");
        if (parameters != null) {
            parameters.add(location == null ? null : "POINT(" + location.getX() + " " + location.getY() + ")");
        }
    }

    public void appendBindUuid(StringBuilder builder, UUID uuid, List<Object> parameters) {
        builder.append("?");
        if (parameters != null) {
            parameters.add(uuid == null ? null : UuidUtils.toBytes(uuid));
        }
    }

    public void appendBindString(StringBuilder builder, String value, List<Object> parameters) {
        builder.append("?");
        if (parameters != null) {
            parameters.add(value == null ? null : value.getBytes(StringUtils.UTF_8));
        }
    }

    public void appendBindValue(StringBuilder builder, Object value, List<Object> parameters) {
        if (value instanceof Location) {
            appendBindLocation(builder, (Location) value, parameters);

        } else if (value instanceof UUID) {
            appendBindUuid(builder, (UUID) value, parameters);

        } else if (value instanceof String) {
            appendBindString(builder, (String) value, parameters);

        } else {
            builder.append("?");
            if (parameters != null) {
                parameters.add(value);
            }
        }
    }

    public void appendValue(StringBuilder builder, Object value) {
        if (value == null) {
            builder.append("NULL");

        } else if (value instanceof Number) {
            builder.append(value);

        } else if (value instanceof UUID) {
            appendUuid(builder, (UUID) value);

        } else if (value instanceof byte[]) {
            appendBytes(builder, (byte[]) value);

        } else if (value instanceof Location) {
            Location valueLocation = (Location) value;
            builder.append("GeomFromText('POINT(");
            builder.append(valueLocation.getX());
            builder.append(" ");
            builder.append(valueLocation.getY());
            builder.append(")')");

        } else {
            appendBytes(builder, value.toString().getBytes(StringUtils.UTF_8));
        }
    }

    protected void appendUuid(StringBuilder builder, UUID value) {
        builder.append("{");
        builder.append(value);
        builder.append("}");
    }

    protected void appendBytes(StringBuilder builder, byte[] value) {
        builder.append("X'");
        builder.append(StringUtils.hex(value));
        builder.append("'");
    }

    protected void appendWhereRegion(StringBuilder builder, Region region, String field) {
        List<Location> locations = region.getLocations();

        builder.append("MBRCONTAINS(GEOMFROMTEXT('POLYGON((");
        for (Location location : locations) {
            builder.append(SqlDatabase.quoteValue(location.getX()));
            builder.append(' ');
            builder.append(SqlDatabase.quoteValue(location.getY()));
            builder.append(", ");
        }
        builder.setLength(builder.length() - 2);
        builder.append("))'), ");
        builder.append(field);
        builder.append(")");
    }

    protected void appendNearestLocation(
            StringBuilder orderbyBuilder,
            StringBuilder selectBuilder,
            StringBuilder whereBuilder,
            Location location, String field) {

        StringBuilder builder = new StringBuilder();

        builder.append("GLENGTH(LINESTRING(GEOMFROMTEXT('POINT(");
        builder.append(location.getX());
        builder.append(" ");
        builder.append(location.getY());
        builder.append(")'), ");
        builder.append(field);
        builder.append("))");

        orderbyBuilder.append(builder);
        selectBuilder.append(builder);
    }

    protected String rewriteQueryWithLimitClause(String query, int limit, long offset) {
        return String.format("%s LIMIT %d OFFSET %d", query, limit, offset);
    }

    /** Creates a table using the given parameters. */
    public void createTable(
            SqlDatabase database,
            String tableName,
            Map<String, ColumnType> columns,
            List<String> primaryKeyColumns)
            throws SQLException {

        if (database.hasTable(tableName)) {
            return;
        }

        StringBuilder ddlBuilder = new StringBuilder();
        appendTablePrefix(ddlBuilder, tableName, columns);

        for (Map.Entry<String, ColumnType> entry : columns.entrySet()) {
            appendColumn(ddlBuilder, entry.getKey(), entry.getValue());
            ddlBuilder.append(", ");
        }

        if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
            ddlBuilder.setLength(ddlBuilder.length() - 2);
        } else {
            appendPrimaryKey(ddlBuilder, primaryKeyColumns);
        }

        appendTableSuffix(ddlBuilder, tableName, columns);
        executeDdl(database, ddlBuilder);
    }

    /** Creates an index using the given parameters. */
    public void createIndex(
            SqlDatabase database,
            String tableName,
            List<String> columns,
            boolean isUnique)
            throws SQLException {

        StringBuilder ddlBuilder = new StringBuilder();
        appendIndexPrefix(ddlBuilder, tableName, columns, isUnique);

        appendIdentifier(ddlBuilder, columns.get(0));
        for (int i = 1, size = columns.size(); i < size; ++ i) {
            ddlBuilder.append(", ");
            appendIdentifier(ddlBuilder, columns.get(i));
        }

        appendIndexSuffix(ddlBuilder, tableName, columns, isUnique);
        executeDdl(database, ddlBuilder);
    }

    public void createRecord(SqlDatabase database) throws SQLException {
        if (database.hasTable(RECORD_TABLE_NAME)) {
            return;
        }

        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(SqlDatabase.ID_COLUMN, ColumnType.UUID);
        columns.put(SqlDatabase.TYPE_ID_COLUMN, ColumnType.UUID);
        columns.put(SqlDatabase.IN_ROW_INDEX_COLUMN, ColumnType.BYTES_SHORT);
        columns.put(SqlDatabase.DATA_COLUMN, ColumnType.BYTES_LONG);

        createTable(database, RECORD_TABLE_NAME, columns, Arrays.asList(SqlDatabase.TYPE_ID_COLUMN, SqlDatabase.ID_COLUMN));
        createIndex(database, RECORD_TABLE_NAME, Arrays.asList(SqlDatabase.ID_COLUMN), true);
    }

    private static final Map<SqlIndex, ColumnType> INDEX_TYPES; static {
        Map<SqlIndex, ColumnType> m = new HashMap<SqlIndex, ColumnType>();
        m.put(SqlIndex.LOCATION, ColumnType.POINT);
        m.put(SqlIndex.NUMBER, ColumnType.DOUBLE);
        m.put(SqlIndex.STRING, ColumnType.BYTES_SHORT);
        m.put(SqlIndex.UUID, ColumnType.UUID);
        INDEX_TYPES = m;
    }

    public void createRecordIndex(
            SqlDatabase database,
            String tableName,
            SqlIndex... types)
            throws SQLException {

        if (database.hasTable(tableName)) {
            return;
        }

        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(SqlDatabase.ID_COLUMN, ColumnType.UUID);
        columns.put(SqlDatabase.SYMBOL_ID_COLUMN, ColumnType.INTEGER);
        for (int i = 0, length = types.length; i < length; ++ i) {
            columns.put(SqlDatabase.VALUE_COLUMN + (i == 0 ? "" : i + 1), INDEX_TYPES.get(types[i]));
        }

        List<String> primaryKeyColumns = new ArrayList<String>(columns.keySet());
        primaryKeyColumns.add(primaryKeyColumns.remove(0));

        createTable(database, tableName, columns, primaryKeyColumns);
        createIndex(database, tableName, Arrays.asList(SqlDatabase.ID_COLUMN), false);
    }

    public void createRecordUpdate(SqlDatabase database) throws SQLException {
        if (database.hasTable(RECORD_UPDATE_TABLE_NAME)) {
            return;
        }

        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(SqlDatabase.ID_COLUMN, ColumnType.UUID);
        columns.put(SqlDatabase.TYPE_ID_COLUMN, ColumnType.UUID);
        columns.put(SqlDatabase.UPDATE_DATE_COLUMN, ColumnType.DOUBLE);

        createTable(database, RECORD_UPDATE_TABLE_NAME, columns, Arrays.asList(SqlDatabase.ID_COLUMN));
        createIndex(database, RECORD_UPDATE_TABLE_NAME, Arrays.asList(SqlDatabase.TYPE_ID_COLUMN, SqlDatabase.UPDATE_DATE_COLUMN), false);
        createIndex(database, RECORD_UPDATE_TABLE_NAME, Arrays.asList(SqlDatabase.UPDATE_DATE_COLUMN), false);
    }

    public void createSymbol(SqlDatabase database) throws SQLException {
        if (database.hasTable(SYMBOL_TABLE_NAME)) {
            return;
        }

        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(SqlDatabase.SYMBOL_ID_COLUMN, ColumnType.SERIAL);
        columns.put(SqlDatabase.VALUE_COLUMN, ColumnType.BYTES_SHORT);

        createTable(database, SYMBOL_TABLE_NAME, columns, Arrays.asList(SqlDatabase.SYMBOL_ID_COLUMN));
        createIndex(database, SYMBOL_TABLE_NAME, Arrays.asList(SqlDatabase.VALUE_COLUMN), true);
    }

    protected void appendTablePrefix(
            StringBuilder builder,
            String name,
            Map<String, ColumnType> columns) {

        builder.append("CREATE TABLE ");
        appendIdentifier(builder, name);
        builder.append(" (");
    }

    protected void appendTableSuffix(
            StringBuilder builder,
            String name,
            Map<String, ColumnType> columns) {

        builder.append(")");
    }

    protected void appendColumn(StringBuilder builder, String name, ColumnType type) {
        appendColumnPrefix(builder, name);
        switch (type) {
            case BYTES_LONG :
                appendColumnTypeBytesLong(builder);
                break;
            case BYTES_SHORT :
                appendColumnTypeBytesShort(builder);
                break;
            case DOUBLE :
                appendColumnTypeDouble(builder);
                break;
            case INTEGER :
                appendColumnTypeInteger(builder);
                break;
            case POINT :
                appendColumnTypePoint(builder);
                break;
            case SERIAL :
                appendColumnTypeSerial(builder);
                break;
            case UUID :
                appendColumnTypeUuid(builder);
                break;
        }
    }

    protected void appendColumnPrefix(StringBuilder builder, String name) {
        appendIdentifier(builder, name);
        builder.append(" ");
    }

    protected void appendColumnTypeBytesLong(StringBuilder builder) {
        builder.append("BIT VARYING NOT NULL");
    }

    protected void appendColumnTypeBytesShort(StringBuilder builder) {
        builder.append("BIT VARYING(");
        builder.append(MAX_BYTES_SHORT_LENGTH * 8);
        builder.append(") NOT NULL");
    }

    protected void appendColumnTypeDouble(StringBuilder builder) {
        builder.append("DOUBLE NOT NULL");
    }

    protected void appendColumnTypeInteger(StringBuilder builder) {
        builder.append("INT NOT NULL");
    }

    protected void appendColumnTypePoint(StringBuilder builder) {
        builder.append("POINT NOT NULL");
    }

    protected void appendColumnTypeSerial(StringBuilder builder) {
        builder.append("SERIAL NOT NULL");
    }

    protected void appendColumnTypeUuid(StringBuilder builder) {
        builder.append("UUID NOT NULL");
    }

    protected void appendPrimaryKey(StringBuilder builder, List<String> columns) {
        builder.append("PRIMARY KEY (");
        appendIdentifier(builder, columns.get(0));
        for (int i = 1, size = columns.size(); i < size; ++ i) {
            builder.append(", ");
            appendIdentifier(builder, columns.get(i));
        }
        builder.append(")");
    }

    protected void appendIndexPrefix(
            StringBuilder builder,
            String tableName,
            List<String> columns,
            boolean isUnique) {

        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append("k_");
        nameBuilder.append(tableName);
        for (String column : columns) {
            nameBuilder.append("_");
            nameBuilder.append(column);
        }

        builder.append("CREATE");
        if (isUnique) {
            builder.append(" UNIQUE");
        }
        builder.append(" INDEX ");
        appendIdentifier(builder, nameBuilder.toString());
        builder.append(" ON ");
        appendIdentifier(builder, tableName);
        builder.append(" (");
    }

    protected void appendIndexSuffix(
            StringBuilder builder,
            String tableName,
            List<String> columns,
            boolean isUnique) {

        builder.append(")");
    }

    private void executeDdl(SqlDatabase database, StringBuilder ddlBuilder) throws SQLException {
        String ddl = ddlBuilder.toString();
        ddlBuilder.setLength(0);

        Connection connection = database.openConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                statement.execute(ddl);
            } finally {
                statement.close();
            }

        } finally {
            database.closeConnection(connection);
        }
    }

    protected void appendSelectFields(StringBuilder builder, List<String> fields) {
        appendIdentifier(builder, SqlDatabase.DATA_COLUMN);
    }

    public static class H2 extends SqlVendor {

        @Override
        protected void appendUuid(StringBuilder builder, UUID value) {
            builder.append("'");
            builder.append(value);
            builder.append("'");
        }

        @Override
        protected void appendColumnTypeBytesLong(StringBuilder builder) {
            builder.append("LONGVARBINARY NOT NULL");
        }

        @Override
        protected void appendColumnTypeBytesShort(StringBuilder builder) {
            builder.append("VARBINARY(");
            builder.append(MAX_BYTES_SHORT_LENGTH);
            builder.append(") NOT NULL");
        }

        @Override
        protected void appendColumnTypePoint(StringBuilder builder) {
            builder.append("DOUBLE NOT NULL");
        }
    }

    public static class MySQL extends SqlVendor {

        private Boolean hasUdfGetFields;

        @Override
        public void appendIdentifier(StringBuilder builder, String identifier) {
            builder.append("`");
            builder.append(identifier.replace("`", "``"));
            builder.append("`");
        }

        @Override
        protected void appendUuid(StringBuilder builder, UUID value) {
            appendBytes(builder, UuidUtils.toBytes(value));
        }

        @Override
        protected void appendTableSuffix(
                StringBuilder builder,
                String name,
                Map<String, ColumnType> columns) {

            builder.append(") ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin");
        }

        @Override
        protected void appendColumnTypeBytesLong(StringBuilder builder) {
            builder.append("LONGBLOB NOT NULL");
        }

        @Override
        protected void appendColumnTypeBytesShort(StringBuilder builder) {
            builder.append("VARBINARY(");
            builder.append(MAX_BYTES_SHORT_LENGTH);
            builder.append(") NOT NULL");
        }

        @Override
        protected void appendColumnTypeSerial(StringBuilder builder) {
            builder.append("INT NOT NULL AUTO_INCREMENT");
        }

        @Override
        protected void appendColumnTypeUuid(StringBuilder builder) {
            builder.append("BINARY(16) NOT NULL");
        }

        @Override
        protected void appendSelectFields(StringBuilder builder, List<String> fields) {
            SqlDatabase database = getDatabase();

            if (hasUdfGetFields == null) {
                Connection connection = database.openConnection();

                try {
                    Statement statement = connection.createStatement();
                    try {
                        ResultSet result = statement.executeQuery("SELECT dari_get_fields('{}', 'test')");
                        try {
                            hasUdfGetFields = true;
                        } finally {
                            result.close();
                        }
                    } finally {
                        statement.close();
                    }

                } catch (SQLException error) {
                    if ("42000".equals(error.getSQLState())) {
                        hasUdfGetFields = false;
                    }

                } finally {
                    database.closeConnection(connection);
                }
            }

            if (Boolean.TRUE.equals(hasUdfGetFields)) {
                builder.append("dari_get_fields(r.");
                appendIdentifier(builder, SqlDatabase.DATA_COLUMN);

                for (ObjectField field : database.getEnvironment().getFields()) {
                    builder.append(", ");
                    appendValue(builder, field.getInternalName());
                }

                for (String field : fields) {
                    builder.append(", ");
                    appendValue(builder, field);
                }

                builder.append(")");

            } else {
                builder.append("r.");
                appendIdentifier(builder, SqlDatabase.DATA_COLUMN);
            }
        }
    }

    public static class PostgreSQL extends SqlVendor {

        @Override
        public void appendIdentifier(StringBuilder builder, String identifier) {
            builder.append(identifier);
        }

        @Override
        protected void appendUuid(StringBuilder builder, UUID value) {
            builder.append("'" + value.toString() + "'");
        }

        @Override
        public void appendValue(StringBuilder builder, Object value) {
            if (value instanceof String) {
                builder.append("'" + value + "'");

            } else if (value instanceof Location) {
                Location valueLocation = (Location) value;
                builder.append("ST_GeomFromText('POINT(");
                builder.append(valueLocation.getX());
                builder.append(" ");
                builder.append(valueLocation.getY());
                builder.append(")', 4326)");

            } else {
                super.appendValue(builder, value);
            }
        }

        @Override
        protected void appendBytes(StringBuilder builder, byte[] value) {
            builder.append("'");
            builder.append(new String(value, StringUtils.UTF_8).replace("'", "''"));
            builder.append("'");
        }

        @Override
        protected void appendWhereRegion(StringBuilder builder, Region region, String field) {
            builder.append(field);
            builder.append(" <-> ST_SetSRID(ST_MakePoint(");
            builder.append(region.getX());
            builder.append(", ");
            builder.append(region.getY());
            builder.append("), 4326) < ");
            builder.append(region.getRadius());
        }

        @Override
        protected void appendNearestLocation(
                StringBuilder orderbyBuilder,
                StringBuilder selectBuilder,
                StringBuilder whereBuilder,
                Location location, String field) {

            StringBuilder builder = new StringBuilder();

            builder.append(field);
            builder.append(" <-> ST_SetSRID(ST_MakePoint(");
            builder.append(location.getX());
            builder.append(", ");
            builder.append(location.getY());
            builder.append("), 4326) ");

            orderbyBuilder.append(builder);
            selectBuilder.append(builder);
        }

        @Override
        public void appendBindLocation(StringBuilder builder, Location location, List<Object> parameters) {
            builder.append("ST_GeomFromText(?, 4326)");
            if (location != null && parameters != null) {
                parameters.add("POINT(" + location.getX() + " " + location.getY() + ")");
            }
        }

        @Override
        public void appendBindUuid(StringBuilder builder, UUID uuid, List<Object> parameters) {
            builder.append("?");
            if (uuid != null && parameters != null) {
                parameters.add(uuid);
            }
        }
    }

    public static class Oracle extends SqlVendor {

        @Override
        public void appendIdentifier(StringBuilder builder, String identifier) {
            builder.append(identifier.replace("\"", "\"\""));
        }

        @Override
        protected void appendUuid(StringBuilder builder, UUID value) {
            appendBytes(builder, UuidUtils.toBytes(value));
        }

        @Override
        protected void appendBytes(StringBuilder builder, byte[] value) {
            builder.append("HEXTORAW('");
            builder.append(StringUtils.hex(value));
            builder.append("')");
        }

        @Override
        protected void appendWhereRegion(StringBuilder builder, Region region, String field) {
            throw new UnsupportedIndexException(this, field);
        }

        @Override
        protected void appendNearestLocation(
                StringBuilder orderbyBuilder,
                StringBuilder selectBuilder,
                StringBuilder whereBuilder,
                Location location, String field) {
            throw new UnsupportedIndexException(this, field);
        }

        @Override
        protected String rewriteQueryWithLimitClause(String query, int limit, long offset) {
            return String.format(
                    "SELECT * FROM " +
                    "    (SELECT a.*, ROWNUM rnum FROM " +
                    "        (%s) a " +
                    "      WHERE ROWNUM <= %d)" +
                    " WHERE rnum  >= %d", query, offset + limit, offset);
        }

        @Override
        public void appendBindLocation(StringBuilder builder, Location location, List<Object> parameters) {
            builder.append("SDO_GEOMETRY(2001, 8307, SDO_POINT_TYPE(?, ?, NULL), NULL, NULL)");
            if (location != null && parameters != null) {
                parameters.add(location.getX());
                parameters.add(location.getY());
            }
        }

        @Override
        public Set<String> getTables(Connection connection) throws SQLException {
            Set<String> tableNames = new HashSet<String>();
            ResultSet result = null;

            String sqlQuery = "SELECT TABLE_NAME FROM USER_TABLES";
            Statement statement = connection.createStatement();

            try {
                result = statement.executeQuery(sqlQuery);
                while (result.next()) {
                    String name = result.getString("TABLE_NAME");
                    if (name != null) {
                        tableNames.add(name);
                    }
                }
            } finally {
                result.close();
            }

            return tableNames;
        }

        @Override
        public boolean hasInRowIndex(Connection connection, String recordTable) throws SQLException {
            ResultSet result = null;

            String sqlQuery =
                    "SELECT 1 FROM USER_TAB_COLS " +
                    " WHERE TABLE_NAME = '" + recordTable + "' " +
                    "   AND COLUMN_NAME = '" + SqlDatabase.IN_ROW_INDEX_COLUMN + "'";
            Statement statement = connection.createStatement();

            try {
                result = statement.executeQuery(sqlQuery);
                while (result.next()) {
                    return true;
                }
            } finally {
                result.close();
            }

            return false;
        }

        @Override
        public boolean supportsDistinctBlob() {
            return false;
        }
    }
}
