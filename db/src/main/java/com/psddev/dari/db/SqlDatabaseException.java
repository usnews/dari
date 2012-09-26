package com.psddev.dari.db;

import com.psddev.dari.util.HtmlObject;
import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.StringUtils;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Thrown when there's an error while executing any of the
 * {@link SqlDatabase} methods.
 */
@SuppressWarnings("serial")
public class SqlDatabaseException extends DatabaseException {

    private final String sqlQuery;
    private final Query<?> query;

    public SqlDatabaseException(
            SqlDatabase database,
            SQLException cause,
            String sqlQuery,
            Query<?> query) {

        super(database, cause);
        this.sqlQuery = sqlQuery;
        this.query = query;
    }

    public SqlDatabaseException(SqlDatabase database, String message, SQLException cause) {
        super(database, message, cause);
        this.sqlQuery = null;
        this.query = null;
    }

    public SqlDatabaseException(SqlDatabase database, String message) {
        super(database, message);
        this.sqlQuery = null;
        this.query = null;
    }

    public String getSqlQuery() {
        return sqlQuery;
    }

    public Query<?> getQuery() {
        return query;
    }

    @Override
    public String getMessage() {
        StringBuilder messageBuilder = new StringBuilder();

        String message = super.getMessage();
        messageBuilder.append(message != null ? message : "Can't execute SQL!");

        String sqlQuery = getSqlQuery();
        if (sqlQuery != null) {
            messageBuilder.append(" (");
            messageBuilder.append(sqlQuery);
            messageBuilder.append(")");
        }

        return messageBuilder.toString();
    }

    public static class ReadTimeout
            extends SqlDatabaseException
            implements DatabaseException.ReadTimeout, HtmlObject {

        public ReadTimeout(
                SqlDatabase database,
                SQLException cause,
                String sqlQuery,
                Query<?> query) {

            super(database, cause, sqlQuery, query);
        }

        // --- HtmlObject support ---

        @Override
        public void format(HtmlWriter writer) throws IOException {
            writer.start("p").html("Query took too long to execute!").end();

            writer.start("p");
                writer.html("Query: ");
                writer.object(getQuery());
            writer.end();

            String sql = getSqlQuery();
            writer.start("p");
                writer.html("SQL: ");
                writer.html(sql);

                writer.html(" (");
                    writer.start("a",
                            "target", "_blank",
                            "href", StringUtils.addQueryParameters(
                                    "/_debug/db-sql",
                                    "sql", "EXPLAIN " + sql));
                        writer.html("View execution plan");
                    writer.end();
                writer.html(")");
            writer.end();

            writer.start("ul", "class", "dari-stack-trace");
                for (StackTraceElement element : getStackTrace()) {
                    writer.start("li").object(element).end();
                }
            writer.end();
        }
    }
}
