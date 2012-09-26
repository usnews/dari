package com.psddev.dari.db;

import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@DebugFilter.Path("db-sql")
@SuppressWarnings("serial")
public class SqlDebugServlet extends HttpServlet {

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        new DebugFilter.PageWriter(getServletContext(), request, response) {{
            startPage("Database", "SQL");

                SqlDatabase database = null;
                List<SqlDatabase> databases = Database.Static.getByClass(SqlDatabase.class);
                for (SqlDatabase db : databases) {
                    if (db.getName().equals(page.param(String.class, "db"))) {
                        database = db;
                        break;
                    }
                }
                if (database == null) {
                    database = Database.Static.getFirst(SqlDatabase.class);
                }

                String sql = page.param(String.class, "sql");

                start("h2").html("Query").end();
                start("form", "action", page.url(null), "class", "form-inline", "method", "post");

                    start("select", "class", "span6", "name", "db");
                        for (SqlDatabase db : Database.Static.getByClass(SqlDatabase.class)) {
                            String dbName = db.getName();
                            start("option",
                                    "selected", db.equals(database) ? "selected" : null,
                                    "value", dbName);
                                html(dbName);
                            end();
                        }
                    end();

                    start("textarea",
                            "class", "span6",
                            "name", "sql",
                            "placeholder", "SQL Statement",
                            "rows", 4,
                            "style", "margin: 4px 0; width: 100%;");
                        html(sql);
                    end();

                    tag("input", "class", "btn btn-primary", "type", "submit", "value", "Run");
                end();

                if (!ObjectUtils.isBlank(sql)) {
                    start("h2").html("Result").end();
                    Connection connection = database.openConnection();

                    try {
                        Statement statement = connection.createStatement();

                        try {
                            long startTime = System.nanoTime();
                            statement.execute(sql);
                            start("p").html("Took ").start("strong").object((System.nanoTime() - startTime) / 1e6).end().html(" milliseconds.");

                            for (; true; statement.getMoreResults()) {
                                int updateCount = statement.getUpdateCount();

                                if (updateCount > -1) {
                                    start("p", "class", "alert alert-info");
                                        start("strong").object(updateCount).end().html(" items updated!");
                                    end();

                                } else {
                                    ResultSet result = statement.getResultSet();
                                    if (result == null) {
                                        break;
                                    }

                                    ResultSetMetaData meta = result.getMetaData();
                                    int count = meta.getColumnCount();

                                    start("table", "class", "table table-condensed");
                                        start("thead");
                                            start("tr");
                                                for (int i = 1; i <= count; ++ i) {
                                                    start("th");
                                                        html(meta.getColumnLabel(i));
                                                    end();
                                                }
                                            end();
                                        end();
                                        start("tbody");
                                            while (result.next()) {
                                                start("tr");
                                                    for (int i = 1; i <= count; ++ i) {
                                                        Object value = result.getObject(i);
                                                        if (value instanceof byte[]) {
                                                            value = new String(result.getBytes(i), StringUtils.UTF_8);
                                                        }
                                                        start("td");
                                                            object(value);
                                                        end();
                                                    }
                                                end();
                                            }
                                        end();
                                    end();
                                }
                            }

                        } finally {
                            statement.close();
                        }

                    } catch (Exception ex) {
                        start("div", "class", "alert alert-error");
                            object(ex);
                        end();

                    } finally {
                        database.closeConnection(connection);
                    }
                }

            endPage();
        }};
    }
}
