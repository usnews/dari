package com.psddev.dari.db;

import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.TaskExecutor;
import com.psddev.dari.util.WebPageContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Debug servlet for running bulk database operations. */
@DebugFilter.Path("db-bulk")
@SuppressWarnings("serial")
public class BulkDebugServlet extends HttpServlet {

    public static final String INDEXER_PREFIX = "Database: Indexer: ";
    public static final String COPIER_PREFIX = "Database: Copier: ";

    // --- HttpServlet support ---

    @Override
    protected void service(
            final HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        final WebPageContext wp = new WebPageContext(this, request, response);
        Database selectedDatabase = Database.Static.getDefault();
        final List<ObjectType> types = new ArrayList<ObjectType>(selectedDatabase.getEnvironment().getTypes());
        Collections.sort(types, new ObjectFieldComparator("internalName", false));

        if (wp.isFormPost()) {
            String action = wp.param(String.class, "action");

            UUID selectedTypeId = wp.param(UUID.class, "typeId");
            ObjectType selectedType = null;
            for (ObjectType type : types) {
                if (type.getId().equals(selectedTypeId)) {
                    selectedType = type;
                    break;
                }
            }

            int writersCount = wp.paramOrDefault(int.class, "writersCount", 5);
            int commitSize = wp.paramOrDefault(int.class, "commitSize", 50);

            if ("index".equals(action)) {
                String executor = INDEXER_PREFIX + (selectedType != null ? selectedType.getInternalName() : "ALL");
                AsyncQueue<Object> queue = new AsyncQueue<Object>();
                Query<Object> query = Query.
                        fromType(selectedType).
                        resolveToReferenceOnly();

                new AsyncDatabaseReader<Object>(
                        executor, queue, selectedDatabase, query).
                        start();

                queue.closeAutomatically();

                for (int i = 0; i < writersCount; ++ i) {
                    new AsyncDatabaseWriter<Object>(
                            executor, queue, selectedDatabase, WriteOperation.INDEX, commitSize, true).
                            start();
                }

            } else if ("copy".equals(action)) {
                Database source = Database.Static.getInstance(wp.param(String.class, "source"));
                Database destination = Database.Static.getInstance(wp.param(String.class, "destination"));

                String executor = COPIER_PREFIX + " from " + source + " to " + destination;
                AsyncQueue<Object> queue = new AsyncQueue<Object>();
                Query<Object> query = Query.
                        fromType(selectedType).
                        resolveToReferenceOnly();

                if (wp.param(boolean.class, "deleteDestination")) {
                    destination.deleteByQuery(query);
                }

                new AsyncDatabaseReader<Object>(
                        executor, queue, source, query).
                        start();

                queue.closeAutomatically();

                for (int i = 0; i < writersCount; ++ i) {
                    new AsyncDatabaseWriter<Object>(
                            executor, queue, destination, WriteOperation.SAVE_UNSAFELY, commitSize, true).
                            start();
                }
            }

            wp.redirect(null);
            return;
        }

        final List<TaskExecutor> indexExecutors = new ArrayList<TaskExecutor>();
        final List<TaskExecutor> copyExecutors = new ArrayList<TaskExecutor>();
        for (TaskExecutor executor : TaskExecutor.Static.getAll()) {
            if (executor.getName().startsWith(INDEXER_PREFIX)) {
                indexExecutors.add(executor);
            } else if (executor.getName().startsWith(COPIER_PREFIX)) {
                copyExecutors.add(executor);
            }
        }

        new DebugFilter.PageWriter(getServletContext(), request, response) {{
            startPage("Database", "Bulk Operations");

                start("h2").html("Index").end();

                start("p");
                    html("Use this when you add ");
                    start("code").html("@FieldIndexed").end();
                    html(" to your model or if queries are returning unexpected results.");
                end();

                start("form", "action", "", "class", "form-horizontal", "method", "post");
                    tag("input", "name", "action", "type", "hidden", "value", "index");

                    start("div", "class", "control-group");
                        start("label", "class", "control-label").html("Type").end();
                        start("div", "class", "controls");
                            start("select", "name", "typeId");
                                start("option", "value", "").html("All").end();
                                for (ObjectType type : types) {
                                    if (!type.isEmbedded()) {
                                        start("option", "value", type.getId());
                                            html(type.getInternalName());
                                        end();
                                    }
                                }
                            end();
                        end();
                    end();

                    start("div", "class", "control-group");
                        start("label", "class", "control-label", "id", wp.createId()).html("# Of Writers").end();
                        start("div", "class", "controls");
                            tag("input", "name", "writersCount", "type", "text", "value", 5);
                        end();
                    end();

                    start("div", "class", "control-group");
                        start("label", "class", "control-label", "id", wp.createId()).html("Commit Size").end();
                        start("div", "class", "controls");
                            tag("input", "name", "commitSize", "type", "text", "value", 50);
                        end();
                    end();

                    start("div", "class", "form-actions");
                        tag("input", "type", "submit", "class", "btn btn-success", "value", "Start");
                    end();
                end();

                if (!indexExecutors.isEmpty()) {
                    start("h3").html("Ongoing Tasks").end();
                    start("ul");
                        for (TaskExecutor executor : indexExecutors) {
                            start("li");
                                start("a", "href", "task");
                                    html(executor.getName());
                                end();
                            end();
                        }
                    end();
                }

                start("h2").html("Copy").end();

                start("p");
                    html("Use this to copy objects from one database to another.");
                end();

                start("form", "action", "", "class", "form-horizontal", "method", "post");
                    tag("input", "name", "action", "type", "hidden", "value", "copy");

                    start("div", "class", "control-group");
                        start("label", "class", "control-label").html("Type").end();
                        start("div", "class", "controls");
                            start("select", "name", "typeId");
                                start("option", "value", "").html("All").end();
                                for (ObjectType type : types) {
                                    if (!type.isEmbedded()) {
                                        start("option", "value", type.getId());
                                            html(type.getInternalName());
                                        end();
                                    }
                                }
                            end();
                        end();
                    end();

                    List<Database> databases = Database.Static.getAll();

                    start("div", "class", "control-group");
                        start("label", "class", "control-label", "id", wp.createId()).html("Source").end();
                        start("div", "class", "controls");
                            start("select", "class", "span2", "id", wp.getId(), "name", "source");
                                start("option", "value", "").html("").end();
                                for (Database database : databases) {
                                    if (!(database instanceof Iterable)) {
                                        start("option", "value", database.getName());
                                            html(database);
                                        end();
                                    }
                                }
                            end();
                        end();
                    end();

                    start("div", "class", "control-group");
                        start("label", "class", "control-label", "id", wp.createId()).html("Destination").end();
                        start("div", "class", "controls");
                            start("select", "class", "span2", "id", wp.getId(), "name", "destination");
                                start("option", "value", "").html("").end();
                                for (Database database : databases) {
                                    if (!(database instanceof Iterable)) {
                                        start("option", "value", database.getName());
                                            html(database);
                                        end();
                                    }
                                }
                            end();
                            start("label", "class", "checkbox", "style", "margin-top: 5px;");
                                tag("input", "name", "deleteDestination", "type", "checkbox", "value", "true");
                                html("Delete before copy");
                            end();
                        end();
                    end();

                    start("div", "class", "form-actions");
                        tag("input", "type", "submit", "class", "btn btn-success", "value", "Start");
                    end();
                end();

                if (!copyExecutors.isEmpty()) {
                    start("h3").html("Ongoing Tasks").end();
                    start("ul");
                        for (TaskExecutor executor : copyExecutors) {
                            start("li");
                                start("a", "href", "task");
                                    html(executor.getName());
                                end();
                            end();
                        }
                    end();
                }

            endPage();
        }};
    }
}
