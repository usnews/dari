package com.psddev.dari.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.TaskExecutor;
import com.psddev.dari.util.WebPageContext;

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

        @SuppressWarnings("all")
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
                        submit();

                queue.closeAutomatically();

                for (int i = 0; i < writersCount; ++ i) {
                    new AsyncDatabaseWriter<Object>(
                            executor, queue, selectedDatabase, WriteOperation.INDEX, commitSize, true).
                            submit();
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
                        submit();

                queue.closeAutomatically();

                for (int i = 0; i < writersCount; ++ i) {
                    new AsyncDatabaseWriter<Object>(
                            executor, queue, destination, WriteOperation.SAVE_UNSAFELY, commitSize, true).
                            submit();
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

                writeStart("h2").writeHtml("Index").writeEnd();

                writeStart("p");
                    writeHtml("Use this when you add ");
                    writeStart("code").writeHtml("@FieldIndexed").writeEnd();
                    writeHtml(" to your model or if queries are returning unexpected results.");
                writeEnd();

                writeStart("form", "action", "", "class", "form-horizontal", "method", "post");
                    writeTag("input", "name", "action", "type", "hidden", "value", "index");

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label").writeHtml("Type").writeEnd();
                        writeStart("div", "class", "controls");
                            writeStart("select", "name", "typeId");
                                writeStart("option", "value", "").writeHtml("All").writeEnd();
                                for (ObjectType type : types) {
                                    if (!type.isEmbedded()) {
                                        writeStart("option", "value", type.getId());
                                            writeHtml(type.getInternalName());
                                        writeEnd();
                                    }
                                }
                            writeEnd();
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("# Of Writers").writeEnd();
                        writeStart("div", "class", "controls");
                            writeTag("input", "name", "writersCount", "type", "text", "value", 5);
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("Commit Size").writeEnd();
                        writeStart("div", "class", "controls");
                            writeTag("input", "name", "commitSize", "type", "text", "value", 50);
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "form-actions");
                        writeTag("input", "type", "submit", "class", "btn btn-success", "value", "Start");
                    writeEnd();
                writeEnd();

                if (!indexExecutors.isEmpty()) {
                    writeStart("h3").writeHtml("Ongoing Tasks").writeEnd();
                    writeStart("ul");
                        for (TaskExecutor executor : indexExecutors) {
                            writeStart("li");
                                writeStart("a", "href", "task");
                                    writeHtml(executor.getName());
                                writeEnd();
                            writeEnd();
                        }
                    writeEnd();
                }

                writeStart("h2").writeHtml("Copy").writeEnd();

                writeStart("p");
                    writeHtml("Use this to copy objects from one database to another.");
                writeEnd();

                writeStart("form", "action", "", "class", "form-horizontal", "method", "post");
                    writeTag("input", "name", "action", "type", "hidden", "value", "copy");

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label").writeHtml("Type").writeEnd();
                        writeStart("div", "class", "controls");
                            writeStart("select", "name", "typeId");
                                writeStart("option", "value", "").writeHtml("All").writeEnd();
                                for (ObjectType type : types) {
                                    if (!type.isEmbedded()) {
                                        writeStart("option", "value", type.getId());
                                            writeHtml(type.getInternalName());
                                        writeEnd();
                                    }
                                }
                            writeEnd();
                        writeEnd();
                    writeEnd();

                    List<Database> databases = Database.Static.getAll();

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("Source").writeEnd();
                        writeStart("div", "class", "controls");
                            writeStart("select", "class", "span2", "id", wp.getId(), "name", "source");
                                writeStart("option", "value", "").writeHtml("").writeEnd();
                                for (Database database : databases) {
                                    if (!(database instanceof Iterable)) {
                                        writeStart("option", "value", database.getName());
                                            writeHtml(database);
                                        writeEnd();
                                    }
                                }
                            writeEnd();
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("Destination").writeEnd();
                        writeStart("div", "class", "controls");
                            writeStart("select", "class", "span2", "id", wp.getId(), "name", "destination");
                                writeStart("option", "value", "").writeHtml("").writeEnd();
                                for (Database database : databases) {
                                    if (!(database instanceof Iterable)) {
                                        writeStart("option", "value", database.getName());
                                            writeHtml(database);
                                        writeEnd();
                                    }
                                }
                            writeEnd();
                            writeStart("label", "class", "checkbox", "style", "margin-top: 5px;");
                                writeTag("input", "name", "deleteDestination", "type", "checkbox", "value", "true");
                                writeHtml("Delete before copy");
                            writeEnd();
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "form-actions");
                        writeTag("input", "type", "submit", "class", "btn btn-success", "value", "Start");
                    writeEnd();
                writeEnd();

                if (!copyExecutors.isEmpty()) {
                    writeStart("h3").writeHtml("Ongoing Tasks").writeEnd();
                    writeStart("ul");
                        for (TaskExecutor executor : copyExecutors) {
                            writeStart("li");
                                writeStart("a", "href", "task");
                                    writeHtml(executor.getName());
                                writeEnd();
                            writeEnd();
                        }
                    writeEnd();
                }

            endPage();
        }};
    }
}
