package com.psddev.dari.db;

import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.TaskExecutor;
import com.psddev.dari.util.WebPageContext;
import com.psddev.dari.util.StorageItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Debug servlet for running bulk storage item operations. */
@DebugFilter.Path("db-storage")
@SuppressWarnings("serial")
public class StorageDebugServlet extends HttpServlet {

    public static final String COPIER_PREFIX = "Database: Storage Item Copier: ";

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
            UUID selectedTypeId = wp.param(UUID.class, "typeId");
            ObjectType selectedType = null;
            for (ObjectType type : types) {
                if (type.getId().equals(selectedTypeId)) {
                    selectedType = type;
                    break;
                }
            }

            String source = wp.param(String.class, "source");
            String destination = wp.param(String.class, "destination");
            boolean saveObject = wp.param(boolean.class, "saveObject");

            String executor = COPIER_PREFIX + " from " + source + " to " + destination;
            AsyncQueue<Object> queue = new AsyncQueue<Object>();
            Database database = Database.Static.getDefault();
            Query<Object> query = Query.fromType(selectedType);

            new AsyncDatabaseReader<Object>(
                    executor, queue, database, query).
                    start();

            queue.closeAutomatically();

            for (int i = 0; i < 5; ++ i) {
                new AsyncStorageItemWriter<Object>(
                        executor, queue, database, WriteOperation.SAVE_UNSAFELY, 50, true, source, destination, saveObject).
                        start();
            }

            wp.redirect(null);
            return;
        }

        final List<TaskExecutor> copyExecutors = new ArrayList<TaskExecutor>();
        for (TaskExecutor executor : TaskExecutor.Static.getAll()) {
            if (executor.getName().startsWith(COPIER_PREFIX)) {
                copyExecutors.add(executor);
            }
        }

        new DebugFilter.PageWriter(getServletContext(), request, response) {{
            startPage("Database", "Storage Item Bulk Operations");

                start("h2");
                    html("Copy");
                end();

                start("p");
                    html("Use this to copy storage items from one location to another.");
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

                    start("div", "class", "control-group");
                        start("label", "class", "control-label", "id", wp.createId()).html("Source").end();
                        start("div", "class", "controls");
                            start("select", "class", "span2", "id", wp.getId(), "name", "source");
                                start("option", "value", "").html("").end();
                                for (String name : StorageItem.Static.getStorages()){
                                    start("option", "value", name).html(name).end();
                                }
                            end();
                        end();
                    end();

                    start("div", "class", "control-group");
                        start("label", "class", "control-label", "id", wp.createId()).html("Destination").end();
                        start("div", "class", "controls");
                            start("select", "class", "span2", "id", wp.getId(), "name", "destination");
                                start("option", "value", "").html("").end();
                                for (String name : StorageItem.Static.getStorages()){
                                    start("option", "value", name).html(name).end();
                                }
                            end();
                            start("label", "class", "checkbox", "style", "margin-top: 5px;");
                                tag("input", "name", "saveObject", "type", "checkbox");
                                html("Save object? (slower - consider reusing the original storage name instead)");
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
