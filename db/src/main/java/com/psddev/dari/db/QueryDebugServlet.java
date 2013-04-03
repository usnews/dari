package com.psddev.dari.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.StringUtils;

@DebugFilter.Path("query")
@SuppressWarnings("serial")
public class QueryDebugServlet extends HttpServlet {

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        @SuppressWarnings("all")
        Page page = new Page(getServletContext(), request, response);

        page.render();
    }

    private static class Page extends DebugFilter.PageWriter {

        private final Pattern ID_PATTERN = Pattern.compile("([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})");
        private final Pattern DATE_FIELD_PATTERN = Pattern.compile("(?m)^(\\s*\".*\" : (\\d{11,}),)$");

        private enum SortOrder {

            ASCENDING("Ascending"),
            DESCENDING("Descending");

            public final String displayName;

            private SortOrder(String displayName) {
                this.displayName = displayName;
            }
        }

        private enum SubAction {

            RAW("Raw"),
            EDIT_RAW("Edit Raw"),
            EDIT_FIELDED("Edit Fielded");

            public final String displayName;

            private SubAction(String displayName) {
                this.displayName = displayName;
            }
        }

        private final String databaseName;
        private Database database;
        private final ObjectType type;
        private final String where;
        private final String sortField;
        private final SortOrder sortOrder;
        private final String additionalFieldsString;
        private final String ignoreReadConnectionString;
        private final boolean ignoreReadConnection;
        private final long offset;
        private final int limit;
        private final String filter;
        private final Query<Object> query;

        public Page(ServletContext context, HttpServletRequest request, HttpServletResponse response) throws IOException {
            super(context, request, response);

            databaseName = page.param(String.class, "db");
            database = Database.Static.getInstance(databaseName);
            type = database.getEnvironment().getTypeById(page.param(UUID.class, "from"));
            where = page.paramOrDefault(String.class, "where", "").trim();
            sortField = page.param(String.class, "sortField");
            sortOrder = page.paramOrDefault(SortOrder.class, "sortOrder", SortOrder.ASCENDING);
            additionalFieldsString = page.param(String.class, "additionalFields");
            ignoreReadConnectionString = page.param(String.class, "ignoreReadConnection");
            ignoreReadConnection = ignoreReadConnectionString == null ? true : Boolean.parseBoolean(ignoreReadConnectionString);
            offset = page.param(long.class, "offset");
            limit = page.paramOrDefault(int.class, "limit", 50);
            filter = "Filter".equals(page.param(String.class, "action")) ? page.param(String.class, "filter") : null;

            UUID idFromWhere = ObjectUtils.to(UUID.class, where);

            if (idFromWhere != null) {
                query = Query.from(Object.class).where("_id = ?", idFromWhere);

            } else {
                query = Query.fromType(type);

                Double timeout = page.param(Double.class, "timeout");
                if (timeout != null) {
                    query.setTimeout(timeout);
                }

                if (!ObjectUtils.isBlank(where)) {
                    query.where(where);
                }

                if (!ObjectUtils.isBlank(sortField)) {
                    if (SortOrder.DESCENDING.equals(sortOrder)) {
                        query.sortDescending(sortField);
                    } else {
                        query.sortAscending(sortField);
                    }
                }
            }

            if (ObjectUtils.isBlank(additionalFieldsString) &&
                    ObjectUtils.isBlank(filter)) {
                query.resolveToReferenceOnly();
            }

            if (ObjectUtils.isBlank(databaseName)) {
                query.setDatabase(null);
                database = query.getDatabase();
            }

            CachingDatabase caching = new CachingDatabase();
            caching.setDelegate(database);
            query.using(caching);
        }

        public void render() throws IOException {
            try {
                Database.Static.setIgnoreReadConnection(ignoreReadConnection);
                String action = page.param(String.class, "action");

                if ("count".equals(action)) {
                    renderCount();

                } else if ("form".equals(action)) {
                    renderForm();

                } else if ("select".equals(action)) {
                    renderSelect();

                } else {
                    renderDefault();
                }

            } catch (Exception ex) {
                writeObject(ex);

            } finally {
                Database.Static.setIgnoreReadConnection(false);
            }
        }

        private void renderCount() throws IOException {
            try {
                if (query.getTimeout() == null) {
                    query.setTimeout(1.0);
                }
                writeObject(query.count());

            } catch (Exception ex) {
                writeHtml("Many (");
                writeStart("a", "href", page.url("", "timeout", 0));
                    writeHtml("Force Count");
                writeEnd();
                writeHtml(")");
            }
        }

        @SuppressWarnings("unchecked")
        private void renderForm() throws IOException {
            State state = State.getInstance(Query.from(Object.class).where("_id = ?", page.param(UUID.class, "id")).using(database).first());

            if (state == null) {
                writeStart("p", "class", "alert").writeHtml("No object!").writeEnd();

            } else {
                ObjectType type = state.getType();

                writeStart("div", "class", "edit", "style", "padding: 10px;");
                    writeStart("h2");
                        writeHtml(type != null ? type.getLabel() : "Unknown Type");
                        writeHtml(": ");
                        writeHtml(state.getLabel());
                    writeEnd();

                    SubAction subAction = page.paramOrDefault(SubAction.class, "subAction", SubAction.RAW);

                    writeStart("ul", "class", "nav nav-tabs");
                        for (SubAction a : SubAction.values()) {
                            writeStart("li", "class", a.equals(subAction) ? "active" : null);
                                writeStart("a", "href", page.url("", "subAction", a));
                                    writeHtml(a.displayName);
                                writeEnd();
                            writeEnd();
                        }
                    writeEnd();

                    if (SubAction.EDIT_RAW.equals(subAction)) {
                        if (page.isFormPost()) {
                            try {
                                state.setValues((Map<String, Object>) ObjectUtils.fromJson(page.param(String.class, "data")));
                                state.save();
                                writeStart("p", "class", "alert alert-success").writeHtml("Saved successfully at " + new Date() + "!").writeEnd();
                            } catch (Exception error) {
                                writeStart("div", "class", "alert alert-error").writeObject(error).writeEnd();
                            }
                        }

                        writeStart("form", "method", "post", "action", page.url(""));
                            writeStart("div", "class", "json");
                                writeStart("textarea", "name", "data", "style", "box-sizing: border-box; height: 40em; width: 100%;");
                                    writeHtml(ObjectUtils.toJson(state.getSimpleValues(), true));
                                writeEnd();
                            writeEnd();
                            writeStart("div", "class", "form-actions");
                                writeTag("input", "class", "btn btn-success", "type", "submit", "value", "Save");
                            writeEnd();
                        writeEnd();

                    } else if (SubAction.EDIT_FIELDED.equals(subAction)) {
                        @SuppressWarnings("all")
                        FormWriter form = new FormWriter(this);
                        form.putAllStandardInputProcessors();

                        if (page.isFormPost()) {
                            try {
                                form.updateAll(state, page.getRequest());
                                state.save();
                                writeStart("p", "class", "alert alert-success").writeHtml("Saved successfully at " + new Date() + "!").writeEnd();
                            } catch (Exception error) {
                                writeStart("div", "class", "alert alert-error").writeObject(error).writeEnd();
                            }
                        }

                        writeStart("form", "method", "post", "action", page.url(""));
                            form.allInputs(state);
                            writeStart("div", "class", "form-actions");
                                writeTag("input", "class", "btn btn-success", "type", "submit", "value", "Save");
                            writeEnd();
                        writeEnd();

                    } else {
                        writeStart("pre");
                            String json = ObjectUtils.toJson(state.getSimpleValues(), true);
                            Matcher dateFieldMatcher = DATE_FIELD_PATTERN.matcher(json);
                            StringBuilder newJson = new StringBuilder();
                            int end = 0;

                            for (; dateFieldMatcher.find(); end = dateFieldMatcher.end()) {
                                String dateString = dateFieldMatcher.group(2);

                                newJson.append(json.substring(end, dateFieldMatcher.start()));
                                newJson.append(dateFieldMatcher.group(1));
                                newJson.append(" /* ");
                                newJson.append(ObjectUtils.to(Date.class, dateString));
                                newJson.append(" */");
                            }

                            newJson.append(json.substring(end));

                            Matcher idMatcher = ID_PATTERN.matcher(page.h(newJson.toString()));
                            write(idMatcher.replaceAll("<a href=\"/_debug/query?where=id+%3D+$1\" target=\"_blank\">$1</a>"));
                        writeEnd();
                    writeEnd();
                }
            }
        }

        private void renderSelect() throws IOException {
            writeStart("div", "style", "padding: 10px;");
                writeStart("form", "action", page.url(null), "class", "form-inline", "method", "get");

                    writeStart("h2").writeHtml("Query").writeEnd();
                    writeStart("div", "class", "row");
                        writeStart("div", "class", "span6");
                            writeStart("select", "class", "span6", "name", "from");
                                writeStart("option", "value", "").writeHtml("ALL TYPES").writeEnd();

                                List<ObjectType> types = new ArrayList<ObjectType>(database.getEnvironment().getTypes());
                                Collections.sort(types, new ObjectFieldComparator("name", false));

                                for (ObjectType t : types) {
                                    if (!t.isEmbedded()) {
                                        writeStart("option",
                                                "selected", t.equals(type) ? "selected" : null,
                                                "value", t.getId());
                                            writeHtml(t.getLabel());
                                            writeHtml(" (");
                                            writeHtml(t.getInternalName());
                                            writeHtml(")");
                                        writeEnd();
                                    }
                                }
                            writeEnd();

                            includeStylesheet("/_resource/chosen/chosen.css");
                            includeScript("/_resource/chosen/chosen.jquery.min.js");
                            writeStart("script", "type", "text/javascript");
                                write("(function() {");
                                    write("$('select[name=from]').chosen({ 'search_contains': true });");
                                write("})();");
                            writeEnd();

                            writeStart("textarea",
                                    "class", "span6",
                                    "name", "where",
                                    "placeholder", "ID or Predicate (Leave Blank to Return All)",
                                    "rows", 4,
                                    "style", "margin-bottom: 4px; margin-top: 4px;");
                                writeHtml(where);
                            writeEnd();

                            writeTag("input", "class", "btn btn-primary", "type", "submit", "name", "action", "value", "Run");
                        writeEnd();

                        writeStart("div", "class", "span6");
                            writeStart("select", "name", "db", "style", "margin-bottom: 4px;");
                                writeStart("option",
                                        "value", "",
                                        "selected", ObjectUtils.isBlank(databaseName) ? "selected" : null);
                                    writeHtml("Default");
                                writeEnd();

                                for (Database db : Database.Static.getAll()) {
                                    String dbName = db.getName();
                                    writeStart("option",
                                            "value", dbName,
                                            "selected", dbName.equals(databaseName) ? "selected" : null);
                                        writeHtml(dbName);
                                    writeEnd();
                                }
                            writeEnd();

                            writeTag("br");
                            writeTag("input",
                                    "class", "input-small",
                                    "name", "sortField",
                                    "type", "text",
                                    "placeholder", "Sort",
                                    "value", sortField);
                            writeHtml(' ');
                            writeStart("select", "class", "input-small", "name", "sortOrder");
                                for (SortOrder so : SortOrder.values()) {
                                    writeStart("option",
                                            "selected", so.equals(sortOrder) ? "selected" : null,
                                            "value", so.name());
                                        writeHtml(so.displayName);
                                    writeEnd();
                                }
                            writeEnd();
                            writeHtml(' ');
                            writeTag("input",
                                    "class", "input-small",
                                    "name", "limit",
                                    "type", "text",
                                    "placeholder", "Limit",
                                    "value", limit);

                            writeTag("br");
                            writeTag("input",
                                    "class", "span6",
                                    "name", "additionalFields",
                                    "type", "text",
                                    "placeholder", "Additional Fields (Comma Separated)",
                                    "style", "margin-top: 4px;",
                                    "value", additionalFieldsString);

                            writeTag("br");
                            writeStart("label", "class", "checkbox");
                                writeTag("input",
                                        "name", "ignoreReadConnection",
                                        "type", "checkbox",
                                        "style", "margin-top: 4px;",
                                        "value", "true",
                                        "checked", ignoreReadConnection ? "checked" : null);
                                writeHtml(" Ignore read-specific connection settings");
                            writeEnd();
                        writeEnd();
                    writeEnd();
                writeEnd();

                try {
                    PaginatedResult<Object> result = query.select(offset, limit);
                    List<Object> items = result.getItems();

                    if (offset == 0 && items.isEmpty()) {
                        writeStart("p", "class", "alert").writeHtml("No matches!").writeEnd();

                    } else {
                        writeStart("h2");
                            writeHtml("Result ");
                            writeObject(result.getFirstItemIndex());
                            writeHtml(" to ");
                            writeObject(result.getLastItemIndex());
                            writeHtml(" of ");
                            writeStart("span", "class", "frame");
                                writeStart("a", "href", page.url("", "action", "count")).writeHtml("?").writeEnd();
                            writeEnd();
                        writeEnd();

                        writeStart("div", "class", "btn-group");
                            writeStart("a",
                                    "class", "btn" + (offset > 0 ? "" : " disabled"),
                                    "href", page.url("", "offset", 0));
                                writeStart("i", "class", "icon-fast-backward").writeEnd();
                                writeHtml(" First");
                            writeEnd();
                            writeStart("a",
                                    "class", "btn" + (result.hasPrevious() ? "" : " disabled"),
                                    "href", page.url("", "offset", result.getPreviousOffset()));
                                writeStart("i", "class", "icon-step-backward").writeEnd();
                                writeHtml(" Previous");
                            writeEnd();
                            writeStart("a",
                                    "class", "btn" + (result.hasNext() ? "" : " disabled"),
                                    "href", page.url("", "offset", result.getNextOffset()));
                                writeHtml("Next ");
                                writeStart("i", "class", "icon-step-forward").writeEnd();
                            writeEnd();
                        writeEnd();

                        String[] additionalFields;
                        if (ObjectUtils.isBlank(additionalFieldsString)) {
                            additionalFields = new String[0];
                        } else {
                            additionalFields = additionalFieldsString.trim().split("\\s*,\\s*");
                        }

                        writeStart("table", "class", "table table-condensed");
                            writeStart("thead");
                                writeStart("tr");
                                    writeStart("th").writeHtml("#").writeEnd();
                                    writeStart("th").writeHtml("ID").writeEnd();
                                    writeStart("th").writeHtml("Type").writeEnd();
                                    writeStart("th").writeHtml("Label").writeEnd();
                                    for (String additionalField : additionalFields) {
                                        writeStart("th").writeHtml(additionalField).writeEnd();
                                    }
                                writeEnd();
                            writeEnd();
                            writeStart("tbody");
                                long offsetCopy = offset;
                                for (Object item : items) {
                                    State itemState = State.getInstance(item);
                                    ObjectType itemType = itemState.getType();

                                    writeStart("tr");
                                        writeStart("td").writeHtml(++ offsetCopy).writeEnd();
                                        writeStart("td");
                                            writeStart("span",
                                                    "class", "link",
                                                    "onclick",
                                                            "var $input = $(this).popup('source').prev();" +
                                                            "$input.val('" + itemState.getId() + "');" +
                                                            "$input.prev().text('" + StringUtils.escapeJavaScript(itemState.getLabel()) + "');" +
                                                            "$(this).popup('close');" +
                                                            "return false;");
                                                writeHtml(itemState.getId());
                                            writeEnd();
                                        writeEnd();
                                        writeStart("td").writeHtml(itemType != null ? itemType.getLabel() : null).writeEnd();
                                        writeStart("td").writeHtml(itemState.getLabel()).writeEnd();
                                        for (String additionalField : additionalFields) {
                                            writeStart("td").writeHtml(itemState.getValue(additionalField)).writeEnd();
                                        }
                                    writeEnd();
                                }
                            writeEnd();
                        writeEnd();
                    }

                } catch (Exception ex) {
                    writeStart("div", "class", "alert alert-error");
                        writeObject(ex);
                    writeEnd();
                }
            writeEnd();
        }

        private void renderDefault() throws IOException {
            startPage("Database", "Query");

                writeStart("style", "type", "text/css");
                    write(".edit input[type=text], .edit textarea { width: 90%; }");
                    write(".edit textarea { min-height: 6em; }");
                writeEnd();

                includeStylesheet("/_resource/jquery/jquery.objectId.css");
                includeStylesheet("/_resource/jquery/jquery.repeatable.css");

                includeScript("/_resource/jquery/jquery.objectId.js");
                includeScript("/_resource/jquery/jquery.repeatable.js");

                writeStart("script", "type", "text/javascript");
                    write("(function() {");
                        write("$('.repeatable').repeatable();");
                        write("$('.objectId').objectId();");
                    write("})();");
                writeEnd();

                writeStart("form", "action", page.url(null), "class", "form-inline", "method", "get");

                    writeStart("h2").writeHtml("Query").writeEnd();
                    writeStart("div", "class", "row");
                        writeStart("div", "class", "span6");
                            writeStart("select", "class", "span6", "name", "from");
                                writeStart("option", "value", "").writeHtml("ALL TYPES").writeEnd();

                                List<ObjectType> types = new ArrayList<ObjectType>(database.getEnvironment().getTypes());
                                Collections.sort(types, new ObjectFieldComparator("name", false));

                                for (ObjectType t : types) {
                                    if (!t.isEmbedded()) {
                                        writeStart("option",
                                                "selected", t.equals(type) ? "selected" : null,
                                                "value", t.getId());
                                            writeHtml(t.getLabel());
                                            writeHtml(" (");
                                            writeHtml(t.getInternalName());
                                            writeHtml(")");
                                        writeEnd();
                                    }
                                }
                            writeEnd();

                            includeStylesheet("/_resource/chosen/chosen.css");
                            includeScript("/_resource/chosen/chosen.jquery.min.js");
                            writeStart("script", "type", "text/javascript");
                                write("(function() {");
                                    write("$('select[name=from]').chosen({ 'search_contains': true });");
                                write("})();");
                            writeEnd();

                            writeStart("textarea",
                                    "class", "span6",
                                    "name", "where",
                                    "placeholder", "ID or Predicate (Leave Blank to Return All)",
                                    "rows", 4,
                                    "style", "margin-bottom: 4px; margin-top: 4px;");
                                writeHtml(where);
                            writeEnd();

                            writeTag("input", "class", "btn btn-primary", "type", "submit", "name", "action", "value", "Run");
                        writeEnd();

                        writeStart("div", "class", "span6");
                            writeStart("select", "name", "db", "style", "margin-bottom: 4px;");
                                writeStart("option",
                                        "value", "",
                                        "selected", ObjectUtils.isBlank(databaseName) ? "selected" : null);
                                    writeHtml("Default");
                                writeEnd();

                                for (Database db : Database.Static.getAll()) {
                                    String dbName = db.getName();
                                    writeStart("option",
                                            "value", dbName,
                                            "selected", dbName.equals(databaseName) ? "selected" : null);
                                        writeHtml(dbName);
                                    writeEnd();
                                }
                            writeEnd();

                            writeTag("br");
                            writeTag("input",
                                    "class", "input-small",
                                    "name", "sortField",
                                    "type", "text",
                                    "placeholder", "Sort",
                                    "value", sortField);
                            writeHtml(' ');
                            writeStart("select", "class", "input-small", "name", "sortOrder");
                                for (SortOrder so : SortOrder.values()) {
                                    writeStart("option",
                                            "selected", so.equals(sortOrder) ? "selected" : null,
                                            "value", so.name());
                                        writeHtml(so.displayName);
                                    writeEnd();
                                }
                            writeEnd();
                            writeHtml(' ');
                            writeTag("input",
                                    "class", "input-small",
                                    "name", "limit",
                                    "type", "text",
                                    "placeholder", "Limit",
                                    "value", limit);

                            writeTag("br");
                            writeTag("input",
                                    "class", "span6",
                                    "name", "additionalFields",
                                    "type", "text",
                                    "placeholder", "Additional Fields (Comma Separated)",
                                    "style", "margin-top: 4px;",
                                    "value", additionalFieldsString);

                            writeTag("br");
                            writeStart("label", "class", "checkbox");
                                writeTag("input",
                                        "name", "ignoreReadConnection",
                                        "type", "checkbox",
                                        "style", "margin-top: 4px;",
                                        "value", "true",
                                        "checked", ignoreReadConnection ? "checked" : null);
                                writeHtml(" Ignore read-specific connection settings");
                            writeEnd();
                        writeEnd();
                    writeEnd();

                    writeStart("h2", "style", "margin-top: 18px;").writeHtml("Filter").writeEnd();
                    writeStart("div", "class", "row");
                        writeStart("div", "class", "span12");
                            writeStart("textarea",
                                    "class", "span12",
                                    "name", "filter",
                                    "placeholder", "Predicate (Leave Blank to Return All)",
                                    "rows", 2,
                                    "style", "margin-bottom: 4px;");
                                writeHtml(filter);
                            writeEnd();
                            writeTag("input", "class", "btn btn-primary", "type", "submit", "name", "action", "value", "Filter");
                        writeEnd();
                    writeEnd();
                writeEnd();

                try {
                    if (ObjectUtils.isBlank(filter)) {
                        PaginatedResult<Object> result = query.select(offset, limit);
                        List<Object> items = result.getItems();

                        if (offset == 0 && items.isEmpty()) {
                            writeStart("p", "class", "alert").writeHtml("No matches!").writeEnd();

                        } else {
                            writeStart("h2");
                                writeHtml("Result ");
                                writeObject(result.getFirstItemIndex());
                                writeHtml(" to ");
                                writeObject(result.getLastItemIndex());
                                writeHtml(" of ");
                                writeStart("span", "class", "frame");
                                    writeStart("a", "href", page.url("", "action", "count")).writeHtml("?").writeEnd();
                                writeEnd();
                            writeEnd();

                            writeStart("div", "class", "btn-group");
                                writeStart("a",
                                        "class", "btn" + (offset > 0 ? "" : " disabled"),
                                        "href", page.url("", "offset", 0));
                                    writeStart("i", "class", "icon-fast-backward").writeEnd();
                                    writeHtml(" First");
                                writeEnd();
                                writeStart("a",
                                        "class", "btn" + (result.hasPrevious() ? "" : " disabled"),
                                        "href", page.url("", "offset", result.getPreviousOffset()));
                                    writeStart("i", "class", "icon-step-backward").writeEnd();
                                    writeHtml(" Previous");
                                writeEnd();
                                writeStart("a",
                                        "class", "btn" + (result.hasNext() ? "" : " disabled"),
                                        "href", page.url("", "offset", result.getNextOffset()));
                                    writeHtml("Next ");
                                    writeStart("i", "class", "icon-step-forward").writeEnd();
                                writeEnd();
                            writeEnd();

                            String[] additionalFields;
                            if (ObjectUtils.isBlank(additionalFieldsString)) {
                                additionalFields = new String[0];
                            } else {
                                additionalFields = additionalFieldsString.trim().split("\\s*,\\s*");
                            }

                            writeStart("table", "class", "table table-condensed");
                                writeStart("thead");
                                    writeStart("tr");
                                        writeStart("th").writeHtml("#").writeEnd();
                                        writeStart("th").writeHtml("ID").writeEnd();
                                        writeStart("th").writeHtml("Type").writeEnd();
                                        writeStart("th").writeHtml("Label").writeEnd();
                                        for (String additionalField : additionalFields) {
                                            writeStart("th").writeHtml(additionalField).writeEnd();
                                        }
                                    writeEnd();
                                writeEnd();
                                writeStart("tbody");
                                    long offsetCopy = offset;
                                    for (Object item : items) {
                                        State itemState = State.getInstance(item);
                                        ObjectType itemType = itemState.getType();

                                        writeStart("tr");
                                            writeStart("td").writeHtml(++ offsetCopy).writeEnd();
                                            writeStart("td").writeHtml(itemState.getId()).writeEnd();
                                            writeStart("td").writeHtml(itemType != null ? itemType.getLabel() : null).writeEnd();
                                            writeStart("td");
                                                writeStart("a",
                                                        "target", "show",
                                                        "href", page.url("",
                                                                "action", "form",
                                                                "id", itemState.getId()));
                                                    writeHtml(itemState.getLabel());
                                                writeEnd();
                                            writeEnd();
                                            for (String additionalField : additionalFields) {
                                                writeStart("td").writeHtml(itemState.getValue(additionalField)).writeEnd();
                                            }
                                        writeEnd();
                                    }
                                writeEnd();
                            writeEnd();
                        }

                    } else {
                        Predicate filterPredicate = PredicateParser.Static.parse(filter);
                        List<Object> items = new ArrayList<Object>();

                        writeStart("h2");
                            writeHtml("Filtered Result");
                        writeEnd();

                        String[] additionalFields;
                        if (ObjectUtils.isBlank(additionalFieldsString)) {
                            additionalFields = new String[0];
                        } else {
                            additionalFields = additionalFieldsString.trim().split("\\s*,\\s*");
                        }

                        writeStart("table", "class", "table table-condensed");
                            writeStart("thead");
                                writeStart("tr");
                                    writeStart("th").writeHtml("#").writeEnd();
                                    writeStart("th").writeHtml("ID").writeEnd();
                                    writeStart("th").writeHtml("Type").writeEnd();
                                    writeStart("th").writeHtml("Label").writeEnd();
                                    for (String additionalField : additionalFields) {
                                        writeStart("th").writeHtml(additionalField).writeEnd();
                                    }
                                writeEnd();
                            writeEnd();
                            writeStart("tbody");
                                long total = 0;
                                long matched = 0;

                                for (Object item : query.iterable(0)) {
                                    ++ total;
                                    if (total % 1000 == 0) {
                                        writeStart("tr");
                                            writeStart("td", "colspan", additionalFields.length + 4);
                                                writeHtml("Read ").writeObject(total).writeHtml(" items");
                                            writeEnd();
                                        writeEnd();
                                        flush();
                                    }

                                    if (!PredicateParser.Static.evaluate(item, filterPredicate)) {
                                        continue;
                                    }

                                    ++ matched;
                                    items.add(item);
                                    State itemState = State.getInstance(item);
                                    ObjectType itemType = itemState.getType();

                                    writeStart("tr");
                                        writeStart("td").writeHtml(matched).writeEnd();
                                        writeStart("td").writeHtml(itemState.getId()).writeEnd();
                                        writeStart("td").writeHtml(itemType != null ? itemType.getLabel() : null).writeEnd();
                                        writeStart("td");
                                            writeStart("a", "href", "?where=id+%3D+" + itemState.getId(), "target", "_blank");
                                                writeHtml(itemState.getLabel());
                                            writeEnd();
                                        writeEnd();
                                        for (String additionalField : additionalFields) {
                                            writeStart("td").writeHtml(itemState.getValue(additionalField)).writeEnd();
                                        }
                                    writeEnd();

                                    if (matched >= limit) {
                                        break;
                                    }
                                }
                            writeEnd();
                        writeEnd();
                    }

                } catch (Exception ex) {
                    writeStart("div", "class", "alert alert-error");
                        writeObject(ex);
                    writeEnd();
                }

            endPage();
        }
    }
}
