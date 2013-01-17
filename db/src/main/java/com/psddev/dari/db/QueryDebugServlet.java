package com.psddev.dari.db;

import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.StringUtils;

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

@DebugFilter.Path("query")
@SuppressWarnings("serial")
public class QueryDebugServlet extends HttpServlet {

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        new Page(getServletContext(), request, response).render();
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
                object(ex);

            } finally {
                Database.Static.setIgnoreReadConnection(false);
            }
        }

        private void renderCount() throws IOException {
            try {
                if (query.getTimeout() == null) {
                    query.setTimeout(1.0);
                }
                object(query.count());

            } catch (Exception ex) {
                html("Many (");
                start("a", "href", page.url("", "timeout", 0));
                    html("Force Count");
                end();
                html(")");
            }
        }

        @SuppressWarnings("unchecked")
        private void renderForm() throws IOException {
            State state = State.getInstance(Query.from(Object.class).where("_id = ?", page.param(UUID.class, "id")).using(database).first());

            if (state == null) {
                start("p", "class", "alert").html("No object!").end();

            } else {
                ObjectType type = state.getType();

                start("div", "class", "edit", "style", "padding: 10px;");
                    start("h2");
                        html(type != null ? type.getLabel() : "Unknown Type");
                        html(": ");
                        html(state.getLabel());
                    end();

                    SubAction subAction = page.paramOrDefault(SubAction.class, "subAction", SubAction.RAW);

                    start("ul", "class", "nav nav-tabs");
                        for (SubAction a : SubAction.values()) {
                            start("li", "class", a.equals(subAction) ? "active" : null);
                                start("a", "href", page.url("", "subAction", a));
                                    html(a.displayName);
                                end();
                            end();
                        }
                    end();

                    if (SubAction.EDIT_RAW.equals(subAction)) {
                        if (page.isFormPost()) {
                            try {
                                state.setValues((Map<String, Object>) ObjectUtils.fromJson(page.param(String.class, "data")));
                                state.save();
                                start("p", "class", "alert alert-success").html("Saved successfully at " + new Date() + "!").end();
                            } catch (Exception error) {
                                start("div", "class", "alert alert-error").object(error).end();
                            }
                        }

                        start("form", "method", "post", "action", page.url(""));
                            start("div", "class", "json");
                                start("textarea", "name", "data", "style", "box-sizing: border-box; height: 40em; width: 100%;");
                                    html(ObjectUtils.toJson(state.getSimpleValues(), true));
                                end();
                            end();
                            start("div", "class", "form-actions");
                                tag("input", "class", "btn btn-success", "type", "submit", "value", "Save");
                            end();
                        end();

                    } else if (SubAction.EDIT_FIELDED.equals(subAction)) {
                        FormWriter form = new FormWriter(this);
                        form.putAllStandardInputProcessors();

                        if (page.isFormPost()) {
                            try {
                                form.updateAll(state, page.getRequest());
                                state.save();
                                start("p", "class", "alert alert-success").html("Saved successfully at " + new Date() + "!").end();
                            } catch (Exception error) {
                                start("div", "class", "alert alert-error").object(error).end();
                            }
                        }

                        start("form", "method", "post", "action", page.url(""));
                            form.allInputs(state);
                            start("div", "class", "form-actions");
                                tag("input", "class", "btn btn-success", "type", "submit", "value", "Save");
                            end();
                        end();

                    } else {
                        start("pre");
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
                        end();
                    end();
                }
            }
        }

        private void renderSelect() throws IOException {
            start("div", "style", "padding: 10px;");
                start("form", "action", page.url(null), "class", "form-inline", "method", "get");

                    start("h2").html("Query").end();
                    start("div", "class", "row");
                        start("div", "class", "span6");
                            start("select", "class", "span6", "name", "from");
                                start("option", "value", "").html("ALL TYPES").end();

                                List<ObjectType> types = new ArrayList<ObjectType>(database.getEnvironment().getTypes());
                                Collections.sort(types, new ObjectFieldComparator("name", false));

                                for (ObjectType t : types) {
                                    if (!t.isEmbedded()) {
                                        start("option",
                                                "selected", t.equals(type) ? "selected" : null,
                                                "value", t.getId());
                                            html(t.getLabel());
                                            html(" (");
                                            html(t.getInternalName());
                                            html(")");
                                        end();
                                    }
                                }
                            end();

                            includeStylesheet("/_resource/chosen/chosen.css");
                            includeScript("/_resource/chosen/chosen.jquery.min.js");
                            start("script", "type", "text/javascript");
                                write("(function() {");
                                    write("$('select[name=from]').chosen({ 'search_contains': true });");
                                write("})();");
                            end();

                            start("textarea",
                                    "class", "span6",
                                    "name", "where",
                                    "placeholder", "ID or Predicate (Leave Blank to Return All)",
                                    "rows", 4,
                                    "style", "margin-bottom: 4px; margin-top: 4px;");
                                html(where);
                            end();

                            tag("input", "class", "btn btn-primary", "type", "submit", "name", "action", "value", "Run");
                        end();

                        start("div", "class", "span6");
                            start("select", "name", "db", "style", "margin-bottom: 4px;");
                                start("option",
                                        "value", "",
                                        "selected", ObjectUtils.isBlank(databaseName) ? "selected" : null);
                                    html("Default");
                                end();

                                for (Database db : Database.Static.getAll()) {
                                    String dbName = db.getName();
                                    start("option",
                                            "value", dbName,
                                            "selected", dbName.equals(databaseName) ? "selected" : null);
                                        html(dbName);
                                    end();
                                }
                            end();

                            tag("br");
                            tag("input",
                                    "class", "input-small",
                                    "name", "sortField",
                                    "type", "text",
                                    "placeholder", "Sort",
                                    "value", sortField);
                            html(' ');
                            start("select", "class", "input-small", "name", "sortOrder");
                                for (SortOrder so : SortOrder.values()) {
                                    start("option",
                                            "selected", so.equals(sortOrder) ? "selected" : null,
                                            "value", so.name());
                                        html(so.displayName);
                                    end();
                                }
                            end();
                            html(' ');
                            tag("input",
                                    "class", "input-small",
                                    "name", "limit",
                                    "type", "text",
                                    "placeholder", "Limit",
                                    "value", limit);

                            tag("br");
                            tag("input",
                                    "class", "span6",
                                    "name", "additionalFields",
                                    "type", "text",
                                    "placeholder", "Additional Fields (Comma Separated)",
                                    "style", "margin-top: 4px;",
                                    "value", additionalFieldsString);

                            tag("br");
                            start("label", "class", "checkbox");
                                tag("input",
                                        "name", "ignoreReadConnection",
                                        "type", "checkbox",
                                        "style", "margin-top: 4px;",
                                        "value", "true",
                                        "checked", ignoreReadConnection ? "checked" : null);
                                html(" Ignore read-specific connection settings");
                            end();
                        end();
                    end();
                end();

                try {
                    PaginatedResult<Object> result = query.select(offset, limit);
                    List<Object> items = result.getItems();

                    if (offset == 0 && items.isEmpty()) {
                        start("p", "class", "alert").html("No matches!").end();

                    } else {
                        start("h2");
                            html("Result ");
                            object(result.getFirstItemIndex());
                            html(" to ");
                            object(result.getLastItemIndex());
                            html(" of ");
                            start("span", "class", "frame");
                                start("a", "href", page.url("", "action", "count")).html("?").end();
                            end();
                        end();

                        start("div", "class", "btn-group");
                            start("a",
                                    "class", "btn" + (offset > 0 ? "" : " disabled"),
                                    "href", page.url("", "offset", 0));
                                start("i", "class", "icon-fast-backward").end();
                                html(" First");
                            end();
                            start("a",
                                    "class", "btn" + (result.hasPrevious() ? "" : " disabled"),
                                    "href", page.url("", "offset", result.getPreviousOffset()));
                                start("i", "class", "icon-step-backward").end();
                                html(" Previous");
                            end();
                            start("a",
                                    "class", "btn" + (result.hasNext() ? "" : " disabled"),
                                    "href", page.url("", "offset", result.getNextOffset()));
                                html("Next ");
                                start("i", "class", "icon-step-forward").end();
                            end();
                        end();

                        String[] additionalFields;
                        if (ObjectUtils.isBlank(additionalFieldsString)) {
                            additionalFields = new String[0];
                        } else {
                            additionalFields = additionalFieldsString.trim().split("\\s*,\\s*");
                        }

                        start("table", "class", "table table-condensed");
                            start("thead");
                                start("tr");
                                    start("th").html("#").end();
                                    start("th").html("ID").end();
                                    start("th").html("Type").end();
                                    start("th").html("Label").end();
                                    for (String additionalField : additionalFields) {
                                        start("th").html(additionalField).end();
                                    }
                                end();
                            end();
                            start("tbody");
                                long offsetCopy = offset;
                                for (Object item : items) {
                                    State itemState = State.getInstance(item);
                                    ObjectType itemType = itemState.getType();

                                    start("tr");
                                        start("td").html(++ offsetCopy).end();
                                        start("td");
                                            start("span",
                                                    "class", "link",
                                                    "onclick",
                                                            "var $input = $(this).popup('source').prev();" +
                                                            "$input.val('" + itemState.getId() + "');" +
                                                            "$input.prev().text('" + StringUtils.escapeJavaScript(itemState.getLabel()) + "');" +
                                                            "$(this).popup('close');" +
                                                            "return false;");
                                                html(itemState.getId());
                                            end();
                                        end();
                                        start("td").html(itemType != null ? itemType.getLabel() : null).end();
                                        start("td").html(itemState.getLabel()).end();
                                        for (String additionalField : additionalFields) {
                                            start("td").html(itemState.getValue(additionalField)).end();
                                        }
                                    end();
                                }
                            end();
                        end();
                    }

                } catch (Exception ex) {
                    start("div", "class", "alert alert-error");
                        object(ex);
                    end();
                }
            end();
        }

        private void renderDefault() throws IOException {
            startPage("Database", "Query");

                start("style", "type", "text/css");
                    write(".edit input[type=text], .edit textarea { width: 90%; }");
                    write(".edit textarea { min-height: 6em; }");
                end();

                includeStylesheet("/_resource/jquery/jquery.objectId.css");
                includeStylesheet("/_resource/jquery/jquery.repeatable.css");

                includeScript("/_resource/jquery/jquery.objectId.js");
                includeScript("/_resource/jquery/jquery.repeatable.js");

                start("script", "type", "text/javascript");
                    write("(function() {");
                        write("$('.repeatable').repeatable();");
                        write("$('.objectId').objectId();");
                    write("})();");
                end();

                start("form", "action", page.url(null), "class", "form-inline", "method", "get");

                    start("h2").html("Query").end();
                    start("div", "class", "row");
                        start("div", "class", "span6");
                            start("select", "class", "span6", "name", "from");
                                start("option", "value", "").html("ALL TYPES").end();

                                List<ObjectType> types = new ArrayList<ObjectType>(database.getEnvironment().getTypes());
                                Collections.sort(types, new ObjectFieldComparator("name", false));

                                for (ObjectType t : types) {
                                    if (!t.isEmbedded()) {
                                        start("option",
                                                "selected", t.equals(type) ? "selected" : null,
                                                "value", t.getId());
                                            html(t.getLabel());
                                            html(" (");
                                            html(t.getInternalName());
                                            html(")");
                                        end();
                                    }
                                }
                            end();

                            includeStylesheet("/_resource/chosen/chosen.css");
                            includeScript("/_resource/chosen/chosen.jquery.min.js");
                            start("script", "type", "text/javascript");
                                write("(function() {");
                                    write("$('select[name=from]').chosen({ 'search_contains': true });");
                                write("})();");
                            end();

                            start("textarea",
                                    "class", "span6",
                                    "name", "where",
                                    "placeholder", "ID or Predicate (Leave Blank to Return All)",
                                    "rows", 4,
                                    "style", "margin-bottom: 4px; margin-top: 4px;");
                                html(where);
                            end();

                            tag("input", "class", "btn btn-primary", "type", "submit", "name", "action", "value", "Run");
                        end();

                        start("div", "class", "span6");
                            start("select", "name", "db", "style", "margin-bottom: 4px;");
                                start("option",
                                        "value", "",
                                        "selected", ObjectUtils.isBlank(databaseName) ? "selected" : null);
                                    html("Default");
                                end();

                                for (Database db : Database.Static.getAll()) {
                                    String dbName = db.getName();
                                    start("option",
                                            "value", dbName,
                                            "selected", dbName.equals(databaseName) ? "selected" : null);
                                        html(dbName);
                                    end();
                                }
                            end();

                            tag("br");
                            tag("input",
                                    "class", "input-small",
                                    "name", "sortField",
                                    "type", "text",
                                    "placeholder", "Sort",
                                    "value", sortField);
                            html(' ');
                            start("select", "class", "input-small", "name", "sortOrder");
                                for (SortOrder so : SortOrder.values()) {
                                    start("option",
                                            "selected", so.equals(sortOrder) ? "selected" : null,
                                            "value", so.name());
                                        html(so.displayName);
                                    end();
                                }
                            end();
                            html(' ');
                            tag("input",
                                    "class", "input-small",
                                    "name", "limit",
                                    "type", "text",
                                    "placeholder", "Limit",
                                    "value", limit);

                            tag("br");
                            tag("input",
                                    "class", "span6",
                                    "name", "additionalFields",
                                    "type", "text",
                                    "placeholder", "Additional Fields (Comma Separated)",
                                    "style", "margin-top: 4px;",
                                    "value", additionalFieldsString);

                            tag("br");
                            start("label", "class", "checkbox");
                                tag("input",
                                        "name", "ignoreReadConnection",
                                        "type", "checkbox",
                                        "style", "margin-top: 4px;",
                                        "value", "true",
                                        "checked", ignoreReadConnection ? "checked" : null);
                                html(" Ignore read-specific connection settings");
                            end();
                        end();
                    end();

                    start("h2", "style", "margin-top: 18px;").html("Filter").end();
                    start("div", "class", "row");
                        start("div", "class", "span12");
                            start("textarea",
                                    "class", "span12",
                                    "name", "filter",
                                    "placeholder", "Predicate (Leave Blank to Return All)",
                                    "rows", 2,
                                    "style", "margin-bottom: 4px;");
                                html(filter);
                            end();
                            tag("input", "class", "btn btn-primary", "type", "submit", "name", "action", "value", "Filter");
                        end();
                    end();
                end();

                try {
                    if (ObjectUtils.isBlank(filter)) {
                        PaginatedResult<Object> result = query.select(offset, limit);
                        List<Object> items = result.getItems();

                        if (offset == 0 && items.isEmpty()) {
                            start("p", "class", "alert").html("No matches!").end();

                        } else {
                            start("h2");
                                html("Result ");
                                object(result.getFirstItemIndex());
                                html(" to ");
                                object(result.getLastItemIndex());
                                html(" of ");
                                start("span", "class", "frame");
                                    start("a", "href", page.url("", "action", "count")).html("?").end();
                                end();
                            end();

                            start("div", "class", "btn-group");
                                start("a",
                                        "class", "btn" + (offset > 0 ? "" : " disabled"),
                                        "href", page.url("", "offset", 0));
                                    start("i", "class", "icon-fast-backward").end();
                                    html(" First");
                                end();
                                start("a",
                                        "class", "btn" + (result.hasPrevious() ? "" : " disabled"),
                                        "href", page.url("", "offset", result.getPreviousOffset()));
                                    start("i", "class", "icon-step-backward").end();
                                    html(" Previous");
                                end();
                                start("a",
                                        "class", "btn" + (result.hasNext() ? "" : " disabled"),
                                        "href", page.url("", "offset", result.getNextOffset()));
                                    html("Next ");
                                    start("i", "class", "icon-step-forward").end();
                                end();
                            end();

                            String[] additionalFields;
                            if (ObjectUtils.isBlank(additionalFieldsString)) {
                                additionalFields = new String[0];
                            } else {
                                additionalFields = additionalFieldsString.trim().split("\\s*,\\s*");
                            }

                            start("table", "class", "table table-condensed");
                                start("thead");
                                    start("tr");
                                        start("th").html("#").end();
                                        start("th").html("ID").end();
                                        start("th").html("Type").end();
                                        start("th").html("Label").end();
                                        for (String additionalField : additionalFields) {
                                            start("th").html(additionalField).end();
                                        }
                                    end();
                                end();
                                start("tbody");
                                    long offsetCopy = offset;
                                    for (Object item : items) {
                                        State itemState = State.getInstance(item);
                                        ObjectType itemType = itemState.getType();

                                        start("tr");
                                            start("td").html(++ offsetCopy).end();
                                            start("td").html(itemState.getId()).end();
                                            start("td").html(itemType != null ? itemType.getLabel() : null).end();
                                            start("td");
                                                start("a",
                                                        "target", "show",
                                                        "href", page.url("",
                                                                "action", "form",
                                                                "id", itemState.getId()));
                                                    html(itemState.getLabel());
                                                end();
                                            end();
                                            for (String additionalField : additionalFields) {
                                                start("td").html(itemState.getValue(additionalField)).end();
                                            }
                                        end();
                                    }
                                end();
                            end();
                        }

                    } else {
                        Predicate filterPredicate = PredicateParser.Static.parse(filter);
                        List<Object> items = new ArrayList<Object>();

                        start("h2");
                            html("Filtered Result");
                        end();

                        String[] additionalFields;
                        if (ObjectUtils.isBlank(additionalFieldsString)) {
                            additionalFields = new String[0];
                        } else {
                            additionalFields = additionalFieldsString.trim().split("\\s*,\\s*");
                        }

                        start("table", "class", "table table-condensed");
                            start("thead");
                                start("tr");
                                    start("th").html("#").end();
                                    start("th").html("ID").end();
                                    start("th").html("Type").end();
                                    start("th").html("Label").end();
                                    for (String additionalField : additionalFields) {
                                        start("th").html(additionalField).end();
                                    }
                                end();
                            end();
                            start("tbody");
                                long total = 0;
                                long matched = 0;

                                for (Object item : query.iterable(0)) {
                                    ++ total;
                                    if (total % 1000 == 0) {
                                        start("tr");
                                            start("td", "colspan", additionalFields.length + 4);
                                                html("Read ").object(total).html(" items");
                                            end();
                                        end();
                                        flush();
                                    }

                                    if (!PredicateParser.Static.evaluate(item, filterPredicate)) {
                                        continue;
                                    }

                                    ++ matched;
                                    items.add(item);
                                    State itemState = State.getInstance(item);
                                    ObjectType itemType = itemState.getType();

                                    start("tr");
                                        start("td").html(matched).end();
                                        start("td").html(itemState.getId()).end();
                                        start("td").html(itemType != null ? itemType.getLabel() : null).end();
                                        start("td");
                                            start("a", "href", "?where=id+%3D+" + itemState.getId(), "target", "_blank");
                                                html(itemState.getLabel());
                                            end();
                                        end();
                                        for (String additionalField : additionalFields) {
                                            start("td").html(itemState.getValue(additionalField)).end();
                                        }
                                    end();

                                    if (matched >= limit) {
                                        break;
                                    }
                                }
                            end();
                        end();
                    }

                } catch (Exception ex) {
                    start("div", "class", "alert alert-error");
                        object(ex);
                    end();
                }

            endPage();
        }
    }
}
