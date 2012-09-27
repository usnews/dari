package com.psddev.dari.db;

import com.psddev.dari.util.CodeUtils;
import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.ObjectUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@DebugFilter.Path("db-schema")
@SuppressWarnings("serial")
public class SchemaDebugServlet extends HttpServlet {

    // --- HttpServlet support ---

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        new DebugFilter.PageWriter(getServletContext(), request, response) {{

            Database database = Database.Static.getDefault();
            List<ObjectType> types = new ArrayList<ObjectType>(database.getEnvironment().getTypes());
            Collections.sort(types);

            Set<ObjectType> selectedTypes = new HashSet<ObjectType>();
            List<UUID> typeIds = page.params(UUID.class, "typeId");
            if (typeIds != null) {
                Set<UUID> typeIdsSet = new HashSet<UUID>();
                typeIdsSet.addAll(typeIds);
                for (ObjectType t : types) {
                    if (typeIdsSet.contains(t.getId())) {
                        selectedTypes.add(t);
                    }
                }
            }

            startPage("Database", "Schema");

                start("form", "method", "get");
                    start("select", "multiple", "multiple", "name", "typeId", "style", "width: 90%;");
                        for (ObjectType t : types) {
                            start("option",
                                    "selected", selectedTypes.contains(t) ? "selected" : null,
                                    "value", t.getId());
                                html(t.getDisplayName());
                                html(" (").html(t.getInternalName()).html(")");
                            end();
                        }
                    end();
                    tag("br");
                    tag("input", "class", "btn", "type", "submit", "value", "View");
                end();

                includeStylesheet("/_resource/chosen/chosen.css");
                includeScript("/_resource/chosen/chosen.jquery.min.js");
                start("script", "type", "text/javascript");
                    write("(function() {");
                        write("$('select[name=typeId]').chosen({ 'search_contains': true });");
                    write("})();");
                end();

                start("style", "type", "text/css");
                    write(".column { display: table-cell; padding-right: 15em; text-align: center; vertical-align: top; }");
                    write(".column dl { margin-bottom: 0; }");
                    write(".type { border: 1px solid black; display: inline-block; margin-bottom: 5em; padding: 0.5em; text-align: left; }");
                    write(".type h2 { white-space: nowrap; }");
                    write(".type dt { margin-bottom: 5px; }");
                    write(".type dd:last-child table { margin-bottom: 0; }");
                    write(".type .reference { color: white; white-space: nowrap; }");
                end();

                start("div", "class", "types");
                    Set<ObjectType> allTypes = new HashSet<ObjectType>();
                    Set<ObjectType> currentTypes = new HashSet<ObjectType>(selectedTypes);

                    while (!currentTypes.isEmpty()) {
                        start("div", "class", "column");
                            allTypes.addAll(currentTypes);
                            Set<ObjectType> nextTypes = new LinkedHashSet<ObjectType>();

                            for (ObjectType t : currentTypes) {

                                Map<String, List<ObjectField>> fieldsByClass = new LinkedHashMap<String, List<ObjectField>>();
                                for (ObjectField field : t.getFields()) {
                                    String declaringClass = field.getJavaDeclaringClassName();
                                    List<ObjectField> fields = fieldsByClass.get(declaringClass);
                                    if (fields == null) {
                                        fields = new ArrayList<ObjectField>();
                                        fieldsByClass.put(declaringClass, fields);
                                    }
                                    fields.add(field);
                                }

                                start("div").end();
                                start("div", "class", "type", "id", "type-" + t.getId());
                                    start("h2").html(t.getDisplayName()).end();
                                    start("dl");

                                        for (Map.Entry<String, List<ObjectField>> entry : fieldsByClass.entrySet()) {
                                            String className = entry.getKey();
                                            File source = CodeUtils.getSource(className);

                                            start("dt");
                                                if (source == null) {
                                                    html(className);

                                                } else {
                                                    start("a",
                                                            "target", "_blank",
                                                            "href", DebugFilter.Static.getServletPath(page.getRequest(), "code",
                                                                    "file", source));
                                                        html(className);
                                                    end();
                                                }
                                            end();

                                            start("dd");
                                                start("table", "class", "table table-condensed");
                                                    start("tbody");
                                                        for (ObjectField field : entry.getValue()) {
                                                            if (page.param(boolean.class, "nf") && !field.getInternalItemType().equals("record")) {
                                                                continue;
                                                            }

                                                            start("tr");
                                                                start("td").html(field.getInternalName()).end();
                                                                start("td").html(field.getInternalType()).end();

                                                                start("td");
                                                                    if (ObjectField.RECORD_TYPE.equals(field.getInternalItemType())) {
                                                                        Set<ObjectType> itemTypes = field.getTypes();
                                                                        if (!ObjectUtils.isBlank(itemTypes)) {
                                                                            for (ObjectType itemType : itemTypes) {
                                                                                if (!allTypes.contains(itemType)) {
                                                                                    nextTypes.add(itemType);
                                                                                }
                                                                                start("a",
                                                                                        "class", "label reference",
                                                                                        "data-typeId", itemType.getId(),
                                                                                        "href", page.url(null, "typeId", itemType.getId()));
                                                                                    html(itemType.getDisplayName());
                                                                                end();
                                                                            }
                                                                        }
                                                                    }
                                                                end();
                                                            end();
                                                        }
                                                    end();
                                                end();
                                            end();
                                        }

                                    end();
                                end();
                            }

                            currentTypes = nextTypes;
                        end();
                    }
                end();

                includeScript("/_resource/dari/db-schema.js");

            endPage();
        }};
    }
}
