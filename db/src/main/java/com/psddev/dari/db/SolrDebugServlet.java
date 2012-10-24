package com.psddev.dari.db;

import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;

import org.apache.solr.client.solrj.response.QueryResponse;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import com.psddev.dari.util.DebugFilter;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;

@DebugFilter.Path("db-solr")
@SuppressWarnings("serial")
public class SolrDebugServlet extends HttpServlet {

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        new DebugFilter.PageWriter(getServletContext(), request, response) {{
            startPage("Database", "Solr");

                SolrDatabase database = null;
                List<SolrDatabase> databases = Database.Static.getByClass(SolrDatabase.class);
                for (SolrDatabase db : databases) {
                    if (db.getName().equals(page.param(String.class, "db"))) {
                        database = db;
                        break;
                    }
                }
                if (database == null) {
                    database = Database.Static.getFirst(SolrDatabase.class);
                }

                String query = page.param(String.class, "query");
                String sort = page.param(String.class, "sort");

                start("h2").html("Query").end();
                start("form", "action", page.url(null), "class", "form-inline", "method", "post");

                    start("select", "class", "span6", "name", "db");
                        for (SolrDatabase db : Database.Static.getByClass(SolrDatabase.class)) {
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
                            "name", "query",
                            "placeholder", "Query",
                            "rows", 4,
                            "style", "margin: 4px 0; width: 100%;");
                        html(query);
                    end();

                    start("h3").html("Sort").end();
                    start("textarea",
                            "class", "span6",
                            "name", "sort",
                            "placeholder", "Sort",
                            "rows", 2,
                            "style", "margin: 4px 0; width: 100%;");
                        html(sort);
                    end();

                    tag("input", "class", "btn btn-primary", "type", "submit", "value", "Run");
                end();

                if (!ObjectUtils.isBlank(query)) {
                    start("h2").html("Result").end();
                    SolrServer server = database.openConnection();
                    SolrQuery solrQuery = new SolrQuery(query);
                    if (!StringUtils.isBlank(sort)) {
                        for (String sortField : sort.split(",")) {
                            String[] parameters = sortField.split(" ");
                            solrQuery.addSortField(parameters[0], SolrQuery.ORDER.valueOf(parameters[1]));
                        }
                    }
                    solrQuery.setParam("debugQuery", true);
                    try {
                        long startTime = System.nanoTime();
                        QueryResponse response = server.query(solrQuery, SolrRequest.METHOD.POST);
                        Map<String,String> explainMap = response.getExplainMap();
                        SolrDocumentList documents = response.getResults();

                        start("p");
                            html("Took ");
                            start("strong").object((System.nanoTime() - startTime) / 1e6).end();
                            html(" milliseconds to find ");
                            start("strong").object(documents.getNumFound()).end();
                            html(" documents.");
                        end();

                        start("table", "class", "table table-condensed");
                            start("thead");
                                start("tr");
                                    start("th").html("id").end();
                                    start("th").html("typeId").end();
                                    start("th").html("object").end();
                                    start("th").html("score").end();
                                end();
                            end();
                            start("tbody");
                                for (SolrDocument document : response.getResults()) {
                                    start("tr");
                                        for (Map.Entry<String, Object> entry : document.entrySet()) {
                                            start("td");
                                                object(entry.getValue());
                                            end();
                                        }
                                        start("td");
                                            Object id = document.get("id");
                                            if (explainMap.containsKey(id)) {
                                                write(explainMap.get(id).replaceAll("\n", "<br>").replaceAll(" ", "&nbsp;"));
                                            }
                                        end();
                                    end();
                                }
                            end();
                        end();

                    } catch (Exception ex) {
                        start("div", "class", "alert alert-error");
                            object(ex);
                        end();
                    }
                }

            endPage();
        }};
    }
}
