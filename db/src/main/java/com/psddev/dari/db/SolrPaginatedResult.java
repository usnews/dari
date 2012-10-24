
package com.psddev.dari.db;

import com.psddev.dari.util.HtmlObject;
import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.StringUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.FacetField;

/**
 * Paginated result for Solr that provides access to
 * faceted results.
 *
 * @author jcollins
 */
public class SolrPaginatedResult<E> extends PaginatedResult<E> implements HtmlObject {

    Class<?> _klass;
    List<FacetField> _facetedFields;

    public SolrPaginatedResult(
            long offset, int limit, long count, List<E> items, List<FacetField> facetedFields,
            Class<?> klass) {
        super(offset, limit, count, items);

        _klass = klass;
        _facetedFields = facetedFields;
    }

    public SolrPaginatedResult(
            long offset, int limit, long count, List<E> items, List<FacetField> facetedFields,
            Class<?> klass, SolrQuery solrQuery) {
        super(offset, limit, count, items);

        _klass = klass;
        _facetedFields = facetedFields;
        this.solrQuery = solrQuery;
    }

    public List<DariFacetField> getFacetedFields() {
        List<DariFacetField> fields = new ArrayList<DariFacetField>();
        if (_facetedFields != null) {
            for(FacetField field : _facetedFields) {
                fields.add(new DariFacetField(_klass, field));
            }
        }

        return fields;
    }

    private transient SolrQuery solrQuery;

    public SolrQuery getSolrQuery() {
        return solrQuery;
    }

    public void setSolrQuery(SolrQuery solrQuery) {
        this.solrQuery = solrQuery;
    }

    @Override
    public void format(HtmlWriter writer) throws IOException {
        writer.start("p");
            writer.start("code").html(this.getClass().getName()).end();
            writer.html(' ');
            writer.start("strong").html(this.getFirstItemIndex()).end();
            writer.html(" to ");
            writer.start("strong").html(this.getLastItemIndex()).end();
            writer.html(" of ");
            writer.start("strong").html(this.getCount()).end();
        writer.end();

        if (Settings.isDebug() && this.getSolrQuery() != null) {
            String solrFullQuery = this.getSolrQuery().toString();
            String solrQuery = this.getSolrQuery().getQuery();
            String solrSort = StringUtils.join(this.getSolrQuery().getSortFields(), ",");

            // Use a form instead of a link if the URL will be too long.
            if (solrFullQuery.length() > 2000) {
                writer.start("span", "class", "solr-query");
                    writer.html("Solr Query: ");
                    writer.html(StringUtils.decodeUri(solrFullQuery));

                    writer.start("form",
                            "class", "solrQueryDebugForm",
                            "method", "post",
                            "action", "/_debug/db-solr",
                            "target", "query");
                        writer.tag("input", "type", "hidden", "name", "query", "value", StringUtils.decodeUri(solrQuery));
                        writer.tag("input", "type", "hidden", "name", "sort", "value", StringUtils.decodeUri(solrSort));
                        writer.tag("input", "class", "btn", "type", "submit", "value", "Execute");
                    writer.end();
                writer.end();

            } else {
                writer.html("Solr Query: ");
                writer.html(StringUtils.decodeUri(solrFullQuery));
                writer.html(" (");
                    writer.start("a",
                            "href", StringUtils.addQueryParameters("/_debug/db-solr", "query", solrQuery, "sort", solrSort),
                            "target", "query");
                        writer.html("Execute");
                    writer.end();
                writer.html(")");
            }
        }

        writer.start("ol");
            for (Object item : this.getItems()) {
                writer.start("li").object(item).html(" Solr Score: " + SolrDatabase.Static.getScore(item)).end();
            }
        writer.end();
    }

    static public class DariFacetField {

        private Class<?> _klass;
        private FacetField _field;

        public DariFacetField(Class<?> klass, FacetField field) {
            this._klass = klass;
            this._field = field;
        }

        public String getName() {
            return _field.getName();
        }

        public Long getCount() {
            return new Long(_field.getValueCount());
        }

        public <T> List<T> getObjects() {
            Map<String, FacetField.Count> index = new HashMap<String, FacetField.Count>();

            List<String> ids = new ArrayList<String>();
            for(FacetField.Count c : _field.getValues()) {
                index.put(c.getName(), c);
                ids.add(c.getName());
            }

            @SuppressWarnings("unchecked")
            List<T> objects = (List<T>) (_klass == null || _klass == Query.class ?
                    Query.fromAll().where("id = ?", ids).selectAll() :
                    Query.from(_klass).where("id = ?", ids).selectAll());

            if (objects != null) {
                for (Object o : objects) {
                    Record record = (Record)o;
                    FacetField.Count c = index.get(record.getId().toString());
                    record.getState().getExtras().put("count", new Long(c.getCount()));
                }
            }

            return objects;
        }
    }
}
