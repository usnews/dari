
package com.psddev.dari.db;

import com.psddev.dari.util.PaginatedResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.response.FacetField;

/**
 * Paginated result for Solr that provides access to
 * faceted results.
 *
 * @author jcollins
 */
public class SolrPaginatedResult<E> extends PaginatedResult<E> {

    Class _klass;
    List<FacetField> _facetedFields;

    public SolrPaginatedResult(
            long offset, int limit, long count, List<E> items, List<FacetField> facetedFields,
            Class klass) {
        super(offset, limit, count, items);

        _klass = klass;
        _facetedFields = facetedFields;
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

    static public class DariFacetField {

        private Class _klass;
        private FacetField _field;

        public DariFacetField(Class klass, FacetField field) {
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

            List<T> objects = null;
            if(_klass == null || _klass == Query.class){
                objects = (List<T>)Query.fromAll().where("id = ?", ids).selectAll();
            }else{
                objects = Query.from(_klass).where("id = ?", ids).selectAll();
            }

            if(objects != null){
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
