package com.psddev.dari.db;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public interface ObjectStruct {

    /** Returns the environment that owns this object. */
    public DatabaseEnvironment getEnvironment();

    /** Returns a list of all fields. */
    public List<ObjectField> getFields();

    /** Returns the field with the given {@code name}. */
    public ObjectField getField(String name);

    /** Sets the list of all the fields. */
    public void setFields(List<ObjectField> fields);

    /** Returns a list of all indexes. */
    public List<ObjectIndex> getIndexes();

    /** Returns the index with the given {@code name}. */
    public ObjectIndex getIndex(String name);

    /** Sets the list of all indexes. */
    public void setIndexes(List<ObjectIndex> indexes);

    /**
     * {@link ObjectStruct} utility methods.
     */
    public static final class Static {

        /**
         * Returns all fields that are indexed in the given {@code struct}.
         *
         * @param struct Can't be {@code null}.
         * @return Never {@code null}.
         */
        public static List<ObjectField> findIndexedFields(ObjectStruct struct) {
            Set<String> indexed = new HashSet<String>();

            for (ObjectIndex index : struct.getIndexes()) {
                indexed.addAll(index.getFields());
            }

            List<ObjectField> fields = struct.getFields();

            for (Iterator<ObjectField> i = fields.iterator(); i.hasNext(); ) {
                ObjectField field = i.next();

                if (!indexed.contains(field.getInternalName())) {
                    i.remove();
                }
            }

            return fields;
        }
    }
}
