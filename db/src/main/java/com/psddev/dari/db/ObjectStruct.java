package com.psddev.dari.db;

import java.util.List;

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
}
