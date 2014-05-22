package com.psddev.dari.db;

public interface VisibilityValues extends Recordable {

    Iterable<?> findVisibilityValues(ObjectIndex index);
}
