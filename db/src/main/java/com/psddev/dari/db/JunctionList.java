package com.psddev.dari.db;

import java.util.AbstractList;
import java.util.List;

import com.psddev.dari.util.Lazy;

/**
 * Internal list implementation that automatically loads a list of objects
 * using a junction query.
 */
class JunctionList extends AbstractList<Object> {

    private final State state;
    private final ObjectField field;

    private final Lazy<List<Object>> list = new Lazy<List<Object>>() {

        @Override
        protected List<Object> create() {
            return field.findJunctionItems(state);
        }
    };

    public JunctionList(State state, ObjectField field) {
        this.state = state;
        this.field = field;
    }

    // --- AbstractList support ---

    @Override
    public void add(int index, Object item) {
        list.get().add(index, item);
    }

    @Override
    public Object get(int index) {
        return list.get().get(index);
    }

    @Override
    public Object remove(int index) {
        return list.get().remove(index);
    }

    @Override
    public Object set(int index, Object item) {
        return list.get().set(index, item);
    }

    @Override
    public int size() {
        return list.get().size();
    }
}
