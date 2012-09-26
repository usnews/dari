package com.psddev.dari.db;

import com.psddev.dari.util.ErrorUtils;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Contains strings and references to other objects. */
public class ReferentialText extends AbstractList<Object> {

    private final List<Object> list = new ArrayList<Object>();

    // --- AbstractList support ---

    private Object checkItem(Object item) {
        ErrorUtils.errorIfNull(item, "item");

        if (item instanceof Reference) {
            return item;

        } else if (item instanceof Map) {
            Reference ref = new Reference();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) item).entrySet()) {
                Object key = entry.getKey();
                ref.put(key != null ? key.toString() : null, entry.getValue());
            }
            return ref;

        } else {
            return item.toString();
        }
    }

    @Override
    public void add(int index, Object item) {
        list.add(index, checkItem(item));
    }

    @Override
    public Object get(int index) {
        return list.get(index);
    }

    @Override
    public Object remove(int index) {
        return list.remove(index);
    }

    @Override
    public Object set(int index, Object item) {
        return list.set(index, checkItem(item));
    }

    @Override
    public int size() {
        return list.size();
    }
}
