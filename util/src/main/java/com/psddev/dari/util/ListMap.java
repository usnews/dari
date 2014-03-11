package com.psddev.dari.util;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

/** Map implementation backed by a list. */
public abstract class ListMap<K, V> implements Map<K, V> {

    private final List<V> list;

    /** Creates an instanced backed by the given {@code list}. */
    public ListMap(List<V> list) {
        this.list = list;
    }

    /** Returns the key associated with the given {@code value}. */
    public abstract K getKey(V value);

    /** Returns the backing list. */
    public List<V> getList() {
        return list;
    }

    // --- Map support ---

    @Override
    public void clear() {
        list.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        for (V item : list) {
            if (ObjectUtils.equals(key, getKey(item))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        for (V item : list) {
            if (ObjectUtils.equals(value, item)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return new ListMapSet<Map.Entry<K, V>>() {
            @Override
            public Map.Entry<K, V> createNext(V value) {
                return new AbstractMap.SimpleEntry<K, V>(getKey(value), value);
            }
        };
    }

    @Override
    public V get(Object key) {
        for (V item : list) {
            if (ObjectUtils.equals(key, getKey(item))) {
                return item;
            }
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return new ListMapSet<K>() {
            @Override
            public K createNext(V value) {
                return getKey(value);
            }
        };
    }

    @Override
    public V put(K key, V value) {
        for (ListIterator<V> i = list.listIterator(); i.hasNext();) {
            V item = i.next();

            if (ObjectUtils.equals(key, getKey(item))) {
                i.set(value);
                return item;
            }
        }

        list.add(value);
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public V remove(Object key) {
        for (Iterator<V> i = list.iterator(); i.hasNext();) {
            V item = i.next();

            if (ObjectUtils.equals(key, getKey(item))) {
                i.remove();
                return item;
            }
        }

        return null;
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public Collection<V> values() {
        return list;
    }

    // --- Object support ---

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;

        } else if (other instanceof Map) {
            return entrySet().equals(((Map<?, ?>) other).entrySet());

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return entrySet().hashCode();
    }

    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();
        string.append('{');

        if (!list.isEmpty()) {
            for (V item : list) {
                string.append(getKey(item));
                string.append('=');
                string.append(item);
                string.append(',');
            }

            string.setLength(string.length() - 1);
        }

        string.append('}');
        return string.toString();
    }

    private abstract class ListMapSet<T> extends AbstractSet<T> {

        protected abstract T createNext(V value);

        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>() {

                private final Iterator<V> listIterator = list.iterator();

                @Override
                public boolean hasNext() {
                    return listIterator.hasNext();
                }

                @Override
                public T next() {
                    return createNext(listIterator.next());
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public int size() {
            return list.size();
        }
    }
}
