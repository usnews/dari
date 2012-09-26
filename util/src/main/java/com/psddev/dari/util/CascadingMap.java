package com.psddev.dari.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/** Read-only map implementation that combines entries from multiple maps. */
public class CascadingMap<K, V> implements Map<K, V> {

    private final List<Map<K, V>>
            _sources = new CopyOnWriteArrayList<Map<K, V>>();

    /**
     * Returns a modifiable list of all the sources that are used to query
     * the entries. Items earlier in the list are queried first.
     */
    public List<Map<K, V>> getSources() {
        return _sources;
    }

    /** Creates an instance without any sources. */
    public CascadingMap() {
    }

    /** Creates an instance with the given array of {@code sources}. */
    public CascadingMap(Map<K, V>... sources) {
        for (Map<K, V> source : sources) {
            _sources.add(source);
        }
    }

    // Combines all the sources, for when an unified view is required.
    private Map<K, V> combine() {
        Map<K, V> combined = new HashMap<K, V>();
        for (int i = _sources.size() - 1; i >= 0; -- i) {
            combined.putAll(_sources.get(i));
        }
        return combined;
    }

    // --- Map support ---

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Object key) {
        for (Map<K, V> source : _sources) {
            if (source.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return combine().containsValue(value);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return combine().entrySet();
    }

    @Override
    public V get(Object key) {
        for (Map<K, V> source : _sources) {
            if (source.containsKey(key)) {
                return source.get(key);
            }
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        for (Map<K, V> source : _sources) {
            if (!source.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Set<K> keySet() {
        return combine().keySet();
    }

    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return combine().size();
    }

    @Override
    public Collection<V> values() {
        return combine().values();
    }

    // --- Object support ---

    @Override
    public boolean equals(Object object) {
        return this == object
                || (object instanceof Map && combine().equals(object));
    }

    @Override
    public int hashCode() {
        return combine().hashCode();
    }
}
