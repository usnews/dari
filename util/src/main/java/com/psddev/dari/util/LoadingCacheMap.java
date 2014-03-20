package com.psddev.dari.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.google.common.cache.LoadingCache;

/**
 * Wrapper around {@link LoadingCache} so that it can be used like a
 * {@link ConcurrentMap}.
 */
public class LoadingCacheMap<K, V> implements ConcurrentMap<K, V> {

    private final Class<K> keyClass;
    private final LoadingCache<K, V> cache;
    private final ConcurrentMap<K, V> cacheMap;

    public LoadingCacheMap(Class<K> keyClass, LoadingCache<K, V> cache) {
        this.keyClass = keyClass;
        this.cache = cache;
        this.cacheMap = cache.asMap();
    }

    // --- ConcurrentMap support ---

    @Override
    public V putIfAbsent(K key, V value) {
        return cacheMap.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return cacheMap.remove(key, value);
    }

    @Override
    public V replace(K key, V value) {
        return cacheMap.replace(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return cacheMap.replace(key, oldValue, newValue);
    }

    // --- Map support ---

    @Override
    public void clear() {
        cacheMap.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return cacheMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return cacheMap.containsValue(value);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return cacheMap.entrySet();
    }

    @Override
    public V get(Object key) {
        return keyClass.isInstance(key) ? cache.getUnchecked(keyClass.cast(key)) : null;
    }

    @Override
    public boolean isEmpty() {
        return cacheMap.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return cacheMap.keySet();
    }

    @Override
    public V put(K key, V value) {
        return cacheMap.put(key, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        cacheMap.putAll(map);
    }

    @Override
    public V remove(Object key) {
        return cacheMap.remove(key);
    }

    @Override
    public int size() {
        return cacheMap.size();
    }

    @Override
    public Collection<V> values() {
        return cacheMap.values();
    }

    // --- Object support ---

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;

        } else if (other instanceof Map) {
            return cacheMap.equals(other);

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return cacheMap.hashCode();
    }

    @Override
    public String toString() {
        return cacheMap.toString();
    }
}
