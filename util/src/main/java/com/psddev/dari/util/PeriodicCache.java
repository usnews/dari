package com.psddev.dari.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Read-only map that will periodically update itself. */
public abstract class PeriodicCache<K, V> implements Map<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicCache.class);

    public static final double DEFAULT_INITIAL_DELAY = 0.0;
    public static final double DEFAULT_INTERVAL = 5.0;
    public static final String TASK_EXECUTOR_NAME = "Periodic Caches";

    private volatile Map<K, V> map = Collections.emptyMap();
    private final Task task;
    private volatile Date updateDate = new Date(0);

    /**
     * Creates an instance that will update every given {@code interval}
     * after the given {@code initialDelay} (in seconds).
     */
    public PeriodicCache(double initialDelay, double interval) {

        if (initialDelay == 0.0) {
            refresh();
            initialDelay = interval;
        }

        task = new Task(TASK_EXECUTOR_NAME, getClass().getName()) {
            @Override
            public void doTask() {
                refresh();
            }
        };

        task.scheduleAtFixedRate(initialDelay, interval);
    }

    /**
     * Creates an instance that will update every given {@code interval}
     * (in seconds).
     */
    public PeriodicCache(double interval) {
        this(DEFAULT_INITIAL_DELAY, interval);
    }

    /**
     * Creates an instance that will update every {@link #DEFAULT_INTERVAL}
     * seconds.
     */
    public PeriodicCache() {
        this(DEFAULT_INITIAL_DELAY, DEFAULT_INTERVAL);
    }

    /** Returns the task used to update the cache. */
    public Task getTask() {
        return task;
    }

    /** Returns the last time that this cache was updated. */
    public Date getUpdateDate() {
        return updateDate;
    }

    /**
     * Returns a map that will replace the existing cache.
     *
     * @return If {@code null}, the cache is not updated.
     */
    protected abstract Map<K, V> update();

    /** Refreshes the cache immediately. */
    public synchronized void refresh() {

        Map<K, V> oldMap = map;
        Map<K, V> newMap = update();
        if (newMap != null) {
            map = newMap;
            updateDate = new Date();

            if (LOGGER.isDebugEnabled()) {
                Set<K> newKeys = new HashSet<K>(newMap.keySet());
                int updateCount = 0, deleteCount = 0, sameCount = 0;
                for (Map.Entry<K, V> e : oldMap.entrySet()) {
                    K key = e.getKey();
                    if (newKeys.contains(key)) {
                        V oldValue = e.getValue();
                        V newValue = newMap.get(key);
                        if (ObjectUtils.equals(oldValue, newValue)) {
                            ++ sameCount;
                        } else {
                            ++ updateCount;
                        }
                        newKeys.remove(key);
                    } else {
                        ++ deleteCount;
                    }
                }
                LOGGER.debug(
                        "Total [{}]; New [{}]; Update [{}]; Delete [{}]; Same [{}]",
                        new Object[] {
                                newMap.size(),
                                newKeys.size(),
                                updateCount,
                                deleteCount,
                                sameCount });
            }
        }
    }

    // --- Map support ---

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public V get(Object key) {
        return map.get(key);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
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
        return map.size();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    // --- Object support ---

    @Override
    public boolean equals(Object object) {
        return map.equals(object);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }
}
