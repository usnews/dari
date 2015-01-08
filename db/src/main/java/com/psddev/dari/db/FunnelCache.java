package com.psddev.dari.db;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.psddev.dari.util.Settings;

enum FunnelCache {
    INSTANCE;

    private static final String CACHE_EXPIRE_MILLISECONDS_SETTING = "dari/funnelCacheMillis";
    private static final String CACHE_SIZE_SETTING = "dari/funnelCacheSize";
    private static final long DEFAULT_CACHE_EXPIRE_MILLISECONDS = 1000;
    private static final long DEFAULT_CACHE_SIZE = 10000;

    private final Cache<String, List<CachedObject>> objectCache = CacheBuilder.
            newBuilder().
            maximumSize(Settings.getOrDefault(Long.class, CACHE_SIZE_SETTING, DEFAULT_CACHE_SIZE)).
            expireAfterWrite(Settings.getOrDefault(Long.class, CACHE_EXPIRE_MILLISECONDS_SETTING, DEFAULT_CACHE_EXPIRE_MILLISECONDS), TimeUnit.MILLISECONDS).build();

    private final ConcurrentHashMap<String, Object> queryLocks = new ConcurrentHashMap<String, Object>();

    static FunnelCache getInstance() {
        return INSTANCE;
    }

    public final List<CachedObject> get(String sqlQuery, Query<?> query, CachedObjectProducer producer) {
        if (sqlQuery == null) {
            throw new NullPointerException();
        }
        List<CachedObject> objects = objectCache.getIfPresent(sqlQuery);
        if (objects == null) {
            Object lock = new Object();
            Object existingLock = queryLocks.putIfAbsent(sqlQuery, lock);
            lock = existingLock != null ? existingLock : lock;
            synchronized (lock) {
                try {
                    objects = producer.apply(sqlQuery, query);
                    objectCache.put(sqlQuery, objects);
                } finally {
                    queryLocks.remove(sqlQuery);
                }
            }
        }
        return objects;
    }

    static final class CachedObject {

        private final UUID id;
        private final UUID typeId;
        private final Map<String, Object> values;
        private final byte[] data;

        CachedObject(UUID id, UUID typeId, Map<String, Object> values, byte[] data) {
            this.id = id;
            this.typeId = typeId;
            this.values = values;
            this.data = data;
        }

        public UUID getId() {
            return id;
        }

        public UUID getTypeId() {
            return typeId;
        }

        public Map<String, Object> getValues() {
            return values;
        }

        public byte[] getData() {
            return data;
        }

        @Override
        public String toString() {
            return String.format("ID: %s, Type: %s", id, typeId);
        }
    }

    interface CachedObjectProducer {
        List<CachedObject> apply(String queryString, Query<?> query);
    }
}
