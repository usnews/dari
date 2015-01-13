package com.psddev.dari.db;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.Stats;

enum FunnelCache {
    INSTANCE;

    private static final Stats STATS = new Stats("Funnel Cache");

    private static final String CACHE_EXPIRE_MILLISECONDS_SETTING = "dari/funnelCacheMillis";
    private static final String CONCURRENCY_LEVEL_SETTING = "dari/funnelCacheConcurrencyLevel";
    private static final String CACHE_SIZE_SETTING = "dari/funnelCacheSize";
    private static final long DEFAULT_CACHE_EXPIRE_MILLISECONDS = 1000;
    private static final int DEFAULT_CONCURRENCY_LEVEL = 20;
    private static final long DEFAULT_CACHE_SIZE = 10000;

    private final Cache<String, List<CachedObject>> objectCache = CacheBuilder.
            newBuilder().
            maximumSize(Settings.getOrDefault(Long.class, CACHE_SIZE_SETTING, DEFAULT_CACHE_SIZE)).
            concurrencyLevel(Settings.getOrDefault(Integer.class, CONCURRENCY_LEVEL_SETTING, DEFAULT_CONCURRENCY_LEVEL)).
            expireAfterWrite(Settings.getOrDefault(Long.class, CACHE_EXPIRE_MILLISECONDS_SETTING, DEFAULT_CACHE_EXPIRE_MILLISECONDS), TimeUnit.MILLISECONDS).build();

    static FunnelCache getInstance() {
        return INSTANCE;
    }

    public final List<CachedObject> get(final CachedObjectProducer producer) {
        Stats.Timer timer = STATS.startTimer();
        try {
            return objectCache.get(producer.getKey(), producer);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            timer.stop("Get");
        }
    }

    static final class CachedObject {

        private final UUID id;
        private final UUID typeId;
        private final Map<String, Object> values;
        private final Map<String, Object> extras;

        CachedObject(UUID id, UUID typeId, Map<String, Object> values, Map<String, Object> extras) {
            this.id = id;
            this.typeId = typeId;
            this.values = values;
            this.extras = extras;
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

        public Map<String, Object> getExtras() {
            return extras;
        }

        @Override
        public String toString() {
            return String.format("ID: %s, Type: %s", id, typeId);
        }
    }

    interface CachedObjectProducer extends Callable<List<CachedObject>> {
        String getKey();
    }
}
