package com.psddev.dari.util;

import java.lang.ref.ReferenceQueue;
import java.util.Date;

/**
 * Factory for creating {@link PullThroughReference}.
 *
 * @deprecated Use {@link com.google.common.cache.CacheBuilder} instead.
 */
@Deprecated
public interface PullThroughReferenceFactory {

    public <K, V> PullThroughReference<K, V> create(ReferenceQueue<V> queue, K key, V value, Date produceDate);

    /**
     * {@link PullThroughReferenceFactory} implementation that
     * creates instances of {@link PullThroughReference.Soft}.
     */
    public static class Soft implements PullThroughReferenceFactory {

        @Override
        public <K, V> PullThroughReference<K, V> create(ReferenceQueue<V> queue, K key, V value, Date produceDate) {
            return new PullThroughReference.Soft<K, V>(queue, key, value, produceDate);
        }
    }

    /**
     * {@link PullThroughReferenceFactory} implementation that
     * creates instances of {@link PullThroughReference.Strong}.
     */
    public static class Strong implements PullThroughReferenceFactory {

        @Override
        public <K, V> PullThroughReference<K, V> create(ReferenceQueue<V> queue, K key, V value, Date produceDate) {
            return new PullThroughReference.Strong<K, V>(key, value, produceDate);
        }
    }

    /**
     * {@link PullThroughReferenceFactory} implementation that
     * creates instances of {@link PullThroughReference.Weak}.
     */
    public static class Weak implements PullThroughReferenceFactory {

        @Override
        public <K, V> PullThroughReference<K, V> create(ReferenceQueue<V> queue, K key, V value, Date produceDate) {
            return new PullThroughReference.Weak<K, V>(queue, key, value, produceDate);
        }
    }
}
