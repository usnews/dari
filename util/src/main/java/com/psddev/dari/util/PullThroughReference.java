package com.psddev.dari.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Date;

/**
 * Reference to a value created in the pull-through classes.
 *
 * @deprecated Use {@link com.google.common.cache.CacheBuilder} instead.
 */
@Deprecated
public interface PullThroughReference<K, V> {

    /**
     * Returns the value.
     *
     * @return May be {@code null}.
     */
    public V get();

    /**
     * Returns the key associated with the value.
     *
     * @return May be {@code null}.
     */
    public K getKey();

    /**
     * Returns when the value was produced.
     *
     * @return Never {@code null}.
     */
    public Date getProduceDate();

    /** Returns {@code true} if the value is available to use. */
    public boolean isAvailable();

    /**
     * {@link PullThroughReference} implementation that may lose its
     * value under memory pressure. This class uses {@link SoftReference}.
     */
    public static class Soft<K, V> extends SoftReference<V> implements PullThroughReference<K, V> {

        private final K key;
        private final Long produceTime;

        public Soft(ReferenceQueue<V> queue, K key, V value, Date produceDate) {
            super(value, queue);
            this.key = key;
            this.produceTime = produceDate != null ? produceDate.getTime() : null;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public Date getProduceDate() {
            return produceTime != null ? new Date(produceTime) : null;
        }

        @Override
        public boolean isAvailable() {
            return !isEnqueued();
        }
    }

    /**
     * {@link PullThroughReference} implementation that holds onto
     * its value forever.
     */
    public static class Strong<K, V> implements PullThroughReference<K, V> {

        private final K key;
        private final V value;
        private final Long produceTime;

        public Strong(K key, V value, Date produceDate) {
            this.key = key;
            this.value = value;
            this.produceTime = produceDate != null ? produceDate.getTime() : null;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V get() {
            return value;
        }

        @Override
        public Date getProduceDate() {
            return produceTime != null ? new Date(produceTime) : null;
        }

        @Override
        public boolean isAvailable() {
            return true;
        }
    }

    /**
     * {@link PullThroughReference} implementation that may lose its
     * value when there are no other references. This class uses
     * {@link WeakReference}.
     */
    public static class Weak<K, V> extends WeakReference<V> implements PullThroughReference<K, V> {

        private final K key;
        private final Long produceTime;

        public Weak(ReferenceQueue<V> queue, K key, V value, Date produceDate) {
            super(value, queue);
            this.key = key;
            this.produceTime = produceDate != null ? produceDate.getTime() : null;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public Date getProduceDate() {
            return produceTime != null ? new Date(produceTime) : null;
        }

        @Override
        public boolean isAvailable() {
            return !isEnqueued();
        }
    }
}
