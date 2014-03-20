package com.psddev.dari.util;

import java.util.Date;

/**
 * Holds a value that's produced and cached on demand.
 *
 * @deprecated Use {@link Lazy} instead.
 */
@Deprecated
public abstract class PullThroughValue<T> {

    private static final PullThroughCache<PullThroughValue<?>, Object>
            VALUES = new PullThroughCache<PullThroughValue<?>, Object>() {

        @Override
        protected boolean isExpired(PullThroughValue<?> key, Date lastProduce) {
            return key.isExpired(lastProduce);
        }

        @Override
        public Object produce(PullThroughValue<?> key) throws Exception {
            return key.produce();
        }
    };

    /**
     * Returns the value, {@link #produce producing} it if necessary.
     * Subsequent calls to this method will return the same cached value
     * until it {@link #isExpired expires} or has been {@link #invalidate
     * invalidated}.
     */
    @SuppressWarnings("unchecked")
    public T get() {
        return (T) VALUES.get(this);
    }

    /**
     * Called by {@link #get} to check if the cached value should be
     * invalidated. Default implementation always returns {@code false},
     * causing the cache to never invalidate.
     */
    protected boolean isExpired(Date lastProduce) {
        return false;
    }

    /** Called by {@link #get} to produce the value. */
    protected abstract T produce() throws Exception;

    /**
     * Returns the last time that the value was produced.
     *
     * @return May be {@code null} if the value was never produced.
     */
    public Date getLastProduce() {
        return VALUES.getLastProduce(this);
    }

    /** Returns {@code true} if the value has been produced. */
    public boolean isProduced() {
        return getLastProduce() != null;
    }

    /** Invalidates all cached value. */
    public synchronized void invalidate() {
        VALUES.invalidate(this);
    }

    // --- Deprecated ---

    /** @deprecated Use {@link #getLastProduce} instead. */
    @Deprecated
    public Date getLastProduceDate() {
        return getLastProduce();
    }
}
