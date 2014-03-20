package com.psddev.dari.util;

/**
 * For when there is a deadlock while producing a value in a pull-through
 * class.
 *
 * @deprecated Use {@link com.google.common.cache.CacheBuilder} instead.
 */
@Deprecated
@SuppressWarnings("serial")
public class PullThroughDeadlockException extends RuntimeException {

    private final Object key;

    public PullThroughDeadlockException(Object key) {
        super(String.format("Deadlock detected while trying to produce [%s]!", key));
        this.key = key;
    }

    /** Returns the key that was being produced and caused the deadlock. */
    public Object getKey() {
        return key;
    }
}
