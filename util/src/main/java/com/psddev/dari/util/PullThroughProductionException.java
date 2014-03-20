package com.psddev.dari.util;

/**
 * For when a pull-through class couldn't produce a value.
 *
 * @deprecated Use {@link com.google.common.cache.CacheBuilder} instead.
 */
@Deprecated
public class PullThroughProductionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final Object key;

    public PullThroughProductionException(Object key, Throwable cause) {
        super(String.format("Can't produce [%s]!", key), cause);
        this.key = key;
    }

    /** Returns the key for a value that couldn't be produced. */
    public Object getKey() {
        return key;
    }
}
