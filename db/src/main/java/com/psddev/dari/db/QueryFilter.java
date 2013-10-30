package com.psddev.dari.db;

/**
 * For {@link Query#selectFiltered}.
 */
public interface QueryFilter<E> {

    /**
     * Returns {@code true} if the given {@code item} should be included in
     * the result.
     *
     * @param item Never {@code null}.
     */
    public boolean include(E item);
}
