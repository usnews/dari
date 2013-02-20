package com.psddev.dari.db;

/**
 * Thrown when use of an {@linkplain Predicate predicate} isn't
 * supported.
 */
@SuppressWarnings("serial")
public class UnsupportedPredicateException extends UnsupportedOperationException {

    private final Object reader;
    private final Predicate predicate;

    /**
     * Creates an instance based on the given {@code reader} and
     * {@code predicate}.
     */
    public UnsupportedPredicateException(Object reader, Predicate predicate) {
        this.reader = reader;
        this.predicate = predicate;
    }

    /** Returns the reader that attempted the use of the predicate. */
    public Object getReader() {
        return reader;
    }

    /** Returns the unsupported predicate. */
    public Predicate getPredicate() {
        return predicate;
    }

    // --- Throwable support ---

    @Override
    public String getMessage() {
        return String.format("[%s] isn't supported by [%s]!", getPredicate(), getReader());
    }
}
