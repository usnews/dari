package com.psddev.dari.db;

/** Thrown when use of a {@linkplain Sorter sorter} isn't supported. */
@SuppressWarnings("serial")
public class UnsupportedSorterException extends UnsupportedOperationException {

    private final Object reader;
    private final Sorter sorter;

    /**
     * Creates an instance based on the give {@code reader} and
     * {@code sorter}.
     */
    public UnsupportedSorterException(Object reader, Sorter sorter) {
        this.reader = reader;
        this.sorter = sorter;
    }

    /** Returns the reader that attempted the use of the sorter. */
    public Object getReader() {
        return reader;
    }

    /** Returns the unsupported sorter. */
    public Sorter getSorter() {
        return sorter;
    }

    // --- Throwable support ---

    @Override
    public String getMessage() {
        return String.format("[%s] isn't supported by [%s]!", getSorter(), getReader());
    }
}
