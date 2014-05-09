package com.psddev.dari.db;

@SuppressWarnings("serial")
public class UnsupportedIndexException
        extends UnsupportedOperationException {

    private final Object reader;
    private final String field;

    public UnsupportedIndexException(Object reader, String field) {
        this.reader = reader;
        this.field = field;
    }

    public Object getReader() {
        return reader;
    }

    public String getField() {
        return field;
    }

    // --- Throwable support ---

    @Override
    public String getMessage() {
        return String.format(
                "[%s] field is not indexed in [%s]!",
                getField(), getReader().getClass().getSimpleName());
    }
}
