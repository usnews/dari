package com.psddev.dari.db;

public class UnsupportedIndexException
        extends UnsupportedOperationException {

    private final Object _reader;
    private final String _field;

    public UnsupportedIndexException(Object reader, String field) {
        _reader = reader;
        _field = field;
    }

    public Object getReader() {
        return _reader;
    }

    public String getField() {
        return _field;
    }

    // --- Throwable support ---

    @Override
    public String getMessage() {
        return String.format(
                "[%s] field is not indexed in [%s]!",
                _field, _reader.getClass().getSimpleName());
    }
}
