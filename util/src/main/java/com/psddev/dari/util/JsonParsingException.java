package com.psddev.dari.util;

/**
 * Thrown when the {@linkplain JsonProcessor JSON processor} encounters
 * a parsing error.
 */
@SuppressWarnings("serial")
public class JsonParsingException extends RuntimeException {

    public JsonParsingException(String message, Throwable cause) {
        super(message, cause);
    }
}
