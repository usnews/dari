package com.psddev.dari.util;

/** Thrown when an object could not be converted into an UUID. */
@SuppressWarnings("serial")
public class UuidFormatException extends IllegalArgumentException {

    /** Creates an instance with the given message. */
    public UuidFormatException(String message) {
        super(message);
    }
}
