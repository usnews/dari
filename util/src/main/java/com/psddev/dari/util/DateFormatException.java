package com.psddev.dari.util;

/** Thrown when an object could not be converted into a date. */
@SuppressWarnings("serial")
public class DateFormatException extends IllegalArgumentException {

    /** Creates an instance with the given message. */
    public DateFormatException(String message) {
        super(message);
    }
}
