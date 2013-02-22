package com.psddev.dari.util;

/**
 * Thrown when the {@linkplain Converter converter} fails to convert an
 * object into an instance of another type.
 */
@SuppressWarnings("serial")
public class ConversionException extends RuntimeException {

    private final Object defaultValue;

    /** Creates a new instance with the given {@code defaultValue}. */
    public ConversionException(Object defaultValue) {
        super();
        this.defaultValue = defaultValue;
    }

    /**
     * Creates a new instance with the given {@code message} and
     * {@code defaultValue}.
     */
    public ConversionException(String message, Object defaultValue) {
        super(message);
        this.defaultValue = defaultValue;
    }

    /**
     * Creates a new instance with the given {@code message}, {@code cause},
     * and {@code defaultValue}.
     */
    public ConversionException(String message, Throwable cause, Object defaultValue) {
        super(message, cause);
        this.defaultValue = defaultValue;
    }

    /**
     * Creates a new instance with the given {@code cause} and
     * {@code defaultValue}.
     */
    public ConversionException(Throwable cause, Object defaultValue) {
        super(cause);
        this.defaultValue = defaultValue;
    }

    /** Creates a new instance. */
    public ConversionException() {
        this((Object) null);
    }

    /** Creates a new instance with the given {@code message}. */
    public ConversionException(String message) {
        this(message, (Object) null);
    }

    /**
     * Creates a new instance with the given {@code message} and
     * {@code cause}.
     */
    public ConversionException(String message, Throwable cause) {
        this(message, cause, (Object) null);
    }

    /** Creates a new instance with the given {@code cause}. */
    public ConversionException(Throwable cause) {
        this(cause, (Object) null);
    }

    /** Returns the default value for when the conversion fails. */
    public Object getDefaultValue() {
        return defaultValue;
    }
}
