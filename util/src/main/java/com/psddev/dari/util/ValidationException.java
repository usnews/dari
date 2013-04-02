package com.psddev.dari.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * For when there are multiple illegal arguments.
 * @author Hyoo Lim
 */
@SuppressWarnings("serial")
public class ValidationException extends IllegalArgumentException {
    private final List<String> _messages;

    /** Creates an exception without any messages. */
    public ValidationException() {
        super();
        _messages = new ArrayList<String>();
    }

    /** Creates an exception with the given list of messages. */
    public ValidationException(List<String> messages) {
        super();
        _messages = Collections.unmodifiableList(messages);
    }

    /** Creates an exception with the given list of messages and cause. */
    public ValidationException(List<String> messages, Throwable cause) {
        super(cause);
        _messages = Collections.unmodifiableList(messages);
    }

    /** Creates an exception with the given cause. */
    public ValidationException(Throwable cause) {
        super(cause);
        _messages = new ArrayList<String>();
    }

    /** Gets a list of messages. */
    public List<String> getMessages() {
        return _messages;
    }

    // --- Throwable support ---
    public String getMessage() {
        StringBuilder mb = new StringBuilder();
        for (String m : _messages) {
            mb.append(m).append("\n");
        }
        return mb.toString();
    }
}
