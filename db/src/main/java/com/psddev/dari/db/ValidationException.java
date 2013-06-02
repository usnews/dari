package com.psddev.dari.db;

import java.util.Collections;
import java.util.List;

/**
 * Thrown when there are invalid states that are about to be saved
 * to the database.
 */
public class ValidationException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    private final List<State> states;

    /** Creates an instance with the given invalid {@code states}. */
    public ValidationException(List<State> states) {
        this.states = states == null ?
                Collections.<State>emptyList() :
                Collections.unmodifiableList(states);
    }

    /** Returns an unmodifiable list of invalid states. */
    public List<State> getStates() {
        return states;
    }

    // --- Throwable support ---

    @Override
    public String getMessage() {
        StringBuilder message = new StringBuilder();
        for (State state : getStates()) {

            message.append('#');
            message.append(state.getId());
            message.append(':');

            for (ObjectField field : state.getErrorFields()) {
                String label = field.getLabel();
                for (String error : state.getErrors(field)) {
                    message.append(" [");
                    message.append(label);
                    message.append("] [");
                    message.append(error);
                    message.append("],");
                }
            }

            message.setLength(message.length() - 1);
            message.append("; ");
        }

        if (message.length() > 0) {
            message.setLength(message.length() - 2);
            return message.toString();

        } else {
            return null;
        }
    }
}
