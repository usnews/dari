package com.psddev.dari.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Thrown when there are many causes to an exception. */
@SuppressWarnings("serial")
public class AggregateException extends RuntimeException {

    private final List<Throwable> causes;

    /** Creates an instance with the given {@code causes}. */
    public AggregateException(Iterable<? extends Throwable> causes) {
        List<Throwable> causesCopy = new ArrayList<Throwable>();
        if (causes != null) {
            for (Throwable cause : causes) {
                if (cause != null) {
                    causesCopy.add(cause);
                }
            }
        }
        this.causes = Collections.unmodifiableList(causesCopy);
    }

    /** Returns all causes. */
    public List<Throwable> getCauses() {
        return causes;
    }

    // --- RuntimeException support ---

    @Override
    public String getMessage() {
        List<? extends Throwable> causes = getCauses();
        if (causes.isEmpty()) {
            return null;

        } else {
            StringBuilder mb = new StringBuilder();
            for (Throwable cause : causes) {
                mb.append('\n').append(cause.getClass().getName());
                mb.append(": ").append(cause.getMessage());
            }
            return mb.toString();
        }
    }
}
