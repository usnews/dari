package com.psddev.dari.db;

import com.psddev.dari.util.ObjectUtils;

/**
 * Automatically changing sequence of numbers, similar to a {@code SEQUENCE}
 * in a RDBMS.
 */
public class Sequence extends Record {

    @Indexed(unique = true)
    @Required
    private String name;

    private double value;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    /** Returns the next number in this sequence. */
    public double next() {
        synchronized (this) {
            State state = getState();

            state.incrementAtomically("value", 1.0);
            state.saveImmediately();

            return ObjectUtils.to(double.class, state.get("value"));
        }
    }

    /** {@link Sequence} utility methods. */
    public static final class Static {

        /**
         * Returns the next number in the sequence with the given {@code name},
         * or the given {@code initialValue} if the sequence has never been
         * used before.
         *
         * @param name Can't be blank.
         */
        public static long nextLong(String name, long initialValue) {
            Sequence s = null;

            while (true) {
                s = Query.from(Sequence.class).where("name = ?", name).master().noCache().first();

                if (s != null) {
                    break;
                }

                s = new Sequence();
                s.setName(name);
                s.setValue(initialValue);

                try {
                    s.saveImmediately();
                    break;

                } catch (ValidationException error) {
                    if (s.getState().getErrors(s.getState().getField("name")).isEmpty()) {
                        throw error;
                    }
                }
            }

            return (long) s.next();
        }
    }
}
