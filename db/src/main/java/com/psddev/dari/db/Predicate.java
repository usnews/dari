package com.psddev.dari.db;

import com.psddev.dari.util.ObjectUtils;

/** Logical condition used with a {@linkplain Query query}. */
@Predicate.Embedded
public abstract class Predicate extends Record {

    private final String operator;

    /** Creates an instance with the given {@code operator}. */
    protected Predicate(String operator) {
        if (ObjectUtils.isBlank(operator)) {
            throw new IllegalArgumentException("Operator can't be blank!");
        }
        this.operator = operator;
    }

    protected Predicate() {
        this.operator = null;
    }

    /** Returns the operator used to test this predicate. */
    public String getOperator() {
        return operator;
    }

    /** @deprecated Use {@link PredicateParser.Static} instead. */
    @Deprecated
    public static final class Static {

        /** @deprecated Use {@link PredicateParser.Static#parse} instead. */
        @Deprecated
        public static Predicate parse(String predicateString, Object... parameters) {
            return PredicateParser.Static.parse(predicateString, parameters);
        }
    }
}
