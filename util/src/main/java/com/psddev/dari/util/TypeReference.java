package com.psddev.dari.util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * References a generic type.
 *
 * <p>This class only implements {@link Comparable} to force the subclass
 * to supply the type argument. It's not actually valid to compare an instance
 * of this class to another.</p>
 *
 * @see <a href="http://gafter.blogspot.com/2006/12/super-type-tokens.html">Super Type Tokens</a>
 */
public abstract class TypeReference<T> implements Comparable<TypeReference<T>> {

    private final Type type;

    protected TypeReference() {
        type = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * Returns the generic type.
     *
     * @return Never {@code null}.
     */
    public Type getType() {
        return type;
    }

    // --- Comparable support ---

    @Override
    public int compareTo(TypeReference<T> object) {
        return 0;
    }

    // --- Object support ---

    @Override
    public int hashCode() {
        return getType().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;

        } else if (other instanceof TypeReference) {
            return getType().equals(((TypeReference<?>) other).getType());

        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return getType().toString();
    }
}
