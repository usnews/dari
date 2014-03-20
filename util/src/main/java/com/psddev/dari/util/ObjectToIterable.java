package com.psddev.dari.util;

import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/** Converts an object into an iterable. */
public class ObjectToIterable implements ConversionFunction<Object, Iterable<Object>> {

    /**
     * Converts the given {@code object} into an iterable if it's an array
     * or an instance of any of the following: {@link Enumeration},
     * {@link Iterable}, or {@link Iterator}. This method differs from
     * {@link #convert} in that it'll return a {@code null} instead of
     * returning a collection of one if it doesn't recognize the type.
     */
    @SuppressWarnings("unchecked")
    public static Iterable<Object> iterable(Object object) {

        if (object == null) {
            return null;

        } else if (object instanceof Iterable) {
            return (Iterable<Object>) object;

        } else if (object.getClass().isArray()) {
            return new ArrayIterable(object);

        } else if (object instanceof Enumeration) {
            return new EnumerationIterable((Enumeration<Object>) object);

        } else if (object instanceof Iterator) {
            return new IteratorIterable((Iterator<Object>) object);

        } else {
            return null;
        }
    }

    // --- ConversionFunction support ---

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Iterable<Object> convert(Converter converter, Type returnType, Object object) throws Exception {

        Iterable<Object> iterable;
        if (object == null) {
            iterable = Collections.emptyList();

        } else if (object instanceof Map) {
            iterable = ((Map) object).entrySet();

        } else {
            iterable = iterable(object);
            if (iterable == null) {
                iterable = Collections.singleton(object);
            }
        }

        if (returnType instanceof Class) {
            // No need to further convert the item in the iterable,
            // because generic type argument isn't available.

        } else if (returnType instanceof ParameterizedType) {
            Type itemType = ((ParameterizedType) returnType).getActualTypeArguments()[0];
            List<Object> list = new ArrayList<Object>();
            for (Object item : iterable) {
                list.add(converter.convert(itemType, item));
            }
            iterable = list;

        } else {
            throw new ConversionException(String.format(
                    "Can't convert to [%s]!", returnType), null);
        }

        return iterable;
    }

    // --- Nested ---

    /** Iterable over an array. */
    private static class ArrayIterable implements Iterable<Object>, Iterator<Object> {

        private final Object array;
        private final int length;
        private int index;

        public ArrayIterable(Object array) {
            this.array = array;
            this.length = Array.getLength(array);
        }

        // --- Iterable support ---

        @Override
        public Iterator<Object> iterator() {
            return this;
        }

        // --- Iterator support ---

        @Override
        public boolean hasNext() {
            return index < length;
        }

        @Override
        public Object next() {
            if (hasNext()) {
                Object item = Array.get(array, index);
                ++ index;
                return item;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /** Iterable over an enumeration. */
    private static class EnumerationIterable implements Iterable<Object>, Iterator<Object> {

        private final Enumeration<Object> enumeration;

        public EnumerationIterable(Enumeration<Object> enumeration) {
            this.enumeration = enumeration;
        }

        // --- Iterable support ---

        @Override
        public Iterator<Object> iterator() {
            return this;
        }

        // --- Iterator support ---

        @Override
        public boolean hasNext() {
            return enumeration.hasMoreElements();
        }

        @Override
        public Object next() {
            return enumeration.nextElement();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /** Iterable over an iterator. */
    private static class IteratorIterable implements Iterable<Object> {

        private final Iterator<Object> iterator;

        public IteratorIterable(Iterator<Object> iterator) {
            this.iterator = iterator;
        }

        // --- Iterable support ---

        @Override
        public Iterator<Object> iterator() {
            return iterator;
        }
    }
}
