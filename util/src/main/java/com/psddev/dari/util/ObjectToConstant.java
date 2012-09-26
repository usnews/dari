package com.psddev.dari.util;

import java.lang.reflect.Type;

/** Converts an object into a constant. */
public class ObjectToConstant<T> implements ConversionFunction<Object, T> {

    private static final ObjectToConstant<Object>
            NULL_INSTANCE = new ObjectToConstant<Object>(null);

    private static final PullThroughCache<Object, ObjectToConstant<?>>
            INSTANCES = new PullThroughCache<Object, ObjectToConstant<?>>() {

        @Override
        protected ObjectToConstant<?> produce(Object constant) {
            return new ObjectToConstant<Object>(constant);
        }
    };

    private final T constant;

    /**
     * Returns an instance that will convert any object into the given
     * {@code constant}.
     */
    public static <T> ObjectToConstant<T> getInstance(T constant) {
        return (ObjectToConstant<T>) (constant == null ? NULL_INSTANCE : INSTANCES.get(constant));
    }

    protected ObjectToConstant(T constant) {
        this.constant = constant;
    }

    @Override
    public T convert(Converter converter, Type returnType, Object object) {
        return constant;
    }
}
