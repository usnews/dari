package com.psddev.dari.util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class ObjectToAnyCollection implements ConversionFunction<Object, Collection<Object>> {

    @SuppressWarnings("rawtypes")
    private final Map<Type, Class<? extends Collection>> implementationClasses = new HashMap<Type, Class<? extends Collection>>();

    public ObjectToAnyCollection() {
        implementationClasses.put(BlockingDeque.class, LinkedBlockingDeque.class);
        implementationClasses.put(BlockingQueue.class, ArrayBlockingQueue.class);
        implementationClasses.put(Collection.class, ArrayList.class);
        implementationClasses.put(Deque.class, ArrayDeque.class);
        implementationClasses.put(List.class, ArrayList.class);
        implementationClasses.put(NavigableSet.class, TreeSet.class);
        implementationClasses.put(Queue.class, LinkedList.class);
        implementationClasses.put(Set.class, HashSet.class);
        implementationClasses.put(SortedSet.class, TreeSet.class);
    }

    /**
     * Returns the implementation class used to create an instance of
     * the given {@code returnType}.
     */
    @SuppressWarnings("rawtypes")
    public Class<? extends Collection> getImplementationClass(Type returnType) {
        Class<? extends Collection> implementationClass = implementationClasses.get(returnType);
        if (implementationClass == null) {
            @SuppressWarnings("unchecked")
            Class<? extends Collection> returnClass = (Class<? extends Collection>) TypeDefinition.getInstance(returnType).getObjectClass();
            implementationClass = implementationClasses.get(returnClass);
            if (implementationClass == null) {
                implementationClass = returnClass;
            }
        }
        return implementationClass;
    }

    /**
     * Puts the the given {@code implementationClass} to be used for
     * creating an instance of the given {@code returnType}.
     */
    @SuppressWarnings("rawtypes")
    public void putImplementationClass(Type returnType, Class<? extends Collection> implementationClass) {
        implementationClasses.put(returnType, implementationClass);
    }

    @SuppressWarnings("rawtypes")
    public <T extends Collection> void putImplementationClass(Class<T> returnClass, Class<? extends T> implementationClass) {
        putImplementationClass((Type) returnClass, implementationClass);
    }

    @SuppressWarnings("rawtypes")
    public <T extends Collection> void putImplementationClass(TypeReference<T> returnType, Class<? extends T> implementationClass) {
        putImplementationClass(returnType.getType(), implementationClass);
    }

    // --- ConversionFunction support ---

    @Override
    public Collection<Object> convert(Converter converter, Type returnType, Object object) throws Exception {

        Type itemType;
        if (returnType instanceof Class) {
            itemType = null;

        } else if (returnType instanceof ParameterizedType) {
            itemType = ((ParameterizedType) returnType).getActualTypeArguments()[0];

        } else {
            throw new ConversionException(String.format(
                    "Cannot convert to [%s] type!", returnType), null);
        }

        @SuppressWarnings("unchecked")
        Collection<Object> collection = TypeDefinition.getInstance(getImplementationClass(returnType)).newInstance();
        for (Object item : converter.convert(Iterable.class, object)) {
            collection.add(itemType != null ? converter.convert(itemType, item) : item);
        }
        return collection;
    }
}
