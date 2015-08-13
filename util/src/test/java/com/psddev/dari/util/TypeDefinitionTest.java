package com.psddev.dari.util;

import java.lang.reflect.*;
import java.util.*;

import org.junit.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.equalTo;;

public class TypeDefinitionTest {

	
	/**
	 * public static TypeDefinition<?> getInstance(Type type)
	 */
	
    @Test
    public void test_getInstance() {
    	TypeDefinition<Bar> typedef = TypeDefinition.getInstance(Bar.class);
    	assertFalse(null == typedef);
    }
	
    /*
     *  Currently fails... should throw a NullPointerException (or some other "nuh uh" type)
     *  since other calls on this object wind up throwing NullPointerExceptions
     */
    @Ignore // TODO: This should be fixed
    @Test (expected=NullPointerException.class)
    public void test_getInstance_null() {
    	TypeDefinition<Bar> typedef = TypeDefinition.getInstance((Class<Bar>)null);
    }

    
    /**
     * public Type getType()
     */
    @Test
    public void test_getType() {
    	TypeDefinition<String> typedef = TypeDefinition.getInstance(String.class);
    	assertEquals("class java.lang.String", typedef.getType().toString());
    }
    
    /*
     * The current behavior is that getObjectClass() winds up throwing the exception, rather than getInstance()
     */
    @Test (expected=NullPointerException.class)
    public void test_getType_null() {
    	TypeDefinition<Bar> typedef = TypeDefinition.getInstance((Class<Bar>)null);
    	typedef.getType();
    }
	
    @Test // When coming from a class instance, it doesn't get to know the parameterized type
    public void test_getType_collection_from_object() {
    	List<Bar> tmp = new ArrayList<Bar>();
    	TypeDefinition typedef = TypeDefinition.getInstance(tmp.getClass());
    	assertEquals("class java.util.ArrayList", typedef.getType().toString());
    }

    @Test // Coming from a TypeReference, it gets to know the parameterized type
    public void test_getType_collection_from_typeref() {
    	TypeDefinition<List<String>> typedef = TypeDefinition.getInstance(new TypeReference<List<String>>() { });
    	assertEquals("java.util.List<java.lang.String>", typedef.getType().toString());
    }

    @Test
    public void test_getType_array() {
    	assertEquals(String[].class, TypeDefinition.getInstance(String[].class).getType());
    }

    @Test
    public void test_getType_primitive() {
    	assertEquals(int.class, TypeDefinition.getInstance(int.class).getType());
    }

    /**
     * getType vs getObjectClass 
     * TODO: Remove these tests once we find a way in which the two calls are different
     */
    @Test // For parameterized types, they're not the same
    public void test_getTypeVsGetObjectClass_generics() {
    	TypeDefinition<List<String>> typedef = TypeDefinition.getInstance(new TypeReference<List<String>>() { });
    	assertEquals("interface java.util.List", typedef.getObjectClass().toString());
    	assertEquals("java.util.List<java.lang.String>", typedef.getType().toString());
    	assertThat(typedef.getObjectClass().toString(), not(equalTo(typedef.getType().toString())));
    }
    
    @Test // For arrays, they're the same
    public void test_getTypeVsGetObjectClass_array() {
    	assertEquals(TypeDefinition.getInstance(String[].class).getObjectClass(), TypeDefinition.getInstance(String[].class).getType());

    }

    @Test // for primitives, they're the same
    public void test_getTypeVsGetObjectClass_primitive() {
    	assertEquals(TypeDefinition.getInstance(int.class).getObjectClass(), TypeDefinition.getInstance(int.class).getType());
    }
    
    @Test // for primitives, they're the same
    public void test_getTypeVsGetObjectClass_object() {
    	assertEquals(TypeDefinition.getInstance(String.class).getObjectClass(), TypeDefinition.getInstance(String.class).getType());
    }
    /**
     * public Class<T> getObjectClass()
     */
    
    @Test
    public void test_getObjectClass() {
    	TypeDefinition<Bar> typedef = TypeDefinition.getInstance(Bar.class);
    	assertEquals(Bar.class, typedef.getObjectClass());
    }
    
    /*
     * The current behavior is that getObjectClass() winds up throwing the exception, rather than getInstance()
     */
    @Test (expected=NullPointerException.class)
    public void test_getObjectClass_null() {
    	TypeDefinition<Bar> typedef = TypeDefinition.getInstance((Class<Bar>)null);
    	typedef.getObjectClass();
    }
	
    @Test
    public void test_getObjectClass_collection() {
    	List<Bar> tmp = new ArrayList<Bar>();
    	TypeDefinition typedef = TypeDefinition.getInstance(tmp.getClass());
    	assertEquals(ArrayList.class, typedef.getObjectClass());
    }

    @Test
    public void test_getObjectClass_array() {
    	assertEquals(String[].class, TypeDefinition.getInstance(String[].class).getObjectClass());
    }

    @Test
    public void test_getObjectClass_primitive() {
    	assertEquals(int.class, TypeDefinition.getInstance(int.class).getObjectClass());
    }

	/**
	 * public List<Class<? super T>> getAssignableClasses()
	 */
	
    @Test
    public void test_getAssignableClasses_subclass() {
        assertEquals(
                Arrays.asList(Qux.class, Bar.class, Foo.class, Object.class),
                TypeDefinition.getInstance(Qux.class).getAssignableClasses());
    }
    
    @Test
    public void test_getAssignableClasses_Object() {
        assertEquals(
                Arrays.asList(Object.class),
                TypeDefinition.getInstance(Object.class).getAssignableClasses());
    }

    /**
     * public List<Field> getAllFields()
     */
    
    @Test
    public void test_getAllFields() throws NoSuchFieldException {
        assertEquals(
                Arrays.asList(
                Qux.class.getDeclaredField("quxField"),
                Bar.class.getDeclaredField("barField"),
                Bar.class.getDeclaredField("sharedField"),
                Foo.class.getDeclaredField("staticField"),
                Foo.class.getDeclaredField("_privateField"),
                Foo.class.getDeclaredField("fProtectedField"),
                Foo.class.getDeclaredField("mPublicField"),
                Foo.class.getDeclaredField("transientField_"),
                Foo.class.getDeclaredField("sharedField")),
                TypeDefinition.getInstance(Qux.class).getAllFields());
    }

    @Test
    public void test_getAllSerializableFields() throws NoSuchFieldException {

        Map<String, List<Field>> expected = new LinkedHashMap<String, List<Field>>();
        expected.put("privateField", Arrays.asList(Foo.class.getDeclaredField("_privateField")));
        expected.put("protectedField", Arrays.asList(Foo.class.getDeclaredField("fProtectedField")));
        expected.put("publicField", Arrays.asList(Foo.class.getDeclaredField("mPublicField")));
        expected.put("sharedField", Arrays.asList(Foo.class.getDeclaredField("sharedField"), Bar.class.getDeclaredField("sharedField")));
        expected.put("barField", Arrays.asList(Bar.class.getDeclaredField("barField")));
        expected.put("quxField", Arrays.asList(Qux.class.getDeclaredField("quxField")));

        assertEquals(
                expected,
                TypeDefinition.getInstance(Qux.class).getAllSerializableFields());
    }

    
    /**
     * public Field getField(String name)
     */
    
    @Test
    public void test_getField() throws NoSuchFieldException {

        assertEquals(
                Qux.class.getDeclaredField("quxField"),
                TypeDefinition.getInstance(Qux.class).getField("quxField"));

        assertEquals(
                Foo.class.getDeclaredField("staticField"),
                TypeDefinition.getInstance(Qux.class).getField("staticField"));

        assertEquals(
                Bar.class.getDeclaredField("sharedField"),
                TypeDefinition.getInstance(Qux.class).getField("sharedField"));
    }

    
    /**
     * public List<Constructor<T>> getConstructors()
     */
    
    @Test
    public void test_getConstructors() {
        assertEquals(
                Arrays.asList(Qux.class.getDeclaredConstructors()),
                TypeDefinition.getInstance(Qux.class).getConstructors());
    }

    
    /**
     * public Constructor<T> getConstructor(Class<?>... parameterClasses)
     */
    
    @Test
    public void test_getConstructor() throws NoSuchMethodException {
        assertEquals(
                Qux.class.getDeclaredConstructor(Map.class),
                TypeDefinition.getInstance(Qux.class).getConstructor(HashMap.class));
    }
    
    @Test (expected=NoSuchMethodException.class)
    public void test_getConstructor_invalid() throws NoSuchMethodException {
        assertEquals(
                Qux.class.getDeclaredConstructor(List.class),
                TypeDefinition.getInstance(Qux.class).getConstructor(HashMap.class));
    }

    
    /**
     * public List<Method> getAllMethods()
     */
    
    @Test
    public void test_getAllMethods() throws NoSuchMethodException {

        List<Method> expected = new ArrayList<Method>();
        expected.add(Qux.class.getDeclaredMethod("getQux"));
        expected.add(Qux.class.getDeclaredMethod("hasQux"));
        expected.add(Qux.class.getDeclaredMethod("setQux", Object.class));
        expected.add(Bar.class.getDeclaredMethod("getBar"));
        expected.add(Foo.class.getDeclaredMethod("setFoo", Object.class));
        Collections.addAll(expected, Object.class.getDeclaredMethods());
        expected.add(Baz.class.getDeclaredMethod("getBaz"));

        assertEquals(
                expected,
                TypeDefinition.getInstance(Qux.class).getAllMethods());
    }

    // TODO: Add a test that actually throws a NoSuchMethodException, if it can actually be thrown
    
    /**
     * public Map<String, Method> getAllGetters() 
     */
    
    @Test
    public void test_getAllGetters() throws NoSuchMethodException {

        Map<String, Method> expected = new LinkedHashMap<String, Method>();
        expected.put("qux", Qux.class.getDeclaredMethod("getQux"));
        expected.put("hasQux", Qux.class.getDeclaredMethod("hasQux"));
        expected.put("bar", Bar.class.getDeclaredMethod("getBar"));
        expected.put("baz", Baz.class.getDeclaredMethod("getBaz"));

        assertEquals(
                expected,
                TypeDefinition.getInstance(Qux.class).getAllGetters());
    }

    @Test
    public void test_getAllGetters_none() throws NoSuchMethodException {
        assertEquals(Collections.<String,Method>emptyMap(), TypeDefinition.getInstance(Object.class).getAllGetters());
    }

    
    /**
     * public Map<String, Method> getAllSetters()
     */
    
    @Test
    public void test_getAllSetters() throws NoSuchMethodException {
        Map<String, Method> expected = new LinkedHashMap<String, Method>() {{
        	put("qux", Qux.class.getDeclaredMethod("setQux", Object.class));
        	put("foo", Foo.class.getDeclaredMethod("setFoo", Object.class));
        }};

        assertEquals(expected, TypeDefinition.getInstance(Qux.class).getAllSetters());
    }

    @Test
    public void test_getAllSetters_none() throws NoSuchMethodException {
        assertEquals(Collections.<String,Method>emptyMap(), TypeDefinition.getInstance(Object.class).getAllSetters());
    }
    
    /**
     * utility code for testing
     */
    private static interface Baz {

        default Object getBaz() {
            return null;
        }
    }

    private static class Foo implements Baz {

        private static Object staticField;
        private Object _privateField;
        protected Object fProtectedField;
        public Object mPublicField;
        private transient Object transientField_;
        private Object sharedField;

        public void setFoo(Object foo) {
        }
    }

    private static class Bar extends Foo {

        private Object barField;
        private Object sharedField;

        public Object getBar() {
            return null;
        }
    }

    private static class Qux extends Bar {

        private Object quxField;

        public Qux() {
        }

        public Qux(Map parameter) {
        }

        public Object getQux() {
            return null;
        }

        public boolean hasQux() {
            return false;
        }

        public void setQux(Object qux) {
        }
    }
}
