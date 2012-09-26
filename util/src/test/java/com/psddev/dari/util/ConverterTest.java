package com.psddev.dari.util;

import java.lang.reflect.Type;
import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.rules.TestName;

public class ConverterTest {
	@Rule public TestName name = new TestName();
	Converter converter;

    @Before
    public void before() {
    	converter = new Converter();
        converter.putAllStandardFunctions();
    	//System.out.println("Starting test: " + name.getMethodName());
    }

    @After
    public void after() {
    	//System.out.println("Ending test: " + name.getMethodName() + "\n");
    }

    /*
     * Test classes
     */
    static class Parent {
    	public String pfield = null;
    	public Parent(String value) { pfield = "p" + value; }
    	@Override public String toString() { return pfield; }
    	@Override public boolean equals(Object o) {
    		if(! (o instanceof Parent)) { return false; }
    		return this.pfield.equals(((Parent)o).pfield);
    	}
    }
    static class Child extends Parent {
    	public String cfield = null;
    	public Child(String value) { super(value); cfield = "c" + value; }
    	@Override public String toString() { return cfield; }
    }
    static class GrandChild extends Child {
    	public String gfield = null;
    	public GrandChild(String value) { super(value); gfield = "g" + value; }
    	@Override public String toString() { return gfield; }
    }
    /*
     * Test interfaces
     */
    static interface IParent {

    }
    static interface IChild extends IParent {

    }
    static interface IGrandChild extends IChild {

    }

    /**
     * public Object convert(Type returnType, Object object)
     */
    @Test // demonstrating one of the builtin converters for a primitive type
    public void convert_builtin_primitive() throws Exception {
    	Object result = converter.convert(int.class, "15");
    	assertEquals(Integer.class, result.getClass());
    	assertEquals(15, result);
    }

    /*
     * Nulls - primitives
     */
    @Test // Converting null to primitive will throw an exception even though there is a converter from null defined
    public void aconvert_null_primitive_error() throws Exception {
    	converter.setThrowError(true);
    	try {
    		converter.convert(int.class, (Object)null);
    	} catch(ConversionException ex) {
    		assertEquals(Integer.class, ex.getDefaultValue().getClass());
    		assertEquals(new Integer(0), ex.getDefaultValue());
    		return;
    	} catch(Exception e) {
    		throw e;
    	}
    	org.junit.Assert.fail("Exception not thrown");
    }
    @Test // Turning off errors means the default value will just be returned for primitives
    public void aconvert_null_primitive_noerror() throws Exception {
    	converter.setThrowError(false);
    	assertEquals(new Integer(0), converter.convert(int.class, (Object)null));
    }

    @Test // Without a null conversion defined, the default Object conversion is used, returning null
    public void convert_null_undefined() throws Exception {
    	converter.setThrowError(true);
    	assertEquals(null, converter.convert(Parent.class, (Object)null));
    }


    @Test // Converting a collection will convert each of the items inside it
    public void convert_builtin_list_members() throws Exception {
    	List<String> inputs = Arrays.asList("1","2","3");
    	List<Integer> expect = Arrays.asList(1,2,3);

    	TypeReference<List<Integer>> typeref = new TypeReference<List<Integer>>() {};
    	List<Integer> output = (List<Integer>)converter.convert(typeref.getType(), inputs);
    	assertEquals(expect, output);
    }

    /*
     * Exceptions
     */
    @Test // Default behavior is to not throw an exception
    public void convert_error_noexception()  {
    	assertEquals(null, converter.convert(Integer.class, "a"));
    }

    @Test (expected=ConversionException.class)
    public void convert_error_exception()  {
    	converter.setThrowError(true);
    	assertEquals(null, converter.convert(Integer.class, "a"));
    }

    /*
     * Direct Conversions
     */
    @Test
    public void convert_direct() throws Exception {
    	converter.putDirectFunction(String.class, Parent.class, new ConversionFunction<String,Parent>() {
    		@Override
    		public Parent convert(Converter converter, Type returnType, String object) throws Exception {
    			return new Parent(object);
    		}
    	});
    	Parent output = converter.convert(Parent.class, "value");
    	assertEquals("pvalue", output.pfield);
    }

    /*
     * inherited conversions
     */
    @Test // GrandChild can be cast to Child, so this conversion (inheritable) will be used
    public void convert_inheritable() throws Exception {
    	converter.putDirectFunction(Child.class, Integer.class, new ConversionFunction<Child,Integer>() {
    		@Override
    		public Integer convert(Converter converter, Type returnType, Child object) throws Exception {
    			return 1000;
    		}
    	});
    	Object output = converter.convert(Integer.class, new GrandChild("value"));
    	assertEquals(1000, output);
    }

    @Test // The Child->Integer conversion should be used since distance(GrandChild,Child) < distance(GrandChild,Parent)
    public void convert_inheritable_distance() throws Exception {
    	converter.putDirectFunction(Child.class, Integer.class, new ConversionFunction<Child,Integer>() {
    		@Override
    		public Integer convert(Converter converter, Type returnType, Child object) throws Exception {
    			return 1000;
    		}
    	});
    	converter.putDirectFunction(Parent.class, Integer.class, new ConversionFunction<Parent,Integer>() {
    		@Override
    		public Integer convert(Converter converter, Type returnType, Parent object) throws Exception {
    			return 2000;
    		}
    	});
    	Object output = converter.convert(Integer.class, new GrandChild("value"));
    	assertEquals(1000, output);
    }

    @Test // make sure the closest conversion that is assignable to {@code returnClass} is returned, not just the closest
    public void convert_unassignable() {
    	converter.putDirectFunction(Child.class, Date.class, new ConversionFunction<Child,Date>() {
    		@Override
    		public Date convert(Converter converter, Type returnType, Child object) throws Exception {
    			return new Date(0);
    		}
    	});
    	converter.putDirectFunction(Parent.class, Integer.class, new ConversionFunction<Parent,Integer>() {
    		@Override
    		public Integer convert(Converter converter, Type returnType, Parent object) throws Exception {
    			return 2000;
    		}
    	});
    	Object output = converter.convert(Integer.class, new GrandChild("value"));
    	assertEquals(2000, output);
    }

    /*
     * inherited conversions - interfaces
     */
    @Ignore // TODO: Figure out if this functionality is broken, or if interfaces just aren't supported (and document)
    @Test // IGrandChild can be cast to IChild, so this conversion (inheritable) will be used
    public void convert_interface_direct() throws Exception {
    	converter.putDirectFunction(IChild.class, Integer.class, new ConversionFunction<IChild,Integer>() {
    		@Override
    		public Integer convert(Converter converter, Type returnType, IChild object) throws Exception {
    			return 1000;
    		}
    	});
    	Object output = converter.convert(Integer.class, new IChild() {});
    	assertEquals(1000, output);
    }

    @Ignore // TODO: Figure out if this functionality is broken, or if interfaces just aren't supported (and document)
    @Test // IGrandChild can be cast to IChild, so this conversion (inheritable) will be used
    public void convert_interface_inheritable() throws Exception {
    	converter.putInheritableFunction(IChild.class, Integer.class, new ConversionFunction<IChild,Integer>() {
    		@Override
    		public Integer convert(Converter converter, Type returnType, IChild object) throws Exception {
    			return 1000;
    		}
    	});
    	Object output = converter.convert(Integer.class, new IGrandChild() {});
    	assertEquals(1000, output);
    }

    @Ignore // TODO: Figure out if this functionality is broken, or if interfaces just aren't supported (and document)
    @Test // The IChild->Integer conversion should be used since distance(IGrandChild,IChild) < distance(IGrandChild,IParent)
    public void convert_interface_inheritable_distance() throws Exception {
    	converter.putInheritableFunction(IChild.class, Integer.class, new ConversionFunction<IChild,Integer>() {
    		@Override
    		public Integer convert(Converter converter, Type returnType, IChild object) throws Exception {
    			return 1000;
    		}
    	});
    	converter.putInheritableFunction(IParent.class, Integer.class, new ConversionFunction<IParent,Integer>() {
    		@Override
    		public Integer convert(Converter converter, Type returnType, IParent object) throws Exception {
    			return 2000;
    		}
    	});
    	Object output = converter.convert(Integer.class, new IGrandChild() {});
    	assertEquals(1000, output);
    }

    /**
     * public <F> ConversionFunction<F, Object> getFunction(Class<F> fromClass, Type toType)
     *
     * NOTE: Not fully tests and tests need to be rewritten to check the actual function returned
     * See {@code convert} tests for testing the actual conversion
     */

    @Test // demonstrating one of the builtin converters for a primitive type
    public void getFunction_builtin_primitive() throws Exception {
    	ConversionFunction<Object, Object> function = converter.getFunction(Object.class, int.class);
    	Object result = function.convert(null, null, "15");
    	assertEquals(Integer.class, result.getClass());
    	assertEquals(15, result);
    }

    // passing null into a function that is retrieved as expecting an object throws an exception
    @Test  (expected=NullPointerException.class)
    public void getFunction_builtin_null() throws Exception {
    	ConversionFunction<Object, Object> function = converter.getFunction(Object.class, int.class);
    	function.convert(null, null, (Object)null);
    }

    @Test // Converting a collection will convert each of the items inside it
    public void getFunction_builtin_list_members() throws Exception {
    	List<String> inputs = Arrays.asList("1","2","3");
    	List<Integer> expect = Arrays.asList(1,2,3);

    	TypeReference<List<Integer>> typeref = new TypeReference<List<Integer>>() {};
    	ConversionFunction<Object, Object> function = converter.getFunction(Object.class, typeref.getType());

    	Object result = function.convert(converter, typeref.getType(), inputs);
    	List<Integer> output = (List<Integer>)result;
    	assertEquals(expect, output);
    }

}


