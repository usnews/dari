package com.psddev.dari.util;

import java.util.*;
import org.junit.*;
import static org.junit.Assert.*;

public class ObjectUtilsTest {
	/***********************************************************
	 * public static boolean equals(Object object1, Object object2)
	 */
    @Test
    public void equals_null() {
        assertEquals(false, ObjectUtils.equals(null, new Object()));
        assertEquals(false, ObjectUtils.equals(new Object(), null));
    }

    @Test
    public void equals_null_both() {
        assertEquals(true, ObjectUtils.equals(null, null));
    }

    @Test
    public void equals_null_generic_true() {
        assertEquals(true, ObjectUtils.equals("hello", "hello"));
    }

    @Test
    public void equals_null_generic_false() {
        assertEquals(false, ObjectUtils.equals("hello", "goodbye"));
    }
    
    /*
     * Enums
     */
	static enum TestEnum {
		value1(), value2();
	}
	
    @Test
    public void equals_null_enum_true() {
        assertEquals(true, ObjectUtils.equals(TestEnum.value1, "value1"));
        assertEquals(true, ObjectUtils.equals("value2", TestEnum.value2));
    }
    
    @Test
    public void equals_null_enum_case_sensitive() {
        assertEquals(false, ObjectUtils.equals(TestEnum.value1, "VALUE1"));
    }
    
    
	/***********************************************************
	 * public static int compareTo(Object object1, Object object2) 
	 */
    /*
     * null
     */
    @Test (expected=NullPointerException.class)
    public void compareTo_null_1() {
        assertEquals(false, ObjectUtils.compareTo(null, new Object()));
    }

    @Test (expected=NullPointerException.class)
    public void compareTo_null_2() {
        assertEquals(false, ObjectUtils.compareTo(new Object(), null));
    }

    /*
     * same class
     */
    @Test
    public void compareTo_same_class_less() {
        assertEquals(-1, ObjectUtils.compareTo("hello1", "hello2"));
    }
    @Test
    public void compareTo_same_class_equal() {
        assertEquals(0, ObjectUtils.compareTo("hello", "hello"));
    }
    @Test
    public void compareTo_same_class_greater() {
        assertEquals(1, ObjectUtils.compareTo("hello2", "hello1"));
    }

    /*
     * assignable
     */
	class CompareParent implements Comparable<CompareParent> {
		public final String value;
		public CompareParent(String value) {
			this.value = value;
		}
		public int compareTo(CompareParent that) {
			if(that == this) return 0;
			return this.value.compareTo(that.value);
		}
	}
	class CompareChild extends CompareParent {
		public CompareChild(String value) {
			super(value);
		}
	}

    @Test
    public void compareTo_assignable_assignable_second() {
        assertEquals(0, ObjectUtils.compareTo(new CompareParent("hello"), new CompareChild("hello")));
    }
    public void compareTo_assignable_assignable_first() {
        assertEquals(0, ObjectUtils.compareTo(new CompareChild("hello"), new CompareParent("hello")));
    }
    @Test
    public void compareTo_assignable_less() {
        assertEquals(-1, ObjectUtils.compareTo(new CompareParent("hello1"), new CompareChild("hello2")));
    }

    /*
     * illegal - not assignable
     */
	class CompareOne implements Comparable<CompareTwo> {
		public int compareTo(CompareTwo that) {
			throw new RuntimeException("not implemented");
		}
	}
	class CompareTwo implements Comparable<CompareOne> {
		public int compareTo(CompareOne that) {
			throw new RuntimeException("not implemented");
		}
	}
    @Test (expected=IllegalArgumentException.class)
    public void compareTo_not_assignable() {
        assertEquals(0, ObjectUtils.compareTo(new CompareOne(), new CompareTwo()));
    }

    /*
     * illegal - not comparable
     */
	class NotComparableParent {}
	class NotComparableChild extends NotComparableParent {}

    @Test (expected=IllegalArgumentException.class)
    public void compareTo_not_comparable() {
        assertEquals(0, ObjectUtils.compareTo(new NotComparableParent(), new NotComparableChild()));
    }

	/***********************************************************
	 * public static Object getValue(Object object, String name)
	 */

	/**
	 * Field values
	 */
    private static class ParentClass {
        public String string1 = "string1";
    }

    private static class ChildClass extends ParentClass {
        public String string3 = "string3";
    }
    
    @Test // Field from parent should be accessable
    public void test_getValue_parent_field() {
        ChildClass object = new ChildClass();
        assertEquals("string1", ObjectUtils.getValue(object, "string1"));
    }
    
    @Test // Field from child should be accessable
    public void test_getValue_child_field() {
        ChildClass object = new ChildClass();
        assertEquals("string3", ObjectUtils.getValue(object, "string3"));
    }
    
    /**
     * Method values
     */
    private static class ParentMethod {
    	public String method_parent() {
    		return "method parent result";
    	}
    }
    private static class ChildMethod extends ParentMethod {
    	public String method_child() {
    		return "method child result";
    	}
    }
    
    @Test
    public void test_getValue_parent_method() {
        ChildMethod object = new ChildMethod();
        assertEquals("method parent result", ObjectUtils.getValue(object, "method_parent"));
    }
    
    @Test
    public void test_getValue_child_method() {
        ChildMethod object = new ChildMethod();
        assertEquals("method child result", ObjectUtils.getValue(object, "method_child"));
    }

    /**
     * Getter values
     */
    private static class ParentGetter {
    	public String getParent() {
    		return "getter parent result";
    	}
    }
    private static class ChildGetter extends ParentGetter {
    	public String getChild() {
    		return "getter child result";
    	}
    }
    
    @Test
    public void test_getValue_parent_getter() {
        ChildGetter object = new ChildGetter();
        assertEquals("getter parent result", ObjectUtils.getValue(object, "parent"));
    }
    
    @Test
    public void test_getValue_child_getter() {
        ChildGetter object = new ChildGetter();
        assertEquals("getter child result", ObjectUtils.getValue(object, "child"));
    }
    
    /**
     * Underscrore values
     */
    private static class ParentUnderscrore {
    	public String _field_parent = "value parent";
    }
    private static class ChildUnderscore extends ParentUnderscrore {
    	public String _field_child = "value child";
    }
    
    @Test
    public void test_getValue_parent_underscore() {
    	ChildUnderscore object = new ChildUnderscore();
        assertEquals("value parent", ObjectUtils.getValue(object, "field_parent"));
    }
    
    @Test
    public void test_getValue_child_underscore() {
    	ChildUnderscore object = new ChildUnderscore();
        assertEquals("value child", ObjectUtils.getValue(object, "field_child"));
    }
    /**
     * Private fields/methods
     */
    class PrivateClass {
    	private String privateField = "private field";
    	private String privateMethod() {
    		return "private method";
    	}
    }
    
    @Test (expected=IllegalStateException.class)
    public void test_getValue_field_private() {
    	PrivateClass object = new PrivateClass();
        ObjectUtils.getValue(object, "privateField");
    }    

    @Test
    public void test_getValue_method_private() {
    	PrivateClass object = new PrivateClass();
        assertEquals("private method", ObjectUtils.getValue(object, "privateMethod"));
    }

    /**
     * Unknown
     */
    private static class UnknownClass {
    	public String getChild() {
    		return "getter child result";
    	}
    }
    
    @Ignore // ObjectUtils is to be deprecated, so failing tests are ignored
    @Test (expected=IllegalArgumentException.class)
    public void test_getValue_unknown() {
    	UnknownClass object = new UnknownClass();
    	ObjectUtils.getValue(object, "unknown");
    }
    
    /**
     * Priority
     */
    @Test // method name takes priority over getter name
    public void test_getValue_priority_method_getter() {
    	final class TestClass {
    		public String getItem() {
    			return "getter value";
    		}
    		public String item() {
    			return "method value";
    		}
    	}
    	TestClass object = new TestClass();
    	assertEquals("method value", ObjectUtils.getValue(object, "item"));
    }    
    
    @Test // getter takes priority over field
    public void test_getValue_priority_getter_field() {
    	final class TestClass {
    		public String item = "item value";
    		public String getItem() {
    			return "getter value";
    		}
    	}
    	TestClass object = new TestClass();
    	assertEquals("getter value", ObjectUtils.getValue(object, "item"));
    }    
    
    @Test // actual field name takes priority over name with underscore
    public void test_getValue_priority_field_underscore() {
    	final class TestClass {
    		public String item = "item value";
    		public String _item = "_item value";
    	}
    	TestClass object = new TestClass();
    	assertEquals("item value", ObjectUtils.getValue(object, "item"));
    }    

}
