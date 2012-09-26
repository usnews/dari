package com.psddev.dari.util;

import java.lang.reflect.Field;
import java.util.*;
import org.apache.commons.collections.CollectionUtils;

import org.junit.*;
import static org.junit.Assert.*;

public class ObjectMapTest {


    @Before
    public void before() {}

    @After
    public void after() {}

    /**
     * public boolean containsKey(Object key)
     */
    static class ObjContainsKey {
		public String field_pub = "pub field value";
		public String field_priv = "priv field value";
    }
    
    @Test 
    public void containsKey_objfields_public() {
    	ObjectMap objmap = new ObjectMap(new ObjContainsKey());
    	assertTrue(objmap.containsKey("field_pub"));
    }
    
    @Test 
    public void containsKey_objfields_private() {
    	ObjectMap objmap = new ObjectMap(new ObjContainsKey());
    	assertTrue(objmap.containsKey("field_priv"));
    }
    
    @Test 
    public void containsKey_objfields_unknown() {
    	ObjectMap objmap = new ObjectMap(new ObjContainsKey());
    	assertFalse(objmap.containsKey("field2"));
    }
    
    @Test 
    public void containsKey_objfields_nonstring() {
    	ObjectMap objmap = new ObjectMap(new ObjContainsKey());
    	assertFalse(objmap.containsKey(new Integer(1)));
    }

    @Test 
    public void containsKey_objfields_inherit() {
    	ObjectMap objmap = new ObjectMap(new ObjContainsKey() {
    		String field_child = "child field value";
    	});
    	assertTrue(objmap.containsKey("field_pub"));
    	assertTrue(objmap.containsKey("field_child"));
    }

    @Test 
    public void containsKey_objfields_extras() {
    	ObjectMap objmap = new ObjectMap(new ObjContainsKey());
    	assertFalse(objmap.containsKey("extrafield"));
    	objmap.put("extrafield", "extravalue");
    	assertTrue(objmap.containsKey("extrafield"));
    }


    /**
     * public boolean containsValue(Object value)
     */

    static class ObjContainsValue {
		public String field_pub = "pub field value";
		public String field_priv = "priv field value";
		public int field_int = 1000;
		Integer field_Integer = new Integer(2000);
    }

    @Test 
    public void containsValue_objfields_public() {
    	ObjectMap objmap = new ObjectMap(new ObjContainsValue());
    	assertTrue(objmap.containsValue("pub field value"));
    }
    
    @Test 
    public void containsValue_objfields_private() {
    	ObjectMap objmap = new ObjectMap(new ObjContainsValue());
    	assertTrue(objmap.containsValue("priv field value"));
    }
    
    @Test 
    public void containsValue_objfields_unknown() {
    	ObjectMap objmap = new ObjectMap(new ObjContainsValue());
    	assertFalse(objmap.containsValue("unknown field value"));
    }
    
    @Test 
    public void containsValue_objfields_primitive() {
    	ObjectMap objmap = new ObjectMap(new ObjContainsValue());
    	assertTrue(objmap.containsValue(1000));
    }
    
    @Test 
    public void containsValue_objfields_objectns() {
    	ObjectMap objmap = new ObjectMap(new ObjContainsValue());
    	assertTrue(objmap.containsValue(2000));
    }

    @Test 
    public void containsValue_objfields_inherit() {
    	ObjectMap objmap = new ObjectMap(new ObjContainsValue() {
    		String field_child = "child field value";
    	});
    	assertTrue(objmap.containsValue("pub field value"));
    	assertTrue(objmap.containsValue("child field value"));
    }

    @Test 
    public void containsValue_objfields_extras() {
    	ObjectMap objmap = new ObjectMap(new ObjContainsValue());
    	assertFalse(objmap.containsValue("extravalue"));
    	objmap.put("extrafield", "extravalue");
    	assertTrue(objmap.containsValue("extravalue"));
    }

    
    /**
     * public Set<Map.Entry<String, Object>> entrySet()
     */
    static class ObjEntrySet {
		public String field_pub = "pub field value";
		private String field_priv = "priv field value";
		Integer field_int = new Integer(1000);
    }
    
    @Test 
    public void entrySet() {
    	Set<Map.Entry<String,Object>> expect = (new HashMap<String,Object>() {{
    		put("field_pub", "pub field value");
    		put("field_priv", "priv field value");
    		put("field_int", new Integer(1000));
    	}}).entrySet();
    	
    	ObjectMap objmap = new ObjectMap(new ObjEntrySet());
    	assertEquals(expect, objmap.entrySet());
    }

    @Test 
    public void entrySet_extras() {
    	Set<Map.Entry<String,Object>> expect = (new HashMap<String,Object>() {{
    		put("field_pub", "pub field value");
    		put("field_priv", "priv field value");
    		put("field_int", new Integer(1000));
    		put("putfield", "put value");
    	}}).entrySet();
    	
    	ObjectMap objmap = new ObjectMap(new ObjEntrySet());
    	objmap.put("putfield", "put value");
    	assertEquals(expect, objmap.entrySet());
    }

    /**
     * public Object get(Object key)
     */

    static class ObjGet {
		public String field_pub = "pub field value";
		public String field_priv = "priv field value";
		public int field_int = 1000;
		Integer field_Integer = new Integer(2000);
    }

    @Test 
    public void get_public() {
    	ObjectMap objmap = new ObjectMap(new ObjGet());
    	assertEquals("pub field value", objmap.get("field_pub"));
    }

    @Test 
    public void get_private() {
    	ObjectMap objmap = new ObjectMap(new ObjGet());
    	assertEquals("priv field value", objmap.get("field_priv"));
    }

    @Test 
    public void get_primitive() {
    	ObjectMap objmap = new ObjectMap(new ObjGet());
    	assertEquals(1000, objmap.get("field_int"));
    }

    @Test 
    public void get_object() {
    	ObjectMap objmap = new ObjectMap(new ObjGet());
    	assertEquals(2000, objmap.get("field_Integer"));
    }

    @Test 
    public void get_unknown() {
    	ObjectMap objmap = new ObjectMap(new ObjGet());
    	assertEquals(null, objmap.get("field_unknown"));
    }

    @Test 
    public void get_extras() {
    	ObjectMap objmap = new ObjectMap(new ObjGet());
    	objmap.put("field_extra", "field extra value");
    	assertEquals("field extra value", objmap.get("field_extra"));
    }
    @Test 
    public void get_inherit() {
    	ObjectMap objmap = new ObjectMap(new ObjGet() {
    		public String field_child = "field child value";
    	});
    	assertEquals("pub field value", objmap.get("field_pub"));
    	assertEquals("field child value", objmap.get("field_child"));
    }

    /**
     * public boolean isEmpty()
     */
    @Test 
    public void isEmpty() {
    	ObjectMap objmap = new ObjectMap(new Object());
    	assertTrue(objmap.isEmpty());
    }
    
    @Test 
    public void isEmpty_false() {
    	ObjectMap objmap = new ObjectMap(new Object() {
    		String field = "value";
    	});
    	assertFalse(objmap.isEmpty());
    }
    @Test 
    public void isEmpty_extras() {
    	ObjectMap objmap = new ObjectMap(new Object());
    	objmap.put("field", "value");
    	assertFalse(objmap.isEmpty());
    }

    @Test 
    public void isEmpty_child() {
    	class Parent { String field = "value"; }
    	class Child extends Parent {} // child has no fields
    	
    	ObjectMap objmap = new ObjectMap(new Child());
    	assertFalse(objmap.isEmpty());
    }

    
    
    /**
     * public Set<String> keySet() 
     */
    static class ObjKeySet {
		public String field_pub = "pub field value";
		private String field_priv = "priv field value";
		Integer field_int = new Integer(1000);
    }
    
    @Test 
    public void keySet() {
    	Set<String> expect = (new HashMap<String,Object>() {{
    		put("field_pub", "pub field value");
    		put("field_priv", "priv field value");
    		put("field_int", new Integer(1000));
    	}}).keySet();
    	
    	ObjectMap objmap = new ObjectMap(new ObjEntrySet());
    	assertEquals(expect, objmap.keySet());
    }

    @Test 
    public void keySet_extras() {
    	Set<String> expect = (new HashMap<String,Object>() {{
    		put("field_pub", "pub field value");
    		put("field_priv", "priv field value");
    		put("field_int", new Integer(1000));
    		put("putfield", "put value");
    	}}).keySet();
    	
    	ObjectMap objmap = new ObjectMap(new ObjEntrySet());
    	objmap.put("putfield", "put value");
    	assertEquals(expect, objmap.keySet());
    }
    
   
    /**
     * public Collection<Object> values()
     */
    static class ObjValues {
		public String field_pub = "pub field value";
		private String field_priv = "priv field value";
		Integer field_int = new Integer(1000);
    }
    
    @Test 
    public void values() {
    	Collection<Object> expect = (new HashMap<String,Object>() {{
    		put("field_pub", "pub field value");
    		put("field_priv", "priv field value");
    		put("field_int", new Integer(1000));
    	}}).values();

    	ObjectMap objmap = new ObjectMap(new ObjValues());
    	assertTrue(CollectionUtils.isEqualCollection(expect, objmap.values()));
    }

    @Test 
    public void values_extras() {
    	Collection<Object> expect = (new HashMap<String,Object>() {{
    		put("field_pub", "pub field value");
    		put("field_priv", "priv field value");
    		put("field_int", new Integer(1000));
    		put("putfield", "put value");
    	}}).values();
    	
    	ObjectMap objmap = new ObjectMap(new ObjValues());
    	objmap.put("putfield", "put value");
    	assertTrue(CollectionUtils.isEqualCollection(expect, objmap.values()));
    }

    
    /**
     * public int size()
     */
    static class ObjSize {
		public String field_pub = "pub field value";
		private String field_priv = "priv field value";
		Integer field_int = new Integer(1000);
    }
    
    @Test 
    public void size() {
    	ObjectMap objmap = new ObjectMap(new ObjValues());
    	assertEquals(3, objmap.size());
    }

    @Test 
    public void size_extras() {
    	ObjectMap objmap = new ObjectMap(new ObjValues());
    	objmap.put("putfield", "put value");
    	assertEquals(4, objmap.size());
    }

    @Test // A value of null does not change the fact that the field is counted
    public void size_null() {
    	ObjValues obj = new ObjValues();
    	obj.field_pub = null;
    	ObjectMap objmap = new ObjectMap(obj);
    	assertEquals(3, objmap.size());
    }
    
    
    /**
     * public Object remove(Object key)
     */
    static class ObjRemove {
		public String field_pub = "pub field value";
		private String field_priv = "priv field value";
		int field_int = 1000;
    }
    
    @Test 
    public void remove_public() {
    	ObjectMap objmap = new ObjectMap(new ObjRemove());
    	objmap.remove("field_pub");
    	assertEquals(null, objmap.get("field_pub"));
    }

    @Test 
    public void remove_private() {
    	ObjectMap objmap = new ObjectMap(new ObjRemove());
    	objmap.remove("field_priv");
    	assertEquals(null, objmap.get("field_priv"));
    }

    @Test // Removing a primitive sets it to that primitive's default value
    public void remove_primitive() {
    	ObjectMap objmap = new ObjectMap(new ObjRemove());
    	objmap.remove("field_int");
    	assertEquals(0, objmap.get("field_int"));
    }
    
    @Test // Removing a field that is part of the object does not modify the size
    public void remove_size_field() {
    	ObjectMap objmap = new ObjectMap(new ObjRemove());
    	objmap.remove("field_pub");
    	assertEquals(3, objmap.size());
    }

    @Test // Removing a field that is an extra does modify the size
    public void remove_size_extra() {
    	ObjectMap objmap = new ObjectMap(new ObjRemove());
    	objmap.put("field_extra", "extra field value");
    	assertEquals(4, objmap.size());
    	
    	objmap.remove("field_extra");
    	assertEquals(3, objmap.size());
    }

   @Test 
    public void remove_extras() {
    	ObjectMap objmap = new ObjectMap(new ObjRemove());
    	objmap.put("field_extra", "extra field value");
    	assertEquals("extra field value", objmap.get("field_extra"));
    	
    	objmap.remove("field_extra");
    	assertEquals(null, objmap.get("field_extra"));
    }

    @Test // Check that the backing object is updated
    public void remove_backer_get() {
    	ObjRemove obj = new ObjRemove();
    	ObjectMap objmap = new ObjectMap(obj);
    	objmap.remove("field_pub");
    	assertEquals(null, obj.field_pub);
    }



    // TODO: Add tests for the following.
    /**
     * public Object Object put(String key, Object value)
     */

    /**
     * public void putAll(Map<? extends String, ? extends Object> map) 
     */

    // TODO: Add tests for Converter
    
    
    
}
