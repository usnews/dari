package com.psddev.dari.util;

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

public class CascadingMapTest {

    private Map<String, String> _source1;
    private Map<String, String> _source2;
    private CascadingMap<String, String> _cascading;
    private Map<String, String> _combined;

    @Before
    public void before() {

        _source1 = new LinkedHashMap<String, String>();
        _source1.put("key0", "key0_source1");
        _source1.put("key1", "key1_source1");
        _source1.put("null1", null);

        _source2 = new LinkedHashMap<String, String>();
        _source2.put("key1", "key1_source2");
        _source2.put("key2", "key2_source2");
        _source2.put("null2", null);

        _cascading = new CascadingMap<String, String>(
                Collections.<String, String>emptyMap(), _source1, _source2);

        _combined = new LinkedHashMap<String, String>();
        _combined.putAll(_source2);
        _combined.putAll(_source1);
    }

    @After
    public void after() {
        _source1 = null;
        _source2 = null;
        _cascading = null;
        _combined = null;
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_clear() {
        _cascading.clear();
    }

    
    /**
     * public boolean containsKey(Object key) {
     */
    
    @Test // make sure a key from the first map is in the cascading
    public void test_containsKey_first_only() {
    	assertTrue(_cascading.containsKey("key0"));
    }
    
    @Test // make sure a key from the second map is in the cascading
    public void test_containsKey_second_only() {
    	assertTrue(_cascading.containsKey("key2"));
    }

    @Test // make sure an overridden key is in the cascading
    public void test_containsKey_both() {
    	assertTrue(_cascading.containsKey("key1"));
    }

    @Test // make sure a key in neither map is not in the second
    public void test_containsKey_missing() {
    	assertFalse(_cascading.containsKey("key3"));
    }

    
    /**
     * public boolean containsValue(Object value) {
     */
  
    @Test
    public void test_containsValue_first_only() {
        assertTrue(_cascading.containsValue("key0_source1"));
    }

    @Test
    public void test_containsValue_second_only() {
        assertTrue(_cascading.containsValue("key2_source2"));
    }

    @Test // The first map on the list overrides the second map on the list
    public void test_containsValue_both() {
    	System.out.println("values: " + _cascading.values());
        assertTrue(_cascading.containsValue("key1_source1"));
    }

    @Test
    public void test_containsValue_overridden() {
        assertTrue(_cascading.containsValue("key1_source1"));
        assertFalse(_cascading.containsValue("key1_source2"));
    }

    @Test
    public void test_containsValue_unknown() {
        assertFalse(_cascading.containsValue("unknown value"));
    }

    
    /**
     * public Set<Map.Entry<K, V>> entrySet()
     */
    
    @Test
    public void test_entrySet() {
        assertEquals(_combined.entrySet(), _cascading.entrySet());
    }

    
    /**
     * public V get(Object key)
     */
    
    @Test
    public void test_get_first_only() {
        assertEquals(_source1.get("key0"), _cascading.get("key0"));
    }

    @Test
    public void test_get_second_only() {
        assertEquals(_source2.get("key2"), _cascading.get("key2"));
    }

    @Test
    public void test_get_overridden() {
        assertEquals(_source1.get("key1"), _cascading.get("key1"));
    }

    @Test
    public void test_get_unknown() {
        assertEquals(_source1.get("keyUnknown"), _cascading.get("keyUnknown"));
    }

    
    /**
     * public boolean isEmpty()
     */
    
    @Test
    public void test_isEmpty_false() {
        assertFalse(_cascading.isEmpty());
    }
    
    @Test
    public void test_isEmpty_true() {
        assertTrue((new CascadingMap<String, String>(Collections.<String, String>emptyMap())).isEmpty());
    }

    /**
     * public Set<K> keySet()
     */
    
    @Test
    public void test_keySet() {
        assertEquals(_combined.keySet(), _cascading.keySet());
    }

    @Test
    public void test_keySet_empty() {
        assertEquals(Collections.<String>emptySet(), (new CascadingMap<String, String>(Collections.<String, String>emptyMap())).keySet());
    }


    @Test(expected = UnsupportedOperationException.class)
    public void test_put() {
        _cascading.put("foo", "bar");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_putAll() {
        _cascading.putAll(Collections.<String, String>emptyMap());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_remove() {
        _cascading.remove("foo");
    }

    /**
     * public int size()
     */
    
    @Test
    public void test_size() {
        assertEquals(_combined.size(), _cascading.size());
    }
    @Test
    public void test_size_0() {
        assertEquals(0, (new CascadingMap<String, String>(Collections.<String, String>emptyMap())).size());
    }

    /**
     * public Collection<V> values()
     */
    @Test
    public void test_values() {
        Set<String> keys = _cascading.keySet();
        Collection<String> values = _cascading.values();
        assertEquals(keys.size(), values.size());
        for (Iterator<String> ki = keys.iterator(), vi = values.iterator();
                ki.hasNext() && vi.hasNext(); ) {
            assertEquals(_cascading.get(ki.next()), vi.next());
        }
    }
}
