package com.psddev.dari.util;

import java.util.*;
import org.junit.*;
import static org.junit.Assert.*;

public class CascadingMapTest {

    private Map<String, String> emptySource;
    private Map<String, String> source1;
    private Map<String, String> source2;
    private List<Map<String, String>> sources;
    private CascadingMap<String, String> cascading;
    private Map<String, String> combined;

    @Before
    public void before() {
        emptySource = Collections.emptyMap();

        source1 = new LinkedHashMap<String, String>();
        source1.put("key0", "key0source1");
        source1.put("key1", "key1source1");
        source1.put("null1", null);

        source2 = new LinkedHashMap<String, String>();
        source2.put("key1", "key1source2");
        source2.put("key2", "key2source2");
        source2.put("null2", null);

        sources = new ArrayList<Map<String, String>>();
        sources.add(emptySource);
        sources.add(source1);
        sources.add(source2);

        cascading = new CascadingMap<String, String>(sources);

        combined = new LinkedHashMap<String, String>();
        combined.putAll(source2);
        combined.putAll(source1);
        combined.putAll(emptySource);
    }

    @After
    public void after() {
        emptySource = null;
        source1 = null;
        source2 = null;
        sources = null;
        cascading = null;
        combined = null;
    }

    @Test
    public void test_getSources() {
        assertEquals(sources, cascading.getSources());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_clear() {
        cascading.clear();
    }
    
    @Test
    public void test_containsKey_first() {
    	assertTrue(cascading.containsKey("key0"));
    }
    
    @Test
    public void test_containsKey_second() {
    	assertTrue(cascading.containsKey("key2"));
    }

    @Test
    public void test_containsKey_both() {
    	assertTrue(cascading.containsKey("key1"));
    }

    @Test
    public void test_containsKey_missing() {
    	assertFalse(cascading.containsKey("key3"));
    }
    
    @Test
    public void test_containsValue_first() {
        assertTrue(cascading.containsValue("key0source1"));
    }

    @Test
    public void test_containsValue_second() {
        assertTrue(cascading.containsValue("key2source2"));
    }

    @Test
    public void test_containsValue_both() {
        assertTrue(cascading.containsValue("key1source1"));
    }

    @Test
    public void test_containsValue_overridden() {
        assertFalse(cascading.containsValue("key1source2"));
    }

    @Test
    public void test_containsValue_unknown() {
        assertFalse(cascading.containsValue("unknown"));
    }

    @Test
    public void test_containsValue_null() {
        assertTrue(cascading.containsValue(null));
    }
    
    @Test
    public void test_entrySet() {
        assertEquals(combined.entrySet(), cascading.entrySet());
    }
    
    @Test
    public void test_get_first() {
        assertEquals(source1.get("key0"), cascading.get("key0"));
    }

    @Test
    public void test_get_second() {
        assertEquals(source2.get("key2"), cascading.get("key2"));
    }

    @Test
    public void test_get_overridden() {
        assertEquals(source1.get("key1"), cascading.get("key1"));
    }

    @Test
    public void test_get_unknown() {
        assertEquals(source1.get("unknown"), cascading.get("unknown"));
    }

    @Test
    public void test_get_missing() {
        assertEquals(null, cascading.get("missing"));
    }
    
    @Test
    public void test_isEmpty_false() {
        assertFalse(cascading.isEmpty());
    }
    
    @Test
    public void test_isEmpty_true() {
        assertTrue(new CascadingMap<String, String>().isEmpty());
    }

    @Test
    public void test_keySet() {
        assertEquals(combined.keySet(), cascading.keySet());
    }

    @Test
    public void test_keySet_empty() {
        assertEquals(Collections.<String>emptySet(), new CascadingMap<String, String>().keySet());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_put() {
        cascading.put("foo", "bar");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_putAll() {
        cascading.putAll(emptySource);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_remove() {
        cascading.remove("foo");
    }

    @Test
    public void test_size() {
        assertEquals(combined.size(), cascading.size());
    }

    @Test
    public void test_size_0() {
        assertEquals(0, new CascadingMap<String, String>().size());
    }

    @Test
    public void test_values_size() {
        Set<String> keys = cascading.keySet();
        Collection<String> values = cascading.values();

        assertEquals(keys.size(), values.size());

        for (Iterator<String> ki = keys.iterator(), vi = values.iterator(); ki.hasNext() && vi.hasNext(); ) {
            assertEquals(cascading.get(ki.next()), vi.next());
        }
    }

    @Test
    public void test_equals() {
        assertEquals(combined, cascading);
    }
}
