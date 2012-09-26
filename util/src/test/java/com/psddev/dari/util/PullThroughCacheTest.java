package com.psddev.dari.util;

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Really only testing the very basics of functionality here. The actual code is a lot more complex
 * than one would guess by the functionality, since it has to handle multiple threads, recursive calls, 
 * etc.
 * 
 * @author rhseeger
 *
 */
public class PullThroughCacheTest {
	PullThroughCache<String,String> cache;
	
	static class TestCache extends PullThroughCache<String,String> {
		Map<String,Integer> counts = new HashMap<String,Integer>();
		public String produce(String s) {
			if(s == null) {
				throw new NullPointerException();
			}
			if(!s.toLowerCase().equals(s)) {
				throw new IllegalStateException("use only lower case letters");
			}
			if(!counts.containsKey(s)) {
				counts.put(s, new Integer(0));
			}
			counts.put(s, counts.get(s)+1);
			return s.toUpperCase() + counts.get(s);
		}
	}


    @Before
    public void before() {
    	cache = new TestCache();
    }

    @After
    public void after() {}

    /**
     * public boolean isProduced(K key)
     */
    @Test
    public void isProduced_false() {
    	assertEquals(false, cache.isProduced("a"));
    }
    @Test
    public void isProduced_true() {
    	cache.get("a");
    	assertEquals(true, cache.isProduced("a"));
    }

    /**
     * public boolean containsKey(Object key)
     */
    @Test
    public void containsKey_true() {
    	assertEquals(true, cache.containsKey("a"));
    }
    
    @Ignore // TODO: This currently fails, with true being returned (for a Date key value is a String,String cache)
    @Test
    public void containsKey_false() {
    	assertEquals(false, cache.containsKey(new Date()));
    }

    /**
     * public V get(Object key)
     */
    @Test
    public void get() {
    	assertEquals("A1", cache.get("a"));
    }
    @Test // the second time should retrieve it from the cache, so we get the first instance returned
    public void get_twice() {
    	cache.get("a");
    	assertEquals("A1", cache.get("a"));
    }
    
    /**
     * public synchronized void invalidate()
     */
    @Test // invalidating the key means it will be refetched
    public void invalidate_all() {
    	cache.get("a");
    	cache.invalidate();
    	assertEquals("A2", cache.get("a"));
    }

    /**
     * public synchronized void invalidate(K key)
     */
    @Test // invalidating the key means it will be refetched
    public void invalidate_key() {
    	cache.get("a");
    	cache.invalidate("a");
    	assertEquals(false, cache.isProduced("a"));
    	assertEquals("A2", cache.get("a"));
    }
    @Test // invalidating the key means it will be refetched
    public void invalidate_key_other() {
    	cache.get("a");
    	cache.get("b");
    	cache.invalidate("a");
    	assertEquals("A2", cache.get("a"));
    	assertEquals("B1", cache.get("b"));
    }

    /**
     * protected boolean isExpired(K key, Date lastProduceDate)
     * Test that overriding isExpired effects production
     */
    @Test // invalidating the key means it will be refetched
    public void isExpired() {
    	cache = new TestCache() {
    		@Override
    		public boolean isExpired(String key, Date lastProduceDate) {
    			return true;
    		}
    	};
    	cache.get("a");
    	assertEquals("A2", cache.get("a"));
    }

}
