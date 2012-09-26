package com.psddev.dari.util;

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

public class DependencyResolverTest {
	String obj1 = "a";
	String obj2 = "b";
	String obj3 = "c";
	String obj4 = "d";
	DependencyResolver<String> resolver = new DependencyResolver<String>();
	
    @Before
    public void before() {}

    @After
    public void after() {}

    /**
     * public List<T> resolve()
     */
    @Test 
    public void resolve_dependants_returned_first() {
    	resolver.addRequired(obj1, obj2);
        assertEquals(Arrays.asList(obj2,obj1), resolver.resolve());
    }

    @Test
    public void resolve_all_dependants_returned() {
    	resolver.addRequired(obj1, obj2, obj3);
    	List<String> result = resolver.resolve();
    	// We don't know what order the dependants will be returned in
    	assertTrue(result.equals(Arrays.asList("c","b","a")) || result.equals(Arrays.asList("b","c","a")));
    }

    @Test
    public void resolve_chained() {
    	resolver.addRequired(obj1, obj2);
    	resolver.addRequired(obj2, obj3);
    	assertEquals(Arrays.asList("c","b","a"), resolver.resolve());
    }

    @Test
    public void resolve_diamond() {
    	resolver.addRequired(obj1, obj2);
    	resolver.addRequired(obj1, obj3);
    	resolver.addRequired(obj2, obj4);
    	resolver.addRequired(obj3, obj4);
    	List<String> result = resolver.resolve();
    	// We don't know what order the middle dependants will be returned in
    	assertTrue(result.equals(Arrays.asList("d","c","b","a")) || result.equals(Arrays.asList("d","b","c","a")));
    }

    @Test (expected=IllegalStateException.class)
    public void resolve_cyclic_simple() {
    	resolver.addRequired(obj1, obj2);
    	resolver.addRequired(obj2, obj1);
    	List<String> result = resolver.resolve();
    }

    @Test (expected=IllegalStateException.class)
    public void resolve_cyclic_complex() {
    	resolver.addRequired(obj1, obj2);
    	resolver.addRequired(obj2, obj3);
    	resolver.addRequired(obj3, obj1);
    	List<String> result = resolver.resolve();
    }

    /*
     * nulls -- TODO: Figure out what the correct behavior is for null in depender, dependant
     */
    @Ignore // TODO: Is this the right behavior
    @Test // The dependant gets added, but not the null
    public void resolve_null_depender_1() {
    	resolver.addRequired((String)null, obj2);
    	assertEquals(Arrays.asList("b"), resolver.resolve());
    }
    
    @Ignore // TODO: This is probably wrong, but only because it's complaining about a Cyclic dependancy in the exception
    @Test (expected=IllegalStateException.class)
    public void resolve_null_dependant_1() {
    	resolver.addRequired(obj1, (String)null);
    	assertEquals(Arrays.asList("c","b","a"), resolver.resolve());
    }
    
    @Ignore // TODO: Is this the right behavior
    @Test (expected=IllegalStateException.class) // Don't allow null dependers at all
    public void resolve_null_depender_2() {
    	resolver.addRequired((String)null, obj2);
    }
    
    @Ignore // TODO: Is this the right behavior
    @Test (expected=IllegalStateException.class) // Don't allow null dependants at all
    public void resolve_null_dependant_2() {
    	resolver.addRequired(obj1, (String)null);
    }

}
