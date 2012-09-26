package com.psddev.dari.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Ignore;
import org.junit.Test;

public class CollectionUtilsTest {
	
	/**
	 * public static <T> T getByPath(Object object, String path)
	 */
	@Test // null path with list returns null
	public void getByPath_path_null_list() {
		List<Integer> input = Arrays.asList(1,2,3);
		assertEquals(null, CollectionUtils.getByPath(input, null));
	}
	@Test // null path with map returns value of null key if there is one
	public void getByPath_path_null_map_with_key() {
		Map<String,Integer> input = new HashMap<String,Integer>() {{
			put("a", 1);
			put(null, 2);
		}};
		assertEquals(2, CollectionUtils.getByPath(input, null));
	}
	@Test // null path with map returns null if not null key
	public void getByPath_path_null_map_without_key() {
		Map<String,Integer> input = new HashMap<String,Integer>() {{
			put("a", 1);
			put("b", 2);
		}};
		assertEquals(null, CollectionUtils.getByPath(input, null));
	}
	@Ignore // TODO: Fixme, only ignored so that we dont break builds when merging with main. Not sure what correct result should be
	@Test // Not sure what it "should" do... maybe empty path just returns the object passed in?
	public void getByPath_path_empty() {
		assertEquals(Arrays.asList(1,2,3), CollectionUtils.getByPath(Arrays.asList(1,2,3), ""));
	}

	/* Lists */
	@Test // lists are zero indexed as normal
	public void getByPath_path_list_first() {
		List<String> input = Arrays.asList("a", "b", "c");
		assertEquals("a", CollectionUtils.getByPath(input, "0"));
	}
	@Test // index out of range returns null
	public void getByPath_path_list_oorange() {
		List<String> input = Arrays.asList("a", "b", "c");
		assertEquals(null, CollectionUtils.getByPath(input, "3"));
	}
	@Test // negative index counts back from the end
	public void getByPath_path_list_negative() {
		List<String> input = Arrays.asList("a", "b", "c");
		assertEquals("c", CollectionUtils.getByPath(input, "-1"));
	}
	@Test // too large negative index returns same as oorgange, null
	public void getByPath_path_list_oorange_negative() {
		List<String> input = Arrays.asList("a", "b", "c");
		assertEquals(null, CollectionUtils.getByPath(input, "-4"));
	}
	
	/* Maps */
	@Test
	public void getByPath_path_map() {
		Map<String,String> input = ObjectUtils.to(Map.class, Arrays.asList("a", "x", "b", "y", "c", "z"));
		assertEquals("x", CollectionUtils.getByPath(input, "a"));
	}
	@Test
	public void getByPath_path_map_unknown_key() {
		Map<String,String> input = ObjectUtils.to(Map.class, Arrays.asList("a", "x", "b", "y", "c", "z"));
		assertEquals(null, CollectionUtils.getByPath(input, "d"));
	}

	/* Composite */
	@Test
	public void getByPath_path_map_tree() {
		Map<String,Object> input = ObjectUtils.to(Map.class, Arrays.asList("a", "x", "b", Arrays.asList(10,20,30), "c", "z"));
		assertEquals(20, CollectionUtils.getByPath(input, "b/1"));
	}
	public void getByPath_path_tree_map() {
		List<Object> input = Arrays.asList("A", ObjectUtils.to(Map.class, Arrays.asList("a", "x", "b", "y")), "C");
		assertEquals("y", CollectionUtils.getByPath(input, "1/b"));
	}

	/* Edge Cases */
	@Test // Paths that go deeper than the nesting of collections return null
	public void getByPath_path_extra_paths() {
		Map<String,String> input = ObjectUtils.to(Map.class, Arrays.asList("a", "x", "b", "y", "c", "z"));
		assertEquals(null, CollectionUtils.getByPath(input, "a/1"));
	}

	
	
	/**
	 * public static <T> T putByPath(Map<String, Object> map, String path, T value)
	 */
	@Test // if the path is null, just add the value to the top level Map with a null key
	public void putByPath_path_null() {
		Map<String,Object> input = new HashMap<String,Object>();
		Map<String,Object> expect = new HashMap<String,Object>() {{
			put(null, "value");
		}};
		CollectionUtils.putByPath(input, null, "value");
		assertEquals(expect, input);
	}

	@Test // single level, just put the key/value in the top level map
	public void putByPath_path_single_level() {
		Map<String,Object> input = new HashMap<String,Object>();
		Map<String,Object> expect = new HashMap<String,Object>() {{
			put("a", "value");
		}};
		CollectionUtils.putByPath(input, "a", "value");
		assertEquals(expect, input);
	}
	
	@Test // two levels, make sure the key/value gets put in the sub-map
	public void putByPath_path_two_level() {
		Map<String,Object> input = new HashMap<String,Object>() {{
			put("a", new HashMap<String,Object>());
		}};
		Map<String,Object> expect = new HashMap<String,Object>() {{
			put("a", new HashMap<String,Object>() {{
				put("b", "value");
			}});
		}};
		CollectionUtils.putByPath(input, "a/b", "value");
		assertEquals(expect, input);
	}

	@Test // if the path is deeper than the map level, add maps until the depth is correct
	public void putByPath_path_deeper_than_maps() {
		Map<String,Object> input = new HashMap<String,Object>();
		Map<String,Object> expect = new HashMap<String,Object>() {{
			put("a", new HashMap<String,Object>() {{
				put("b", new HashMap<String,Object>() {{
					put("c", "value");
				}});
			}});
		}};
		CollectionUtils.putByPath(input, "a/b/c", "value");
		assertEquals(expect, input);
	}

	@Test // if the path causes the creation of a submap, it replaces an actual value in that slot
	public void putByPath_path_replaces_value() {
		Map<String,Object> input = new HashMap<String,Object>() {{
			put("a", "1");
		}};
		Map<String,Object> expect = new HashMap<String,Object>() {{
			put("a", new HashMap<String,Object>() {{
				put("b", new HashMap<String,Object>() {{
					put("c", "value");
				}});
			}});
		}};
		CollectionUtils.putByPath(input, "a/b/c", "value");
		assertEquals(expect, input);
	}
	
	@Test // adding elements doesn't impact currently existing elements that don't overlap
	public void putByPath_path_existing_not_overlapping() {
		Map<String,Object> input = new HashMap<String,Object>() {{
			put("x", "1");
			put("y", new HashMap<String,Object>() {{
				put("z", "2");
			}});
		}};
		Map<String,Object> expect = new HashMap<String,Object>() {{
			put("x", "1");
			put("y", new HashMap<String,Object>() {{
				put("a", "value 2");
				put("z", "2");
			}});
			put("a", new HashMap<String,Object>() {{
				put("b", new HashMap<String,Object>() {{
					put("c", "value 1");
				}});
			}});
		}};
		CollectionUtils.putByPath(input, "a/b/c", "value 1");
		CollectionUtils.putByPath(input, "y/a", "value 2");
		assertEquals(expect, input);
	}

	@Test // the return value is the value that was returned from adding the value from the nested map
	public void putByPath_return_value() {
		Map<String,Object> input = new HashMap<String,Object>() {{
			put("a", new HashMap<String,Object>() {{
				put("b", new HashMap<String,Object>() {{
					put("c", "value 1");
				}});
			}});
		}};
		assertEquals("value 1", CollectionUtils.putByPath(input, "a/b/c", "value 2"));
		assertEquals(null, CollectionUtils.putByPath(input, "a/b/d", "value 3"));
	}
}
