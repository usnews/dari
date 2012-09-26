package com.psddev.dari.util;

import java.util.*;
import org.junit.*;
import static org.junit.Assert.*;

public class TransformerBuilderTest {

    @Test
    public void test_mapType() {
        assertEquals(TreeMap.class, new TransformerBuilder()
                .setMapType(TreeMap.class)
                .includeAllFields(Foo.class)
                .toTransformer()
                .transform(new Foo())
                .getClass());
    }

    @Test
    public void test_transform() {

        Map<String, String> expected = new HashMap<String, String>();
        expected.put("field1", "foo1");
        expected.put("field2", "foo2");
        expected.put("extra", "bar");

        assertEquals(expected, new TransformerBuilder()
                .includeAllFields(Foo.class)
                .includeGetters(Bar.class, "extra")
                .toTransformer()
                .transform(new Bar()));
    }

    private static class Foo {

        private String field1 = "foo1";
        private String field2 = "foo2";
        private transient String ignored = "foo3";
    }

    private static class Bar extends Foo {

        public String getExtra() {
            return "bar";
        }
    }
}
