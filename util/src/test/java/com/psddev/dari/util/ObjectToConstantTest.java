package com.psddev.dari.util;

import java.util.*;
import org.junit.*;
import static org.junit.Assert.*;

public class ObjectToConstantTest {

    @Test
    public void test_constant() {
        ObjectToConstant<String> toFoo = ObjectToConstant.getInstance("foo");
        assertEquals("foo", toFoo.convert(null, null, 1));
    }

    @Test
    public void test_null() {
        ObjectToConstant<Object> toNull = ObjectToConstant.getInstance(null);
        assertEquals(null, toNull.convert(null, null, 1));
    }
}
