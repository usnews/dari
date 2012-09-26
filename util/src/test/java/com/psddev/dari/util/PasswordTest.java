package com.psddev.dari.util;

import java.util.*;
import org.junit.*;
import static org.junit.Assert.*;

public class PasswordTest {

    @Test
    public void test1() {
        String passwordString = "foo";
        Password password = Password.create(passwordString);
        assertTrue(password.check(passwordString));
    }

    @Test
    public void test2() {
        String passwordString = "foo";
        Password password = Password.createCustom("MD5", "salt", passwordString);
        assertTrue(password.check(passwordString));
    }

    @Test
    public void test3() {
        String passwordString = "foo";
        Password password = Password.valueOf(StringUtils.hex(StringUtils.sha1(passwordString)));
        assertTrue(password.check(passwordString));
    }
}
