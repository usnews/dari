package com.psddev.dari.util.sa;

import java.lang.reflect.Method;

public class JvmLogger {

    protected String format(Method method, int line, String message) {
        StringBuilder sb = new StringBuilder();

        sb.append('[');
        sb.append(method.getDeclaringClass().getName());
        sb.append('.');
        sb.append(method.getName());
        sb.append(':');
        sb.append(line);
        sb.append("] ");
        sb.append(message);
        return sb.toString();
    }

    public void info(Method method, int line, String message) {
    }

    public void warn(Method method, int line, String message) {
    }

    public void error(Method method, int line, String message) {
    }
}
