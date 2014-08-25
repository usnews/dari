package com.psddev.dari.maven;

import java.lang.reflect.Method;

import org.apache.maven.plugin.logging.Log;

import com.psddev.dari.util.sa.JvmLogger;

public class AnalyzeAllLogger extends JvmLogger {

    private final Log log;
    private boolean hasErrors;

    public AnalyzeAllLogger(Log log) {
        this.log = log;
    }

    public boolean hasErrors() {
        return hasErrors;
    }

    @Override
    public void info(Method method, int line, String message) {
        log.info(format(method, line, message));
    }

    @Override
    public void warn(Method method, int line, String message) {
        log.warn(format(method, line, message));
    }

    @Override
    public void error(Method method, int line, String message) {
        hasErrors = true;

        log.error(format(method, line, message));
    }
}
