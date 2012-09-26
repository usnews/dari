package com.psddev.dari.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import java.util.logging.Filter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

/**
 * Captures log messages in this thread.
 * @author Hyoo Lim
 */
public class LogCapture {

    private final String _id = UUID.randomUUID().toString();
    private final Map<Logger, CaptureHandler> _handlers
            = new HashMap<Logger, CaptureHandler>();
    private final List<String> _logs = new ArrayList<String>();

    public void putLogger(Logger logger, Level level) {
        _handlers.put(logger, new CaptureHandler(logger.getLevel(), level));
    }

    public void removeLogger(Logger logger) {
        _handlers.remove(logger);
    }

    /** Starts capturing log messages. */
    public void start() {
        for (Map.Entry<Logger, CaptureHandler> e : _handlers.entrySet()) {
            Logger logger = e.getKey();
            CaptureHandler handler = e.getValue();
            logger.setLevel(handler.getNewLevel());
            logger.addHandler(handler);
            logger.log(handler.getNewLevel(), "Started log capture: " + _id);
        }
        _logs.clear();
    }

    /** Stops capturing log messages and returns them. */
    public List<String> stop() {
        for (Map.Entry<Logger, CaptureHandler> e : _handlers.entrySet()) {
            Logger logger = e.getKey();
            CaptureHandler handler = e.getValue();
            logger.setLevel(handler.getOldLevel());
            logger.removeHandler(handler);
        }
        return _logs;
    }

    private class CaptureHandler extends Handler {

        private Level _oldLevel;
        private Level _newLevel;

        public CaptureHandler(Level oldLevel, Level newLevel) {
            _oldLevel = oldLevel;
            _newLevel = newLevel;
            setLevel(Level.ALL);
            setFormatter(new SimpleFormatter());
            setFilter(new CaptureFilter());
        }

        public Level getOldLevel() {
            return _oldLevel;
        }

        public Level getNewLevel() {
            return _newLevel;
        }

        @Override
        public void close() {
        }

        @Override
        public void flush() {
        }

        @Override
        public void publish(LogRecord record) {
            if (getFilter().isLoggable(record)) {
                _logs.add(getFormatter().format(record));
            }
        }

    }

    private class CaptureFilter implements Filter {

        private int _threadId;

        @Override
        public boolean isLoggable(LogRecord record) {

            // the real java thread id is not available via LogRecord,
            // so use the _id generated at the time of the LogCapture
            // object construction to map the LogRecord-specific thread id
            if (_threadId == 0) {
                String message = record.getMessage();
                if (message != null && message.contains(_id)) {
                    _threadId = record.getThreadID();
                }
                return false;
            } else {
                return _threadId == record.getThreadID();
            }
        }

    }

}
