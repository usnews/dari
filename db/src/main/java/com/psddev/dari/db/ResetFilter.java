package com.psddev.dari.db;

import java.io.IOException;
import java.util.NoSuchElementException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.AbstractFilter;

/** Resets all components so that they're in a valid state. */
public class ResetFilter extends AbstractFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationFilter.class);

    // --- AbstractFilter support ---

    @Override
    protected void doDestroy() {
        SqlDatabase.closeAll();
        SqlDatabase.Static.deregisterAllDrivers();

        // Clean up after incorrect use of Apache HttpClient 3.x.
        try {
            LOGGER.info("Shutting down [{}]", MultiThreadedHttpConnectionManager.class);
            MultiThreadedHttpConnectionManager.shutdownAll();
        } catch (NoClassDefFoundError ex) {
        }
    }

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        // Clear all default database overrides
        try {
            try {
                while (true) {
                    Database.Static.restoreDefault();
                }
            } catch (NoSuchElementException error) {
            }
        } catch (Throwable ex) {
        }

        // Make sure the databases aren't stuck in read-only mode.
        try {
            Database.Static.setIgnoreReadConnection(false);
        } catch (Throwable ex) {
        }

        // Clear all batch writes.
        try {
            for (Database database : Database.Static.getAll()) {
                try {
                    while (true) {
                        database.endWrites();
                    }
                } catch (IllegalStateException ex) {
                    continue;
                }
            }
        } catch (Throwable ex) {
        }

        chain.doFilter(request, response);
    }
}
