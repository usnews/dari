package com.psddev.dari.db;

import com.psddev.dari.util.AbstractFilter;
import com.psddev.dari.util.Settings;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Enables various per-request database result caching. */
public class CachingDatabaseFilter extends AbstractFilter {

    public static final String CACHE_PARAMETER = "_cache";

    // --- AbstractFilter support ---
    @Override
    protected void doForward(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {
        doRequest(request, response, chain);
    }

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        if (Settings.getOrDefault(boolean.class, "dari/isCachingFilterEnabled", true) &&
                !Boolean.FALSE.toString().equals(request.getParameter(CACHE_PARAMETER))) {

            CachingDatabase caching = new CachingDatabase();
            caching.setDelegate(Database.Static.getDefault());
            SqlDatabase defaultSql = Database.Static.getFirst(SqlDatabase.class);

            try {
                Database.Static.overrideDefault(caching);
                defaultSql.beginThreadLocalReadConnection();

                chain.doFilter(request, response);

            } finally {
                Database.Static.restoreDefault();
                defaultSql.endThreadLocalReadConnection();
            }

        } else {
            chain.doFilter(request, response);
        }
    }
}
