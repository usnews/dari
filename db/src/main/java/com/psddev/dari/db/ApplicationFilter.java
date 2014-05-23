package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.Filter;

import com.psddev.dari.util.AbstractFilter;
import com.psddev.dari.util.StandardFilter;

/**
 * Takes care of initializing and destroying all the components used in
 * a typical Dari application.
 *
 * <p>This filter loads:</p>
 *
 * <ul>
 * <li>{@link StandardFilter}</li>
 * <li>{@link WebResourceOverrideFilter}</li>
 * <li>{@link ResetFilter}</li>
 * <li>{@link ProfilingDatabaseFilter}</li>
 * <li>{@link CachingDatabaseFilter}</li>
 * </ul>
 */
public class ApplicationFilter extends AbstractFilter {

    // --- AbstractFilter support ---

    @Override
    protected Iterable<Class<? extends Filter>> dependencies() {
        List<Class<? extends Filter>> dependencies = new ArrayList<Class<? extends Filter>>();
        dependencies.add(StandardFilter.class);
        dependencies.add(WebResourceOverrideFilter.class);
        dependencies.add(ResetFilter.class);
        dependencies.add(ProfilingDatabaseFilter.class);
        dependencies.add(CachingDatabaseFilter.class);
        return dependencies;
    }
}
