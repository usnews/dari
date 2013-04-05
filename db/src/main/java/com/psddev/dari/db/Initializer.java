package com.psddev.dari.db;

import java.util.Set;

import org.slf4j.Logger;

/**
 * Automatically initializes a {@linkplain Database database}.
 *
 * @deprecated No replacement.
 */
@Deprecated
public interface Initializer {

    /**
     * Returns a set of initializer classes that this one depends on.
     *
     * @return May be {@code null} if there are no dependencies.
     */
    public Set<Class<? extends Initializer>> dependencies();

    /**
     * Executes this initializer on the given {@code database} and writes
     * any messages to the given {@code logger}.
     */
    public void execute(Database database, Logger logger) throws Exception;
}
