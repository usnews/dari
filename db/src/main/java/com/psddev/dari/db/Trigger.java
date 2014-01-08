package com.psddev.dari.db;

/**
 * Code that's executed in response to some event in an instance of
 * {@link State}.
 *
 * @see State#fireTrigger
 */
public interface Trigger {

    /**
     * Executes this trigger on the given {@code object}.
     *
     * @param object Can't be {@code null}.
     */
    public void execute(Object object);
}
