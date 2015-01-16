package com.psddev.dari.db;

import java.util.IdentityHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Skeletal {@link Trigger} implementation that will only fire once.
 */
public abstract class TriggerOnce implements Trigger {

    private static final Logger LOGGER = LoggerFactory.getLogger(TriggerOnce.class);

    private final Map<Object, Void> executed = new IdentityHashMap<Object, Void>();

    /**
     * Executes this trigger only once on the given {@code object}.
     *
     * @param object Can't be {@code null}.
     */
    protected abstract void executeOnce(Object object);

    @Override
    public final void execute(Object object) {
        if (executed.containsKey(object)) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                        "Already fired trigger [{}] from [{}] on [{}]",
                        new Object[] { this, object.getClass().getName(), State.getInstance(object).getId() });
            }

        } else {
            executed.put(object, null);
            executeOnce(object);
        }
    }

    protected boolean isMissing(Class<?> cls) {
        return false;
    }
}
