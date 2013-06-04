package com.psddev.dari.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Pings a service to ensure that it's operational. */
public interface Ping {

    /**
     * Implementation should be quick to return and be careful not to throw
     * any unintentional errors that'd mark the service non-operational.
     */
    public void ping() throws Throwable;

    /** {@link Ping} utility methods. */
    public static final class Static {

        private static final PullThroughValue<Set<Class<? extends Ping>>> PING_CLASSES = new PullThroughValue<Set<Class<? extends Ping>>>() {
            @Override
            protected Set<Class<? extends Ping>> produce() {
                return ObjectUtils.findClasses(Ping.class);
            }
        };

        /**
         * Pings a service using an instance of the given {@code pingClass}.
         *
         * @return {@code null} if there wasn't an error.
         */
        public static Throwable ping(Class<? extends Ping> pingClass) {
            try {
                TypeDefinition.getInstance(pingClass).newInstance().ping();
                return null;
            } catch (Throwable error) {
                return error;
            }
        }

        /**
         * Pings all available services.
         *
         * @return Map of all services that ran along with any errors.
         */
        public static Map<Class<?>, Throwable> pingAll() {
            Map<Class<?>, Throwable> errors = new HashMap<Class<?>, Throwable>();
            for (Class<? extends Ping> pingClass : PING_CLASSES.get()) {
                errors.put(pingClass, ping(pingClass));
            }
            return errors;
        }
    }
}
