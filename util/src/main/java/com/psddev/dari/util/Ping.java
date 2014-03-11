package com.psddev.dari.util;

import java.util.HashMap;
import java.util.Map;

/** Pings a service to ensure that it's operational. */
public interface Ping {

    /**
     * Implementation should be quick to return and be careful not to throw
     * any unintentional errors that'd mark the service non-operational.
     */
    public void ping() throws Throwable;

    /** {@link Ping} utility methods. */
    public static final class Static {

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

            for (Class<? extends Ping> pingClass : ClassFinder.Static.findClasses(Ping.class)) {
                errors.put(pingClass, ping(pingClass));
            }

            return errors;
        }
    }
}
