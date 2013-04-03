package com.psddev.dari.db;

import java.util.Set;

import org.slf4j.Logger;

/**
 * Automatically initializes all the {@link Recordable} classes found
 * in the current class loader.
 *
 * @deprecated No replacement.
 */
@Deprecated
public class TypeInitializer implements Initializer {

    @Override
    public Set<Class<? extends Initializer>> dependencies() {
        return null;
    }

    @Override
    public void execute(Database database, Logger logger) {
        /*
        Set<Class<?>> classes = (Set) ObjectUtils.findClasses(Recordable.class);
        database.getEnvironment().initializeTypes(classes);
        for (Class<?> objectClass : classes) {
            logger.info("Initialized type: {}", objectClass.getName());
        }
        */
    }
}
