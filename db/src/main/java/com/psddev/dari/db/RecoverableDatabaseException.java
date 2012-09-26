package com.psddev.dari.db;

/**
 * Thrown when there's a recoverable error while executing any of the
 * {@link Database} methods.
 */
@SuppressWarnings("serial")
public class RecoverableDatabaseException extends DatabaseException {

    public RecoverableDatabaseException(Database database) {
        super(database);
    }

    public RecoverableDatabaseException(Database database, String message) {
        super(database, message);
    }

    public RecoverableDatabaseException(Database database, String message, Throwable cause) {
        super(database, message, cause);
    }

    public RecoverableDatabaseException(Database database, Throwable cause) {
        super(database, cause);
    }
}
