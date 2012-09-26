package com.psddev.dari.db;

/**
 * Thrown when there's an error while executing any of the
 * {@link Database} methods.
 */
@SuppressWarnings("serial")
public class DatabaseException extends RuntimeException {

    private final Database database;

    public DatabaseException(Database database) {
        super();
        this.database = database;
    }

    public DatabaseException(Database database, String message) {
        super(message);
        this.database = database;
    }

    public DatabaseException(Database database, String message, Throwable cause) {
        super(message, cause);
        this.database = database;
    }

    public DatabaseException(Database database, Throwable cause) {
        super(cause);
        this.database = database;
    }

    /** Returns the database where the error occurred. */
    public Database getDatabase() {
        return database;
    }

    /**
     * Marker interface for an error caused by a read operation taking
     * too long.
     */
    public interface ReadTimeout {
    }

    // --- Deprecated ---

    /** @deprecated Use {@link #DatabaseException(Database)} instead. */
    @Deprecated
    public DatabaseException() {
        this((Database) null);
    }

    /** @deprecated Use {@link #DatabaseException(Database, String)} instead. */
    @Deprecated
    public DatabaseException(String message) {
        this((Database) null, message);
    }

    /** @deprecated Use {@link #DatabaseException(Database, String, Throwable)} instead. */
    @Deprecated
    public DatabaseException(String message, Throwable cause) {
        this((Database) null, message, cause);
    }

    /** @deprecated Use {@link #DatabaseException(Database, Throwable)} instead. */
    @Deprecated
    public DatabaseException(Throwable cause) {
        this((Database) null, cause);
    }
}
