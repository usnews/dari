package com.psddev.dari.db;

/** All possible write operations that can be executed in a database. */
public enum WriteOperation {

    SAVE() {
        @Override
        public void execute(Database database, State state) {
            database.save(state);
        }
    },

    SAVE_UNSAFELY() {
        @Override
        public void execute(Database database, State state) {
            database.saveUnsafely(state);
        }
    },

    DELETE() {
        @Override
        public void execute(Database database, State state) {
            database.delete(state);
        }
    },

    INDEX() {
        @Override
        public void execute(Database database, State state) {
            database.index(state);
        }
    };

    /**
     * Executes this write operation on the given {@code state} within
     * the given {@code database}.
     */
    public abstract void execute(Database database, State state);
}
