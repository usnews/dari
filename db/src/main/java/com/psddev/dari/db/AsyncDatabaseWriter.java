package com.psddev.dari.db;

import com.psddev.dari.util.AsyncConsumer;
import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.ObjectUtils;

import java.util.ArrayList;
import java.util.List;

/** Background task that efficiently writes to a database from a queue. */
public class AsyncDatabaseWriter<E> extends AsyncConsumer<E> {

    public static final double DEFAULT_COMMIT_SIZE_JITTER = 0.2;

    private final Database database;
    private final WriteOperation operation;
    private final int commitSize;
    private final boolean isCommitEventually;

    private double commitSizeJitter = DEFAULT_COMMIT_SIZE_JITTER;

    private transient int nextCommitSize;
    private transient State lastState;
    private final transient List<State> states = new ArrayList<State>();

    /**
     * Creates a new instance that runs in the given {@code executor},
     * consumes items from the given {@code input} queue, and writes
     * them to the given {@code database}.
     *
     * @param executor If {@code null}, uses the default executor.
     * @param input Can't be {@code null}.
     * @param database Can't be {@code null}.
     * @param operation Can't be {@code null}.
     * @param commitSize Number of items to save in a single commit.
     * @param isCommitEventually If {@code true},
     *        {@link Database#commitWritesEventually} is used instead of
     *        {@link Database#commitWrites}.
     * @throws IllegalArgumentException If the given {@code input}
     *         or {@code database} is {@code null}.
     */
    public AsyncDatabaseWriter(
            String executor,
            AsyncQueue<E> input,
            Database database,
            WriteOperation operation,
            int commitSize,
            boolean isCommitEventually) {

        super(executor, input);

        if (database == null) {
            throw new IllegalArgumentException("Database can't be null!");
        }
        if (operation == null) {
            throw new IllegalArgumentException("Operation can't be null!");
        }

        this.database = database;
        this.operation = operation;
        this.commitSize = commitSize;
        this.isCommitEventually = isCommitEventually;
    }

    /**
     * Returns the {@linkplain ObjectUtils#jitter jitter scale} to apply
     * to the commit size.
     */
    public double getCommitSizeJitter() {
        return commitSizeJitter;
    }

    /**
     * Sets the {@linkplain ObjectUtils#jitter jitter scale} to apply
     * to the commit size.
     */
    public void setCommitSizeJitter(double commitSizeJitter) {
        this.commitSizeJitter = commitSizeJitter;
    }

    // Commits all pending writes.
    private void commit() {
        try {
            database.beginWrites();

            for (State state : states) {
                operation.execute(database, state);
            }

            if (isCommitEventually) {
                database.commitWritesEventually();
            } else {
                database.commitWrites();
            }

        } finally {
            states.clear();
            database.endWrites();
        }
    }

    // --- AsyncConsumer support ---

    // Calculates the number of items to save in the next commit.
    private void calculateNextCommitSize() {
        nextCommitSize = (int) ObjectUtils.jitter(commitSize, getCommitSizeJitter());
    }

    @Override
    protected void beforeStart() {
        super.beforeStart();
        calculateNextCommitSize();
    }

    @Override
    protected void consume(E item) {
        lastState = State.getInstance(item);
        states.add(lastState);
        if (states.size() >= nextCommitSize) {
            calculateNextCommitSize();
            commit();
        }
    }

    @Override
    protected void finished() {
        super.finished();
        commit();

        if (lastState != null) {
            database.beginWrites();
            try {
                operation.execute(database, lastState);
                database.commitWrites();
            } finally {
                database.endWrites();
            }
        }
    }

    // --- Deprecated ---

    /** @deprecated Use {@link #AsyncDatabaseWriter(String, AsyncQueue, Database, WriteOperation, int, boolean)} instead. */
    @Deprecated
    public AsyncDatabaseWriter(
            AsyncQueue<E> input,
            Database database,
            WriteOperation operation,
            int commitSize,
            boolean isCommitEventually) {

        this(
                null,
                input,
                database,
                operation,
                commitSize,
                isCommitEventually);
    }

    /** @deprecated Use {@link #AsyncDatabaseWriter(String, AsyncQueue, Database, WriteOperation, int, boolean)} instead. */
    @Deprecated
    public AsyncDatabaseWriter(
            AsyncQueue<E> input,
            Database database,
            int commitSize,
            boolean isCommitEventually,
            boolean isSaveUnsafely) {

        this(
                null,
                input,
                database,
                isSaveUnsafely ? WriteOperation.SAVE_UNSAFELY : WriteOperation.SAVE,
                commitSize,
                isCommitEventually);
    }
}
