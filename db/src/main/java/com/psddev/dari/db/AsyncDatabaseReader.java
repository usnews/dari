package com.psddev.dari.db;

import java.util.Iterator;

import com.psddev.dari.util.AsyncProducer;
import com.psddev.dari.util.AsyncQueue;

/** Background task that efficiently reads from a database into a queue. */
public class AsyncDatabaseReader<E> extends AsyncProducer<E> {

    private final Iterator<E> iterator;

    /**
     * Creates a new instance that runs in the given {@code executor},
     * and produces items that match the given {@code query} from the
     * given {@code database} into the given {@code output} queue.
     *
     * @param executor If {@code null}, uses the default executor.
     * @param output If {@code null}, creates a new queue.
     * @param database Can't be {@code null}.
     * @param query Can't be {@code null}.
     * @throws IllegalArgumentException If the given {@code database}
     *         or {@code query} is {@code null}.
     */
    public AsyncDatabaseReader(
            String executor,
            AsyncQueue<E> output,
            Database database,
            Query<E> query) {

        super(executor, output);

        if (database == null) {
            throw new IllegalArgumentException("Database can't be null!");
        }
        if (query == null) {
            throw new IllegalArgumentException("Query can't be null!");
        }

        this.iterator = query.using(database).iterable(0).iterator();
    }

    /** @deprecated Use {@link #AsyncDatabaseReader(String, AsyncQueue, Database, Query)} instead. */
    @Deprecated
    public AsyncDatabaseReader(AsyncQueue<E> output, Database database, Query<E> query) {
        this(null, output, database, query);
    }

    /** @deprecated Use {@link #AsyncDatabaseReader(String, AsyncQueue, Database, Query)} instead. */
    @Deprecated
    public AsyncDatabaseReader(Database database, Query<E> query) {
        this(null, database, query);
    }

    @Override
    protected E produce() {
        return iterator.hasNext() ? iterator.next() : null;
    }
}
