package com.psddev.dari.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background task that consumes all items from an {@linkplain #getInput
 * input queue}.
 *
 * <p>Sub-classes must implement:
 *
 * <ul>
 * <li>{@link #consume}
 *
 * <p>Optionally, they can futher override:
 *
 * <ul>
 * <li>{@link #beforeStart}
 * <li>{@link #handleError}
 * <li>{@link #finished}
 */
public abstract class AsyncConsumer<E> extends Task {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncConsumer.class);

    private final AsyncQueue<E> input;

    private long consumeCount;
    private long consumeDuration;
    private long errorCount;

    /**
     * Creates an instance that runs in the given {@code executor} and
     * consumes items in the given {@code input} queue.
     *
     * @param executor If {@code null}, uses the default executor.
     * @param input Can't be {@code null}.
     * @throws IllegalArgumentException If the given {@code input}
     *         is {@code null}.
     */
    public AsyncConsumer(String executor, AsyncQueue<E> input) {
        super(executor, null);
        if (input == null) {
            throw new IllegalArgumentException("Input can't be null!");
        }
        this.input = input;
    }

    /** @deprecated Use {@link #AsyncConsumer(String, AsyncQueue)} instead. */
    public AsyncConsumer(AsyncQueue<E> input) {
        this(null, input);
    }

    /** Returns the input queue. */
    public final AsyncQueue<E> getInput() {
        return input;
    }

    public long getConsumeCount() {
        return consumeCount;
    }

    public long getConsumeDuration() {
        return consumeDuration;
    }

    public long getErrorCount() {
        return errorCount;
    }

    /**
     * Called before processing starts. Default implementation doesn't
     * do anything. Sub-classes must call {@code super.beforeStart}
     * at the beginning of this method.
     */
    protected void beforeStart() {
    }

    /** Called to consume the given {@code item}. */
    protected abstract void consume(E item) throws Exception;

    /**
     * Called to handle the given {@code error} that occurred during
     * {@link #consume}. Default implementation logs the error at the
     * {@code WARNING} level.
     */
    protected void handleError(E item, Exception error) {
        LOGGER.warn(String.format("Failed to consume [%s]!", item), error);
    }

    /**
     * Called when all consumption have finished. Default implementation
     * doesn't do anything. Sub-classes must call {@code super.finished}
     * at the beginning of this method.
     */
    protected void finished() {
    }

    // --- Task support ---

    @Override
    protected final void doTask() {
        beforeStart();

        try {
            for (E item;
                    shouldContinue() && (item = input.remove()) != null;
                    addProgressIndex(1)) {

                try {
                    long startTime = System.nanoTime();
                    try {
                        consume(item);
                        ++ consumeCount;
                    } finally {
                        consumeDuration += System.nanoTime() - startTime;
                    }

                } catch (Exception ex) {
                    ++ errorCount;
                    handleError(item, ex);
                }
            }

        } finally {
            finished();
        }
    }
}
