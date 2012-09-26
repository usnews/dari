package com.psddev.dari.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background task that consumes all items from an {@linkplain #getInput
 * input queue}, processes them, and produces new ones into an {@linkplain
 * #getOutput output queue}.
 *
 * <p>Sub-classes must implement:
 *
 * <ul>
 * <li>{@link #process}
 *
 * <p>Optionally, they can further override:
 *
 * <ul>
 * <li>{@link #beforeStart}
 * <li>{@link #handleError}
 * <li>{@link #finished}
 */
public abstract class AsyncProcessor<S, D> extends AsyncConsumer<S> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncProducer.class);

    private final AsyncQueue<D> output;

    /**
     * Creates an instance that runs in the given {@code executor} with
     * the given {@code input} and {@code output} queues.
     *
     * @param executor If {@code null}, uses the default executor.
     * @param input Can't be {@code null}.
     * @param output If {@code null}, creates a new queue.
     * @throws IllegalArgumentException If the given {@code input}
     *         is {@code null}.
     */
    public AsyncProcessor(String executor, AsyncQueue<S> input, AsyncQueue<D> output) {
        super(executor, input);
        this.output = output == null ? new AsyncQueue<D>() : output;
        this.output.addProducer(this);
    }

    /** @deprecated Use {@link #AsyncProcessor(String, AsyncQueue, AsyncQueue)} instead. */
    @Deprecated
    public AsyncProcessor(AsyncQueue<S> input, AsyncQueue<D> output) {
        this(null, input, output);
    }

    /** @deprecated Use {@link #AsyncProcessor(String, AsyncQueue, AsyncQueue)} instead. */
    @Deprecated
    public AsyncProcessor(AsyncQueue<S> input) {
        this(null, input, null);
    }

    /** Returns the output queue. */
    public final AsyncQueue<D> getOutput() {
        return output;
    }

    /** Called to process the given {@code item}. */
    protected abstract D process(S item) throws Exception;

    // --- AsyncConsumer support ---

    @Override
    protected final void consume(S item) throws Exception {
        output.add(process(item));
    }

    @Override
    protected void handleError(S item, Exception error) {
        LOGGER.warn(String.format("Failed to process [%s]!", item), error);
    }

    @Override
    protected void finished() {
        try {
            super.finished();
        } finally {
            this.output.removeProducer(this);
        }
    }
}
