package com.psddev.dari.util;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background task that produces items into an {@linkplain #getOutput
 * output queue}.
 */
public abstract class AsyncProducer<E> extends Task {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncProducer.class);

    private final AsyncQueue<E> output;

    private long produceCount;
    private long produceDuration;
    private long errorCount;

    /**
     * Creates an instance that runs in the given {@code executor} and
     * produces items into the given {@code output} queue.
     *
     * @param executor If {@code null}, uses the default executor.
     * @param output If {@code null}, creates a new queue.
     */
    public AsyncProducer(String executor, AsyncQueue<E> output) {
        super(executor, null);
        this.output = output == null ? new AsyncQueue<E>() : output;
        this.output.addProducer(this);
    }

    /** @deprecated Use {@link #AsyncProducer(String, AsyncQueue)} instead. */
    @Deprecated
    public AsyncProducer(AsyncQueue<E> output) {
        this(null, output);
    }

    /** @deprecated Use {@link #AsyncProducer(String, AsyncQueue)} instead. */
    @Deprecated
    public AsyncProducer() {
        this(null, null);
    }

    /** Returns the output queue. */
    public final AsyncQueue<E> getOutput() {
        return output;
    }

    public long getProduceCount() {
        return produceCount;
    }

    public long getProduceDuration() {
        return produceDuration;
    }

    public long getErrorCount() {
        return errorCount;
    }

    /**
     * Called before production starts. Default implementation doesn't
     * do anything. Sub-classes must call {@code super.beforeStart}
     * at the beginning of this method.
     */
    protected void beforeStart() {
    }

    /**
     * Called to produce an item.
     *
     * @return If {@code null}, it won't be added, and this task will end.
     */
    protected abstract E produce() throws Exception;

    /**
     * Called to handle the given {@code error} that occurred during
     * {@link #produce}. Default implementation logs the error at the
     * {@code WARNING} level.
     */
    protected void handleError(Exception error) {
        LOGGER.warn("Failed to produce!", error);
    }

    /**
     * Called when all production have finished. Default implementation
     * doesn't do anything. Sub-classes must call {@code super.finished}
     * at the beginning of this method.
     */
    protected void finished() {
    }

    // --- Task support ---

    @Override
    protected void doTask() {
        try {
            beforeStart();

            try {
                for (E item;
                        shouldContinue() && !output.isClosed();
                        addProgressIndex(1)) {

                    try {
                        long startTime = System.nanoTime();
                        try {
                            item = produce();
                            ++ produceCount;
                        } finally {
                            produceDuration += System.nanoTime() - startTime;
                        }

                        if (item != null) {
                            output.add(item);
                        } else {
                            break;
                        }

                    } catch (Exception ex) {
                        ++ errorCount;
                        handleError(ex);
                    }
                }

            } finally {
                finished();
            }

        } finally {
            output.removeProducer(this);
        }
    }

    /** {@link AsyncProducer} utility methods. */
    public static final class Static {

        /**
         * Creates an {@link AsyncProducer} instance that runs in the
         * given {@code executor} and produces items from the given
         * {@code iterable} into the given {@code queue}.
         */
        public static <T> AsyncProducer<T> inExecutorFromIterableIntoQueue(String executor, Iterable<T> iterable, AsyncQueue<T> queue) {
            final Iterator<T> iterator = iterable.iterator();

            return new AsyncProducer<T>(executor, queue) {

                @Override
                protected T produce() {
                    return iterator.hasNext() ? iterator.next() : null;
                }
            };
        }

        /**
         * @deprecated Use {@link #inExecutorFromIterableIntoQueue} instead.
         */
        @Deprecated
        public static <T> AsyncProducer<T> inExecutorFromIterable(String executor, Iterable<T> iterable) {
            return inExecutorFromIterableIntoQueue(executor, iterable, null);
        }

        /**
         * Creates an {@link AsyncProducer} instance that runs in the
         * default executor and produces items from the given
         * {@code iterable}.
         */
        public static <T> AsyncProducer<T> fromIterable(Iterable<T> iterable) {
            return inExecutorFromIterableIntoQueue(null, iterable, null);
        }

        /** @deprecated Use {@link #fromIterable} instead. */
        @Deprecated
        public static <T> AsyncProducer<T> createWithIterable(Iterable<T> iterable) {
            return fromIterable(iterable);
        }
    }
}
