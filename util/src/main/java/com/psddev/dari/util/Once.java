package com.psddev.dari.util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * For making sure that something runs only once safely and efficiently.
 *
 * <p>Typical usage looks like:</p>
 *
 * <p><blockquote><code>
 * public class Foo {
 *     private static final Once INIT_ONCE = new Once() {
 *         {@literal @}Override
 *         protected void run() {
 *             // Do something.
 *         }
 *     }
 *
 *     private void bar() {
 *         INIT_ONCE.ensure();
 *         // Do something else that depends on the init.
 *     }
 * }
 * </code></blockquote></p>
 */
public abstract class Once {

    private final Lock readLock;
    private final Lock writeLock;
    private Thread running;
    private boolean ran;

    /**
     * Creates an instance based on the given {@code lock}.
     *
     * @param lock Can't be {@code null}.
     */
    public Once(ReadWriteLock lock) {
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

    /**
     * Creates an instance based on {@link ReentrantReadWriteLock}.
     */
    public Once() {
        this(new ReentrantReadWriteLock());
    }

    /**
     * Runs some code.
     */
    protected abstract void run() throws Exception;

    /**
     * Ensures that {@link #run} has been called at least once.
     */
    public final void ensure() {
        readLock.lock();

        try {
            if (ran ||
                    Thread.currentThread().equals(running)) {
                return;
            }

            readLock.unlock();

            try {
                writeLock.lock();

                try {
                    if (!ran) {
                        try {
                            running = Thread.currentThread();
                            run();
                            ran = true;

                        } catch (Exception error) {
                            ErrorUtils.rethrow(error);

                        } finally {
                            running = null;
                        }
                    }

                } finally {
                    writeLock.unlock();
                }

            } finally {
                readLock.lock();
            }

        } finally {
            readLock.unlock();
        }
    }

    /**
     * Resets so that the next invocation of {@link #ensure} can call
     * {@link #run} again.
     */
    public final void reset() {
        writeLock.lock();

        try {
            ran = false;

        } finally {
            writeLock.unlock();
        }
    }
}
