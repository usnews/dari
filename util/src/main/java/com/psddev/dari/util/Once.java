package com.psddev.dari.util;

import java.util.concurrent.locks.ReadWriteLock;

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

    private volatile Thread running;
    private volatile boolean ran;

    /**
     * Creates an instance.
     *
     * @param lock Not used.
     * @deprecated Use {@link Once()} instead.
     */
    @Deprecated
    public Once(ReadWriteLock lock) {
    }

    /**
     * Creates an instance.
     */
    public Once() {
    }

    /**
     * Runs some code.
     */
    protected abstract void run() throws Exception;

    /**
     * Ensures that {@link #run} has been called at least once.
     */
    public final void ensure() {
        if (Thread.currentThread().equals(running) || ran) {
            return;
        }

        synchronized (this) {
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
        }
    }

    /**
     * Resets so that the next invocation of {@link #ensure} can call
     * {@link #run} again.
     */
    public final void reset() {
        ran = false;
    }
}
