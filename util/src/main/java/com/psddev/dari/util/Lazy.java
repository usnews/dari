package com.psddev.dari.util;

/**
 * For lazily creating a value safely and efficiently.
 *
 * <p>Typical usage looks like:</p>
 *
 * <p><blockquote><code>
 * public class Foo {
 *     private static final Lazy&lt;Foo&gt; INSTANCE = new Lazy&lt;Foo&gt;() {
 *         @Override
 *         protected Foo create() {
 *             return new Foo();
 *         }
 *     }
 *
 *     public static Foo getInstance() {
 *         return INSTANCE.get();
 *     }
 * }
 * </code></blockquote></p>
 */
public abstract class Lazy<T> {

    private T value;

    private final Once createOnce = new Once() {
        @Override
        protected void run() throws Exception {
            value = create();
        }
    };

    /**
     * Creates and returns the value.
     */
    protected abstract T create() throws Exception;

    /**
     * Returns the value, creating it using {@link #create} if necessary.
     */
    public final T get() {
        createOnce.ensure();
        return value;
    }

    /**
     * Resets so that the next invocation of {@link #get} can call
     * {@link #create} again.
     */
    public final void reset() {
        createOnce.reset();
    }
}
