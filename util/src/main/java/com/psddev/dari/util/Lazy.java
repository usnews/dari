package com.psddev.dari.util;

/**
 * For lazily creating a value safely and efficiently.
 *
 * <p>Typical usage looks like:</p>
 *
 * <p><blockquote><code>
 * public class Foo {
 *     private static final Lazy&lt;Foo&gt; INSTANCE = new Lazy&lt;Foo&gt;() {
 *         {@literal @}Override
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
    private volatile boolean created;

    private final Once createOnce = new Once() {

        @Override
        protected void run() throws Exception {
            value = create();
            created = true;
        }
    };

    /**
     * Creates and returns the value.
     *
     * @return May be {@code null}.
     */
    protected abstract T create() throws Exception;

    /**
     * Returns the value, creating it using {@link #create} if necessary.
     *
     * @return May be {@code null} if {@link #create} returns {@code null}.
     */
    public final T get() {
        createOnce.ensure();
        return value;
    }

    /**
     * Runs any additional logic to destroy the value if necessary.
     * The default implementation of this method does nothing.
     *
     * @param value May be {@code null} if {@link #create} returned
     * {@code null}.
     */
    protected void destroy(T value) {
    }

    /**
     * Resets so that the next invocation of {@link #get} can call
     * {@link #create} again.
     */
    public final void reset() {
        try {
            if (created) {
                destroy(value);
            }

        } finally {
            created = false;
            createOnce.reset();
        }
    }
}
