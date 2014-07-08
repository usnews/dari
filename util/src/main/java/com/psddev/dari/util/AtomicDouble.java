package com.psddev.dari.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@code double} value that may be updated atomically.
 * This implementation uses {@link AtomicLong} internally.
 *
 * @deprecated Use {@link com.google.common.util.concurrent.AtomicDouble} instead.
 */
@Deprecated
@SuppressWarnings("serial")
public class AtomicDouble extends Number {

    private final AtomicLong value;

    /** Creates an instance with the given {@code initialValue}. */
    public AtomicDouble(double initialValue) {
        this.value = new AtomicLong(Double.doubleToLongBits(initialValue));
    }

    /** Creates an instance with the initial value of {@code 0.0}. */
    public AtomicDouble() {
        this(0.0);
    }

    /** Atomically adds the given {@code delta} to the current value. */
    public double addAndGet(double delta) {
        while (true) {
            long currentLong = value.get();
            double newDouble = Double.longBitsToDouble(currentLong) + delta;
            if (value.compareAndSet(currentLong, Double.doubleToLongBits(newDouble))) {
                return newDouble;
            }
        }
    }

    /**
     * Atomically sets to the given {@code update} value if the current
     * value is equal to the given {@code expect} value.
     */
    public boolean compareAndSet(double expect, double update) {
        return value.compareAndSet(Double.doubleToLongBits(expect), Double.doubleToLongBits(update));
    }

    /** Returns the current value. */
    public double get() {
        return Double.longBitsToDouble(value.get());
    }

    /** Atomically adds the given {@code delta}. */
    public double getAndAdd(double delta) {
        while (true) {
            long currentLong = value.get();
            double currentDouble = Double.longBitsToDouble(currentLong);
            double newDouble = currentDouble + delta;
            if (value.compareAndSet(currentLong, Double.doubleToLongBits(newDouble))) {
                return currentDouble;
            }
        }
    }

    /**
     * Atomically sets the given {@code newValue} and returns the old value.
     */
    public double getAndSet(double newValue) {
        return Double.longBitsToDouble(value.getAndSet(Double.doubleToLongBits(newValue)));
    }

    /** Eventually sets to the given {@code newValue}. */
    public void lazySet(double newValue) {
        value.lazySet(Double.doubleToLongBits(newValue));
    }

    /** Sets to the given {@code newValue}. */
    public void set(double newValue) {
        value.set(Double.doubleToLongBits(newValue));
    }

    /**
     * Atomically sets to the given {@code update} value if the current
     * value is equal to the given {@code expect} value.
     */
    public boolean weakCompareAndSet(double expect, double update) {
        return value.weakCompareAndSet(Double.doubleToLongBits(expect), Double.doubleToLongBits(update));
    }

    // --- Number support ---

    @Override
    public double doubleValue() {
        return get();
    }

    @Override
    public float floatValue() {
        return (float) get();
    }

    @Override
    public int intValue() {
        return (int) get();
    }

    @Override
    public long longValue() {
        return (long) get();
    }

    // --- Object support ---

    @Override
    public String toString() {
        return value.toString();
    }
}
