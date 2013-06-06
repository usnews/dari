package com.psddev.dari.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

public class LazyTest {

    @Test
    public void testGet() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<Thread>();

        for (int i = 0; i < 200; ++ i) {
            threads.add(new Worker(latch));
        }

        for (Thread t : threads) {
            t.start();
        }

        latch.countDown();

        for (Thread t : threads) {
            t.join();
        }

        Assert.assertEquals(1, Foo.getInstanceCount());
    }

    public static class Foo {

        private static final Lazy<Foo> INSTANCE = new Lazy<Foo>() {
            @Override
            protected Foo create() {
                return new Foo();
            }
        };

        private static final AtomicLong INSTANCE_COUNT = new AtomicLong();

        public Foo() {
            INSTANCE_COUNT.incrementAndGet();
        }

        public static Foo getInstance() {
            return INSTANCE.get();
        }

        public static long getInstanceCount() {
            return INSTANCE_COUNT.get();
        }
    }

    public static class Worker extends Thread {

        private final CountDownLatch latch;

        public Worker(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();

            } catch (InterruptedException error) {
                ErrorUtils.rethrow(error);
            }

            Foo.getInstance();
        }
    }
}
