package com.psddev.dari.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OnceTest {

    private Map<Once, Once> results;
    private CountDownLatch latch;
    private List<Thread> threads;

    @Before
    public void before() {
        results = new ConcurrentHashMap<Once, Once>();
        latch = new CountDownLatch(1);
        threads = new ArrayList<Thread>();
    }

    @After
    public void after() {
        results = null;
        latch = null;
        threads = null;
    }

    @Test
    public void testEnsure() throws InterruptedException {
        testOnce(new Once() {
            @Override
            protected void run() throws InterruptedException {
                Thread.sleep(1000);
                results.put(this, this);
            }
        });
    }

    @Test
    public void testReentrancy() throws InterruptedException {
        testOnce(new Once() {
            @Override
            protected void run() throws InterruptedException {
                ensure();
                results.put(this, this);
            }
        });
    }

    public void testOnce(Once once) throws InterruptedException {
        for (int i = 0; i < 200; ++ i) {
            threads.add(new Worker(latch, once));
        }

        for (Thread t : threads) {
            t.start();
        }

        latch.countDown();

        for (Thread t : threads) {
            t.join();
        }

        Assert.assertEquals(1, results.size());
    }

    public static class Worker extends Thread {

        private final CountDownLatch latch;
        private final Once once;

        public Worker(CountDownLatch latch, Once once) {
            this.latch = latch;
            this.once = once;
        }

        @Override
        public void run() {
            try {
                latch.await();

            } catch (InterruptedException error) {
                ErrorUtils.rethrow(error);
            }

            once.ensure();
        }
    }
}
