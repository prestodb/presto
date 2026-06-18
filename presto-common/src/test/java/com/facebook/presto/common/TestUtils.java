/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.common;

import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

public class TestUtils
{
    @Test
    public void testCheckArgumentFailWithMessage()
    {
        try {
            Utils.checkArgument(false, "test %s", "test");
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "test test");
        }
    }

    @Test
    public void testCheckArgumentPassWithMessage()
    {
        try {
            Utils.checkArgument(true, "test %s", "test");
        }
        catch (IllegalArgumentException e) {
            fail();
        }
    }

    @Test
    public void testMemoizedSupplierThreadSafe()
            throws Throwable
    {
        Function<Supplier<Boolean>, Supplier<Boolean>> memoizer =
                supplier -> Utils.memoizedSupplier(supplier);
        testSupplierThreadSafe(memoizer);
    }

    /**
     * Vendored from Guava
     */
    private void testSupplierThreadSafe(Function<Supplier<Boolean>, Supplier<Boolean>> memoizer)
            throws Throwable
    {
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicReference<Throwable> thrown = new AtomicReference<>(null);
        final int numThreads = 3;
        final Thread[] threads = new Thread[numThreads];
        final long timeout = TimeUnit.SECONDS.toNanos(60);

        final Supplier<Boolean> supplier =
                new Supplier<Boolean>()
                {
                    boolean isWaiting(Thread thread)
                    {
                        switch (thread.getState()) {
                            case BLOCKED:
                            case WAITING:
                            case TIMED_WAITING:
                                return true;
                            default:
                                return false;
                        }
                    }

                    int waitingThreads()
                    {
                        int waitingThreads = 0;
                        for (Thread thread : threads) {
                            if (isWaiting(thread)) {
                                waitingThreads++;
                            }
                        }
                        return waitingThreads;
                    }

                    @Override
                    @SuppressWarnings("ThreadPriorityCheck") // doing our best to test for races
                    public Boolean get()
                    {
                        // Check that this method is called exactly once, by the first
                        // thread to synchronize.
                        long t0 = System.nanoTime();
                        while (waitingThreads() != numThreads - 1) {
                            if (System.nanoTime() - t0 > timeout) {
                                thrown.set(
                                        new TimeoutException(
                                                "timed out waiting for other threads to block"
                                                        + " synchronizing on supplier"));
                                break;
                            }
                            Thread.yield();
                        }
                        count.getAndIncrement();
                        return Boolean.TRUE;
                    }
                };

        final Supplier<Boolean> memoizedSupplier = memoizer.apply(supplier);

        for (int i = 0; i < numThreads; i++) {
            threads[i] =
                    new Thread()
                    {
                        @Override
                        public void run()
                        {
                            assertSame(Boolean.TRUE, memoizedSupplier.get());
                        }
                    };
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        if (thrown.get() != null) {
            throw thrown.get();
        }
        assertEquals(1, count.get());
    }
}
