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
package com.facebook.presto.util;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertLessThanOrEqual;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestAsyncSemaphore
{
    private final ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("async-semaphore-%s")));

    @Test
    public void testInlineExecution()
            throws Exception
    {
        AsyncSemaphore<Runnable> asyncSemaphore = new AsyncSemaphore<>(1, executor, new Function<Runnable, ListenableFuture<?>>()
        {
            @Override
            public ListenableFuture<?> apply(Runnable task)
            {
                return sameThreadExecutor().submit(task);
            }
        });

        final AtomicInteger count = new AtomicInteger();

        List<ListenableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            futures.add(asyncSemaphore.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    count.incrementAndGet();
                }
            }));
        }

        // Wait for completion
        Futures.allAsList(futures).get(1, TimeUnit.MINUTES);

        Assert.assertEquals(count.get(), 1000);
    }

    @Test
    public void testSingleThreadBoundedConcurrency()
            throws Exception
    {
        AsyncSemaphore<Runnable> asyncSemaphore = new AsyncSemaphore<>(1, executor, new Function<Runnable, ListenableFuture<?>>()
        {
            @Override
            public ListenableFuture<?> apply(Runnable task)
            {
                return executor.submit(task);
            }
        });

        final AtomicInteger count = new AtomicInteger();
        final AtomicInteger concurrency = new AtomicInteger();

        List<ListenableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            futures.add(asyncSemaphore.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    count.incrementAndGet();
                    int currentConcurrency = concurrency.incrementAndGet();
                    assertLessThanOrEqual(currentConcurrency, 1);
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                    concurrency.decrementAndGet();
                }
            }));
        }

        // Wait for completion
        Futures.allAsList(futures).get(1, TimeUnit.MINUTES);

        Assert.assertEquals(count.get(), 1000);
    }

    @Test
    public void testMultiThreadBoundedConcurrency()
            throws Exception
    {
        AsyncSemaphore<Runnable> asyncSemaphore = new AsyncSemaphore<>(2, executor, new Function<Runnable, ListenableFuture<?>>()
        {
            @Override
            public ListenableFuture<?> apply(Runnable task)
            {
                return executor.submit(task);
            }
        });

        final AtomicInteger count = new AtomicInteger();
        final AtomicInteger concurrency = new AtomicInteger();

        List<ListenableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            futures.add(asyncSemaphore.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    count.incrementAndGet();
                    int currentConcurrency = concurrency.incrementAndGet();
                    assertLessThanOrEqual(currentConcurrency, 2);
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                    concurrency.decrementAndGet();
                }
            }));
        }

        // Wait for completion
        Futures.allAsList(futures).get(1, TimeUnit.MINUTES);

        Assert.assertEquals(count.get(), 1000);
    }

    @Test
    public void testMultiSubmitters()
            throws Exception
    {
        final AsyncSemaphore<Runnable> asyncSemaphore = new AsyncSemaphore<>(2, executor, new Function<Runnable, ListenableFuture<?>>()
        {
            @Override
            public ListenableFuture<?> apply(Runnable task)
            {
                return executor.submit(task);
            }
        });

        final AtomicInteger count = new AtomicInteger();
        final AtomicInteger concurrency = new AtomicInteger();

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(100);
        for (int i = 0; i < 100; i++) {
            executor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    Uninterruptibles.awaitUninterruptibly(startLatch, 1, TimeUnit.MINUTES);
                    asyncSemaphore.submit(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            count.incrementAndGet();
                            int currentConcurrency = concurrency.incrementAndGet();
                            assertLessThanOrEqual(currentConcurrency, 2);
                            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                            concurrency.decrementAndGet();
                            completionLatch.countDown();
                        }
                    });
                }
            });
        }
        // Start the submitters;
        startLatch.countDown();

        // Wait for completion
        Uninterruptibles.awaitUninterruptibly(completionLatch, 1, TimeUnit.MINUTES);

        Assert.assertEquals(count.get(), 100);
    }

    @Test
    public void testFailedTasks()
            throws Exception
    {
        AsyncSemaphore<Runnable> asyncSemaphore = new AsyncSemaphore<>(2, executor, new Function<Runnable, ListenableFuture<?>>()
        {
            @Override
            public ListenableFuture<?> apply(Runnable task)
            {
                return executor.submit(task);
            }
        });

        final AtomicInteger successCount = new AtomicInteger();
        final AtomicInteger failureCount = new AtomicInteger();
        final AtomicInteger concurrency = new AtomicInteger();
        final CountDownLatch completionLatch = new CountDownLatch(1000);

        List<ListenableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            ListenableFuture<?> future = asyncSemaphore.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    int currentConcurrency = concurrency.incrementAndGet();
                    assertLessThanOrEqual(currentConcurrency, 2);
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                    concurrency.decrementAndGet();
                    throw new IllegalStateException();
                }
            });
            Futures.addCallback(future, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(@Nullable Object result)
                {
                    successCount.incrementAndGet();
                    completionLatch.countDown();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    failureCount.incrementAndGet();
                    completionLatch.countDown();
                }
            });
            futures.add(future);
        }

        // Wait for all tasks and callbacks to complete
        completionLatch.await(1, TimeUnit.MINUTES);

        for (ListenableFuture<?> future : futures) {
            try {
                future.get();
                Assert.fail();
            }
            catch (Exception ignored) {
            }
        }

        Assert.assertEquals(successCount.get(), 0);
        Assert.assertEquals(failureCount.get(), 1000);
    }

    @Test
    public void testFailedTaskSubmission()
            throws Exception
    {
        final AtomicInteger successCount = new AtomicInteger();
        final AtomicInteger failureCount = new AtomicInteger();
        final AtomicInteger concurrency = new AtomicInteger();
        final CountDownLatch completionLatch = new CountDownLatch(1000);

        AsyncSemaphore<Runnable> asyncSemaphore = new AsyncSemaphore<>(2, executor, new Function<Runnable, ListenableFuture<?>>()
        {
            @Override
            public ListenableFuture<?> apply(Runnable task)
            {
                int currentConcurrency = concurrency.incrementAndGet();
                assertLessThanOrEqual(currentConcurrency, 2);
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                concurrency.decrementAndGet();
                throw new IllegalStateException();
            }
        });

        List<ListenableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            ListenableFuture<?> future = asyncSemaphore.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    Assert.fail(); // Should never execute this
                }
            });
            Futures.addCallback(future, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(@Nullable Object result)
                {
                    successCount.incrementAndGet();
                    completionLatch.countDown();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    failureCount.incrementAndGet();
                    completionLatch.countDown();
                }
            });
            futures.add(future);
        }

        // Wait for all tasks and callbacks to complete
        completionLatch.await(1, TimeUnit.MINUTES);

        for (ListenableFuture<?> future : futures) {
            try {
                future.get();
                Assert.fail();
            }
            catch (Exception ignored) {
            }
        }

        Assert.assertEquals(successCount.get(), 0);
        Assert.assertEquals(failureCount.get(), 1000);
    }

    @Test
    public void testFailedTaskWithMultipleSubmitters()
            throws Exception
    {
        final AtomicInteger successCount = new AtomicInteger();
        final AtomicInteger failureCount = new AtomicInteger();
        final AtomicInteger concurrency = new AtomicInteger();
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(100);

        final AsyncSemaphore<Runnable> asyncSemaphore = new AsyncSemaphore<>(2, executor, new Function<Runnable, ListenableFuture<?>>()
        {
            @Override
            public ListenableFuture<?> apply(Runnable task)
            {
                int currentConcurrency = concurrency.incrementAndGet();
                assertLessThanOrEqual(currentConcurrency, 2);
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                concurrency.decrementAndGet();
                throw new IllegalStateException();
            }
        });

        final Queue<ListenableFuture<?>> futures = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < 100; i++) {
            executor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    Uninterruptibles.awaitUninterruptibly(startLatch, 1, TimeUnit.MINUTES);
                    ListenableFuture<?> future = asyncSemaphore.submit(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            Assert.fail(); // Should never execute this
                        }
                    });
                    futures.add(future);
                    Futures.addCallback(future, new FutureCallback<Object>()
                    {
                        @Override
                        public void onSuccess(@Nullable Object result)
                        {
                            successCount.incrementAndGet();
                            completionLatch.countDown();
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            failureCount.incrementAndGet();
                            completionLatch.countDown();
                        }
                    });
                }
            });
        }
        // Start the submitters;
        startLatch.countDown();

        // Wait for completion
        Uninterruptibles.awaitUninterruptibly(completionLatch, 1, TimeUnit.MINUTES);

        // Make sure they all report failure
        for (ListenableFuture<?> future : futures) {
            try {
                future.get();
                Assert.fail();
            }
            catch (Exception ignored) {
            }
        }

        Assert.assertEquals(successCount.get(), 0);
        Assert.assertEquals(failureCount.get(), 100);
    }

    @Test
    public void testNoStackOverflow()
            throws Exception
    {
        AsyncSemaphore<Object> asyncSemaphore = new AsyncSemaphore<>(1, executor, new Function<Object, ListenableFuture<?>>()
        {
            @Override
            public ListenableFuture<?> apply(Object object)
            {
                return Futures.immediateFuture(null);
            }
        });

        List<ListenableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            futures.add(asyncSemaphore.submit(new Object()));
        }

        // Wait for completion
        Futures.allAsList(futures).get(1, TimeUnit.MINUTES);
    }
}
