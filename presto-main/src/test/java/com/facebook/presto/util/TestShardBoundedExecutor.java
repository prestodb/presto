package com.facebook.presto.util;

import com.google.common.base.Throwables;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestShardBoundedExecutor
{
    @Test
    public void testSanity()
            throws Exception
    {
        final AtomicInteger count = new AtomicInteger(0);
        Runnable countTask = new Runnable()
        {
            @Override
            public void run()
            {
                count.incrementAndGet();
            }
        };
        EnqueuingExecutor enqueuingExecutor = new EnqueuingExecutor();
        ShardBoundedExecutor<String> executor = new ShardBoundedExecutor<>(enqueuingExecutor);

        // Schedule three count tasks: A, A, B
        Assert.assertFalse(executor.isActive("a"));
        Assert.assertFalse(executor.isActive("b"));
        executor.execute("a", countTask);
        executor.execute("a", countTask);
        executor.execute("b", countTask);
        Assert.assertTrue(executor.isActive("a"));
        Assert.assertTrue(executor.isActive("b"));

        // Run them all
        enqueuingExecutor.runAll();
        Assert.assertEquals(count.get(), 3);
        Assert.assertFalse(executor.isActive("a"));
        Assert.assertFalse(executor.isActive("b"));

        // Schedule just another B count task
        executor.execute("b", countTask);
        Assert.assertTrue(executor.isActive("b"));

        // Run the B task
        enqueuingExecutor.runNext();
        Assert.assertEquals(count.get(), 4);
        Assert.assertFalse(executor.isActive("b"));
    }

    @Test
    public void testShardSerialization()
            throws Exception
    {
        final CountDownLatch finishedALatch = new CountDownLatch(2);
        final AtomicInteger countA = new AtomicInteger(0);
        Runnable countTaskA = new Runnable()
        {
            @Override
            public void run()
            {
                countA.incrementAndGet();
                finishedALatch.countDown();
            }
        };

        final CountDownLatch mainAwaitLatch = new CountDownLatch(1);
        final CountDownLatch triggerLatch = new CountDownLatch(1);
        Runnable countTaskABlocked = new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    mainAwaitLatch.countDown();
                    triggerLatch.await();
                    countA.incrementAndGet();
                    finishedALatch.countDown();
                }
                catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }

            }
        };

        final AtomicInteger countB = new AtomicInteger(0);
        Runnable countTaskB = new Runnable()
        {
            @Override
            public void run()
            {
                countB.incrementAndGet();
            }
        };

        EnqueuingExecutor enqueuingExecutor = new EnqueuingExecutor();
        ShardBoundedExecutor<String> executor = new ShardBoundedExecutor<>(enqueuingExecutor);

        // Launch blocking task A, and wait for it to run to the latch
        executor.execute("a", countTaskABlocked);
        runInThread(enqueuingExecutor.removeNext());
        mainAwaitLatch.await();
        Assert.assertTrue(executor.isActive("a"));
        Assert.assertEquals(countA.get(), 0);

        // Launch task B, and run it to completion (should not be affected by A)
        executor.execute("b", countTaskB);
        Assert.assertTrue(executor.isActive("b"));
        enqueuingExecutor.runNext();
        Assert.assertFalse(executor.isActive("b"));
        Assert.assertEquals(countB.get(), 1);

        // Launch non-blocking task A, and have it queued behind the existing blocking task A
        executor.execute("a", countTaskA);
        runInThread(enqueuingExecutor.removeNext());
        Assert.assertTrue(executor.isActive("a"));
        Assert.assertEquals(countA.get(), 0);

        // Now unblock A and let everything finish
        triggerLatch.countDown();
        finishedALatch.await();
        Assert.assertEquals(countA.get(), 2);
    }

    @Test
    public void testConcurrency()
            throws Exception
    {
        final AtomicInteger countA = new AtomicInteger(0);
        Runnable countATaskBase = new Runnable()
        {
            @Override
            public void run()
            {
                countA.incrementAndGet();
            }
        };

        final AtomicInteger countB = new AtomicInteger(0);
        Runnable countBTaskBase = new Runnable()
        {
            @Override
            public void run()
            {
                countB.incrementAndGet();
            }
        };

        // Use ParallismChecker to make sure expected parallelism is not exceeded
        Runnable countATask = new ParallelismChecker(1, countATaskBase);
        Runnable countBTask = new ParallelismChecker(1, countBTaskBase);
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        ShardBoundedExecutor<String> executor = new ShardBoundedExecutor<>(executorService);

        Assert.assertFalse(executor.isActive("a"));
        Assert.assertFalse(executor.isActive("b"));
        for (int i = 0; i < 100000; i++) {
            executor.execute("a", countATask);
            executor.execute("b", countBTask);
        }

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        Assert.assertFalse(executor.isActive("a"));
        Assert.assertFalse(executor.isActive("b"));

        Assert.assertEquals(countA.get(), 100000);
        Assert.assertEquals(countB.get(), 100000);
    }

    private void runInThread(final Runnable task)
    {
        new Thread() {
            @Override
            public void run()
            {
                task.run();
            }
        }.start();
    }
}
