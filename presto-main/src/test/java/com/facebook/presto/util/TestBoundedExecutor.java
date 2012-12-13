package com.facebook.presto.util;

import com.google.common.base.Throwables;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestBoundedExecutor
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
        BoundedExecutor boundedExecutor = new BoundedExecutor(enqueuingExecutor, 1);

        // Launch two tasks
        boundedExecutor.execute(countTask);
        boundedExecutor.execute(countTask);

        Assert.assertEquals(count.get(), 0);
        Assert.assertEquals(enqueuingExecutor.size(), 2);

        // Run both tasks
        enqueuingExecutor.runAll();

        Assert.assertEquals(count.get(), 2);
        Assert.assertTrue(enqueuingExecutor.isEmpty());

        // Launch another task
        boundedExecutor.execute(countTask);

        Assert.assertEquals(count.get(), 2);
        Assert.assertEquals(enqueuingExecutor.size(), 1);

        // Run single task
        enqueuingExecutor.runNext();

        Assert.assertEquals(count.get(), 3);
        Assert.assertTrue(enqueuingExecutor.isEmpty());
    }

    @Test
    public void testTaskMerge()
            throws Exception
    {
        final AtomicInteger count1 = new AtomicInteger(0);
        final CountDownLatch mainAwaitLatch = new CountDownLatch(1);
        final CountDownLatch continuationLatch = new CountDownLatch(1);
        final CountDownLatch finishedLatch = new CountDownLatch(2);
        Runnable countTask1 = new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    mainAwaitLatch.countDown();
                    continuationLatch.await();
                    count1.incrementAndGet();
                    finishedLatch.countDown();
                }
                catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
            }
        };

        final AtomicInteger count2 = new AtomicInteger(0);
        Runnable countTask2 = new Runnable()
        {
            @Override
            public void run()
            {
                count2.incrementAndGet();
            }
        };

        EnqueuingExecutor enqueuingExecutor = new EnqueuingExecutor();

        BoundedExecutor boundedExecutor1 = new BoundedExecutor(enqueuingExecutor, 1);
        BoundedExecutor boundedExecutor2 = new BoundedExecutor(enqueuingExecutor, 1);

        // Submit Task1, Task1, Task2
        boundedExecutor1.execute(countTask1);
        boundedExecutor1.execute(countTask1);
        boundedExecutor2.execute(countTask2);

        // Run Task1 in another thread (it should block)
        runInThread(enqueuingExecutor.removeNext());
        mainAwaitLatch.await(); // Wait for thread to block
        Assert.assertEquals(count1.get(), 0);
        Assert.assertEquals(count2.get(), 0);

        // Run next task (another Task1) which should get merged with existing Task1
        enqueuingExecutor.runNext();
        Assert.assertEquals(count1.get(), 0);
        Assert.assertEquals(count2.get(), 0);

        // Run the last task (Task2) which should run straight to completion without waiting for Task1
        enqueuingExecutor.runNext();
        Assert.assertEquals(count1.get(), 0);
        Assert.assertEquals(count2.get(), 1);

        // Unblock all Task1s, both should run to completion
        continuationLatch.countDown();
        finishedLatch.await();
        Assert.assertEquals(count1.get(), 2);
        Assert.assertEquals(count2.get(), 1);
    }

    @Test
    public void testConcurrency()
            throws Exception
    {
        final AtomicInteger count = new AtomicInteger(0);
        Runnable countTaskBase = new Runnable()
        {
            @Override
            public void run()
            {
                count.incrementAndGet();
            }
        };

        // Use ParallismChecker to make sure expected parallelism is not exceeded
        Runnable countTask1 = new ParallelismChecker(1, countTaskBase);
        Runnable countTask2 = new ParallelismChecker(2, countTaskBase);
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        BoundedExecutor boundedExecutor1 = new BoundedExecutor(executorService, 1);
        BoundedExecutor boundedExecutor2 = new BoundedExecutor(executorService, 2);

        for (int i = 0; i < 100000; i++) {
            boundedExecutor1.execute(countTask1);
            boundedExecutor2.execute(countTask2);
        }

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        Assert.assertEquals(count.get(), 200000);
    }

    @Test
    public void testExceptionThrowingTask()
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
        BoundedExecutor boundedExecutor = new BoundedExecutor(enqueuingExecutor, 1);

        // Make sure that BoundedExecutor still works after a task throws an Exception
        boundedExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                throw new RuntimeException();
            }
        });
        enqueuingExecutor.runNext();

        Assert.assertTrue(enqueuingExecutor.isEmpty());

        boundedExecutor.execute(countTask);
        enqueuingExecutor.runNext();

        Assert.assertEquals(count.get(), 1);
        Assert.assertTrue(enqueuingExecutor.isEmpty());
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
