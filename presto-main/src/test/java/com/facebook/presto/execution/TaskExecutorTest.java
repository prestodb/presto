package com.facebook.presto.execution;

import com.facebook.presto.execution.TaskExecutor.TaskHandle;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.execution.TaskExecutor.createTaskExecutor;
import static com.facebook.presto.util.Threads.threadsNamed;
import static org.testng.Assert.assertEquals;

public class TaskExecutorTest
{
    private ExecutorService executor = Executors.newCachedThreadPool(threadsNamed("task-processor-%d"));

    @Test(invocationCount = 100)
    public void test()
            throws Exception
    {
        TaskExecutor taskExecutor = createTaskExecutor(executor, 4);
        TaskHandle taskHandle = taskExecutor.addTask(new TaskId("test", "test", "test"));

        final Phaser beginPhase = new Phaser();
        beginPhase.register();
        final Phaser verificationComplete = new Phaser();
        verificationComplete.register();

        // add two jobs
        TestingJob driver1 = new TestingJob(beginPhase, verificationComplete, 10);
        ListenableFuture<?> future1 = taskExecutor.addSplit(taskHandle, driver1);
        TestingJob driver2 = new TestingJob(beginPhase, verificationComplete, 10);
        ListenableFuture<?> future2 = taskExecutor.addSplit(taskHandle, driver2);
        assertEquals(driver1.getCompletedPhases(), 0);
        assertEquals(driver2.getCompletedPhases(), 0);

        // verify worker have arrived but haven't processed yet
        beginPhase.arriveAndAwaitAdvance();
        assertEquals(driver1.getCompletedPhases(), 0);
        assertEquals(driver2.getCompletedPhases(), 0);
        verificationComplete.arriveAndAwaitAdvance();

        // advance one phase and verify
        beginPhase.arriveAndAwaitAdvance();
        assertEquals(driver1.getCompletedPhases(), 1);
        assertEquals(driver2.getCompletedPhases(), 1);

        verificationComplete.arriveAndAwaitAdvance();

        // add one more job
        TestingJob driver3 = new TestingJob(beginPhase, verificationComplete, 10);
        ListenableFuture<?> future3 = taskExecutor.addSplit(taskHandle, driver3);

        // advance one phase and verify
        beginPhase.arriveAndAwaitAdvance();
        assertEquals(driver1.getCompletedPhases(), 2);
        assertEquals(driver2.getCompletedPhases(), 2);
        assertEquals(driver3.getCompletedPhases(), 0);
        verificationComplete.arriveAndAwaitAdvance();

        // advance to the end of the first two task and verify
        beginPhase.arriveAndAwaitAdvance();
        for (int i = 0; i < 7; i++) {
            verificationComplete.arriveAndAwaitAdvance();
            beginPhase.arriveAndAwaitAdvance();
            assertEquals(beginPhase.getPhase(), verificationComplete.getPhase() + 1);
        }
        assertEquals(driver1.getCompletedPhases(), 10);
        assertEquals(driver2.getCompletedPhases(), 10);
        assertEquals(driver3.getCompletedPhases(), 8);
        future1.get(1, TimeUnit.SECONDS);
        future2.get(1, TimeUnit.SECONDS);
        verificationComplete.arriveAndAwaitAdvance();

        // advance two more times and verify
        beginPhase.arriveAndAwaitAdvance();
        verificationComplete.arriveAndAwaitAdvance();
        beginPhase.arriveAndAwaitAdvance();
        assertEquals(driver1.getCompletedPhases(), 10);
        assertEquals(driver2.getCompletedPhases(), 10);
        assertEquals(driver3.getCompletedPhases(), 10);
        future3.get(1, TimeUnit.SECONDS);
        verificationComplete.arriveAndAwaitAdvance();

        assertEquals(driver1.getFirstPhase(), 0);
        assertEquals(driver2.getFirstPhase(), 0);
        assertEquals(driver3.getFirstPhase(), 2);

        assertEquals(driver1.getLastPhase(), 10);
        assertEquals(driver2.getLastPhase(), 10);
        assertEquals(driver3.getLastPhase(), 12);
    }

    private static class TestingJob
            implements SplitRunner
    {
        private final Phaser awaitWorkers;
        private final Phaser awaitVerifiers;
        private final int requiredPhases;
        private final AtomicInteger completedPhases = new AtomicInteger();

        private final AtomicInteger firstPhase = new AtomicInteger(-1);
        private final AtomicInteger lastPhase = new AtomicInteger(-1);

        public TestingJob(Phaser awaitWorkers, Phaser awaitVerifiers, int requiredPhases)
        {
            this.awaitWorkers = awaitWorkers;
            this.awaitVerifiers = awaitVerifiers;
            this.requiredPhases = requiredPhases;
            awaitWorkers.register();
            awaitVerifiers.register();
        }

        private int getFirstPhase()
        {
            return firstPhase.get();
        }

        private int getLastPhase()
        {
            return lastPhase.get();
        }

        private int getCompletedPhases()
        {
            return completedPhases.get();
        }

        @Override
        public ListenableFuture<?> process()
                throws Exception
        {
            int phase = awaitWorkers.arriveAndAwaitAdvance();
            firstPhase.compareAndSet(-1, phase - 1);
            lastPhase.set(phase);
            awaitVerifiers.arriveAndAwaitAdvance();

            completedPhases.getAndIncrement();
            return Futures.immediateFuture(null);
        }

        @Override
        public boolean isFinished()
        {
            boolean isFinished = completedPhases.get() >= requiredPhases;
            if (isFinished) {
                awaitVerifiers.arriveAndDeregister();
                awaitWorkers.arriveAndDeregister();
            }
            return isFinished;
        }
    }
}
