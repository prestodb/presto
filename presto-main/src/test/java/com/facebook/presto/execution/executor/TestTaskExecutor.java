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
package com.facebook.presto.execution.executor;

import com.facebook.presto.execution.SplitRunner;
import com.facebook.presto.execution.TaskId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.execution.executor.MultilevelSplitQueue.LEVEL_CONTRIBUTION_CAP;
import static com.facebook.presto.execution.executor.MultilevelSplitQueue.LEVEL_THRESHOLD_SECONDS;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.testing.Assertions.assertLessThanOrEqual;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTaskExecutor
{
    @Test(invocationCount = 100)
    public void testTasksComplete()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(4, 8, ticker);
        taskExecutor.start();
        ticker.increment(20, MILLISECONDS);

        try {
            TaskId taskId = new TaskId("test", 0, 0);
            TaskHandle taskHandle = taskExecutor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS));

            Phaser beginPhase = new Phaser();
            beginPhase.register();
            Phaser verificationComplete = new Phaser();
            verificationComplete.register();

            // add two jobs
            TestingJob driver1 = new TestingJob(ticker, new Phaser(1), beginPhase, verificationComplete, 10, 0);
            ListenableFuture<?> future1 = getOnlyElement(taskExecutor.enqueueSplits(taskHandle, true, ImmutableList.of(driver1)));
            TestingJob driver2 = new TestingJob(ticker, new Phaser(1), beginPhase, verificationComplete, 10, 0);
            ListenableFuture<?> future2 = getOnlyElement(taskExecutor.enqueueSplits(taskHandle, true, ImmutableList.of(driver2)));
            assertEquals(driver1.getCompletedPhases(), 0);
            assertEquals(driver2.getCompletedPhases(), 0);

            // verify worker have arrived but haven't processed yet
            beginPhase.arriveAndAwaitAdvance();
            assertEquals(driver1.getCompletedPhases(), 0);
            assertEquals(driver2.getCompletedPhases(), 0);
            ticker.increment(10, MILLISECONDS);
            assertEquals(taskExecutor.getMaxActiveSplitTime(), 10);
            verificationComplete.arriveAndAwaitAdvance();

            // advance one phase and verify
            beginPhase.arriveAndAwaitAdvance();
            assertEquals(driver1.getCompletedPhases(), 1);
            assertEquals(driver2.getCompletedPhases(), 1);

            verificationComplete.arriveAndAwaitAdvance();

            // add one more job
            TestingJob driver3 = new TestingJob(ticker, new Phaser(1), beginPhase, verificationComplete, 10, 0);
            ListenableFuture<?> future3 = getOnlyElement(taskExecutor.enqueueSplits(taskHandle, false, ImmutableList.of(driver3)));

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
            future1.get(1, SECONDS);
            future2.get(1, SECONDS);
            verificationComplete.arriveAndAwaitAdvance();

            // advance two more times and verify
            beginPhase.arriveAndAwaitAdvance();
            verificationComplete.arriveAndAwaitAdvance();
            beginPhase.arriveAndAwaitAdvance();
            assertEquals(driver1.getCompletedPhases(), 10);
            assertEquals(driver2.getCompletedPhases(), 10);
            assertEquals(driver3.getCompletedPhases(), 10);
            future3.get(1, SECONDS);
            verificationComplete.arriveAndAwaitAdvance();

            assertEquals(driver1.getFirstPhase(), 0);
            assertEquals(driver2.getFirstPhase(), 0);
            assertEquals(driver3.getFirstPhase(), 2);

            assertEquals(driver1.getLastPhase(), 10);
            assertEquals(driver2.getLastPhase(), 10);
            assertEquals(driver3.getLastPhase(), 12);

            // no splits remaining
            ticker.increment(30, MILLISECONDS);
            assertEquals(taskExecutor.getMaxActiveSplitTime(), 0);
        }
        finally {
            taskExecutor.stop();
        }
    }

    @Test(invocationCount = 100)
    public void testQuantaFairness()
    {
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(1, 2, ticker);
        taskExecutor.start();
        ticker.increment(20, MILLISECONDS);

        try {
            TaskHandle shortQuantaTaskHandle = taskExecutor.addTask(new TaskId("shortQuanta", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS));
            TaskHandle longQuantaTaskHandle = taskExecutor.addTask(new TaskId("longQuanta", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS));

            Phaser globalPhaser = new Phaser();

            TestingJob shortQuantaDriver = new TestingJob(ticker, new Phaser(), new Phaser(), globalPhaser, 10, 10);
            TestingJob longQuantaDriver = new TestingJob(ticker, new Phaser(), new Phaser(), globalPhaser, 10, 20);

            taskExecutor.enqueueSplits(shortQuantaTaskHandle, true, ImmutableList.of(shortQuantaDriver));
            taskExecutor.enqueueSplits(longQuantaTaskHandle, true, ImmutableList.of(longQuantaDriver));

            for (int i = 0; i < 11; i++) {
                globalPhaser.arriveAndAwaitAdvance();
            }

            assertTrue(shortQuantaDriver.getCompletedPhases() >= 7 && shortQuantaDriver.getCompletedPhases() <= 8);
            assertTrue(longQuantaDriver.getCompletedPhases() >= 3 && longQuantaDriver.getCompletedPhases() <= 4);

            globalPhaser.arriveAndDeregister();
        }
        finally {
            taskExecutor.stop();
        }
    }

    @Test(invocationCount = 100)
    public void testLevelMovement()
    {
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(2, 2, ticker);
        taskExecutor.start();
        ticker.increment(20, MILLISECONDS);

        try {
            TaskHandle testTaskHandle = taskExecutor.addTask(new TaskId("test", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS));

            Phaser globalPhaser = new Phaser();
            globalPhaser.bulkRegister(3);

            int quantaTimeMills = 500;
            int phasesPerSecond = 1000 / quantaTimeMills;
            int totalPhases = LEVEL_THRESHOLD_SECONDS[LEVEL_THRESHOLD_SECONDS.length - 1] * phasesPerSecond;
            TestingJob driver1 = new TestingJob(ticker, globalPhaser, new Phaser(), new Phaser(), totalPhases, quantaTimeMills);
            TestingJob driver2 = new TestingJob(ticker, globalPhaser, new Phaser(), new Phaser(), totalPhases, quantaTimeMills);

            taskExecutor.enqueueSplits(testTaskHandle, true, ImmutableList.of(driver1, driver2));

            int completedPhases = 0;
            for (int i = 0; i < (LEVEL_THRESHOLD_SECONDS.length - 1); i++) {
                for (; (completedPhases / phasesPerSecond) < LEVEL_THRESHOLD_SECONDS[i + 1]; completedPhases++) {
                    globalPhaser.arriveAndAwaitAdvance();
                }

                assertEquals(testTaskHandle.getPriority().getLevel(), i + 1);
            }

            globalPhaser.arriveAndDeregister();
        }
        finally {
            taskExecutor.stop();
        }
    }

    @Test(invocationCount = 100)
    public void testNoInstantaneousFairness()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(1, 2, new MultilevelSplitQueue(true, 2), true, ticker);
        taskExecutor.start();
        ticker.increment(20, MILLISECONDS);

        try {
            for (int i = 2; i < (LEVEL_THRESHOLD_SECONDS.length - 1); i++) {
                int levelStartMillis = (int) SECONDS.toMillis(LEVEL_THRESHOLD_SECONDS[i]);
                int nextLevelStartMillis = (int) SECONDS.toMillis(LEVEL_THRESHOLD_SECONDS[i + 1]);

                TaskHandle longJob = taskExecutor.addTask(new TaskId("longTask", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS));
                TestingJob longSplit = new TestingJob(ticker, new Phaser(), new Phaser(), new Phaser(), (nextLevelStartMillis / 100) - 20, 100);

                taskExecutor.enqueueSplits(longJob, true, ImmutableList.of(longSplit));
                longSplit.getCompletedFuture().get();

                TaskHandle shortJob = taskExecutor.addTask(new TaskId("shortTask", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS));
                TestingJob shortSplit = new TestingJob(ticker, new Phaser(), new Phaser(), new Phaser(), (levelStartMillis / 100) + 1, 100);

                taskExecutor.enqueueSplits(shortJob, true, ImmutableList.of(shortSplit));
                shortSplit.getCompletedFuture().get();

                Phaser globalPhaser = new Phaser(2);
                TestingJob shortSplit1 = new TestingJob(ticker, globalPhaser, new Phaser(), new Phaser(), 20, 10);
                TestingJob shortSplit2 = new TestingJob(ticker, globalPhaser, new Phaser(), new Phaser(), 20, 10);
                TestingJob longSplit1 = new TestingJob(ticker, globalPhaser, new Phaser(), new Phaser(), 20, 10);
                TestingJob longSplit2 = new TestingJob(ticker, globalPhaser, new Phaser(), new Phaser(), 20, 10);

                taskExecutor.enqueueSplits(longJob, true, ImmutableList.of(longSplit1, longSplit2));
                taskExecutor.enqueueSplits(shortJob, true, ImmutableList.of(shortSplit1, shortSplit2));

                for (int j = 0; j < 10; j++) {
                    globalPhaser.arriveAndAwaitAdvance();
                }

                assertLessThanOrEqual(longSplit1.getCompletedPhases() + longSplit2.getCompletedPhases(), 2);
                assertGreaterThanOrEqual(shortSplit1.getCompletedPhases() + shortSplit2.getCompletedPhases(), 8);

                globalPhaser.arriveAndDeregister();
                longSplit1.getCompletedFuture().get();
                longSplit2.getCompletedFuture().get();
                shortSplit1.getCompletedFuture().get();
                shortSplit2.getCompletedFuture().get();
                longJob.destroy();
                shortJob.destroy();
            }
        }
        finally {
            taskExecutor.stop();
        }
    }

    @Test(invocationCount = 100)
    public void testLevelMultipliers()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(1, 3, new MultilevelSplitQueue(false, 2), false, ticker);
        taskExecutor.start();
        ticker.increment(20, MILLISECONDS);

        try {
            for (int i = 0; i < (LEVEL_THRESHOLD_SECONDS.length - 1); i++) {
                TaskHandle[] taskHandles = {
                        taskExecutor.addTask(new TaskId("test1", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS)),
                        taskExecutor.addTask(new TaskId("test2", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS)),
                        taskExecutor.addTask(new TaskId("test3", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS))
                };

                // move task 0 to next level
                TestingJob task0Job = new TestingJob(ticker, new Phaser(1), new Phaser(), new Phaser(), 1, LEVEL_THRESHOLD_SECONDS[i + 1] * 1000);
                taskExecutor.enqueueSplits(
                        taskHandles[0],
                        true,
                        ImmutableList.of(task0Job));
                // move tasks 1 and 2 to this level
                TestingJob task1Job = new TestingJob(ticker, new Phaser(1), new Phaser(), new Phaser(), 1, LEVEL_THRESHOLD_SECONDS[i] * 1000);
                taskExecutor.enqueueSplits(
                        taskHandles[1],
                        true,
                        ImmutableList.of(task1Job));
                TestingJob task2Job = new TestingJob(ticker, new Phaser(1), new Phaser(), new Phaser(), 1, LEVEL_THRESHOLD_SECONDS[i] * 1000);
                taskExecutor.enqueueSplits(
                        taskHandles[2],
                        true,
                        ImmutableList.of(task2Job));

                task0Job.getCompletedFuture().get();
                task1Job.getCompletedFuture().get();
                task2Job.getCompletedFuture().get();

                // then, start new drivers for all tasks
                Phaser globalPhaser = new Phaser(2);
                int phasesForNextLevel = LEVEL_THRESHOLD_SECONDS[i + 1] - LEVEL_THRESHOLD_SECONDS[i];
                TestingJob[] drivers = new TestingJob[6];
                for (int j = 0; j < 6; j++) {
                    drivers[j] = new TestingJob(ticker, globalPhaser, new Phaser(), new Phaser(), phasesForNextLevel, 1000);
                }

                taskExecutor.enqueueSplits(taskHandles[0], true, ImmutableList.of(drivers[0], drivers[1]));
                taskExecutor.enqueueSplits(taskHandles[1], true, ImmutableList.of(drivers[2], drivers[3]));
                taskExecutor.enqueueSplits(taskHandles[2], true, ImmutableList.of(drivers[4], drivers[5]));

                // run all three drivers
                int lowerLevelStart = drivers[2].getCompletedPhases() + drivers[3].getCompletedPhases() + drivers[4].getCompletedPhases() + drivers[5].getCompletedPhases();
                int higherLevelStart = drivers[0].getCompletedPhases() + drivers[1].getCompletedPhases();
                while (Arrays.stream(drivers).noneMatch(TestingJob::isFinished)) {
                    globalPhaser.arriveAndAwaitAdvance();

                    int lowerLevelEnd = drivers[2].getCompletedPhases() + drivers[3].getCompletedPhases() + drivers[4].getCompletedPhases() + drivers[5].getCompletedPhases();
                    int lowerLevelTime = lowerLevelEnd - lowerLevelStart;
                    int higherLevelEnd = drivers[0].getCompletedPhases() + drivers[1].getCompletedPhases();
                    int higherLevelTime = higherLevelEnd - higherLevelStart;

                    if (higherLevelTime > 20) {
                        assertGreaterThan(lowerLevelTime, (higherLevelTime * 2) - 10);
                        assertLessThan(higherLevelTime, (lowerLevelTime * 2) + 10);
                    }
                }

                try {
                    globalPhaser.arriveAndDeregister();
                }
                catch (IllegalStateException e) {
                    // under high concurrency sometimes the deregister call can occur after completion
                    // this is not a real problem
                }
                taskExecutor.removeTask(taskHandles[0]);
                taskExecutor.removeTask(taskHandles[1]);
                taskExecutor.removeTask(taskHandles[2]);
            }
        }
        finally {
            taskExecutor.stop();
        }
    }

    @Test
    public void testTaskHandle()
    {
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(4, 8, ticker);
        taskExecutor.start();

        try {
            TaskId taskId = new TaskId("test", 0, 0);
            TaskHandle taskHandle = taskExecutor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS));

            Phaser beginPhase = new Phaser();
            beginPhase.register();
            Phaser verificationComplete = new Phaser();
            verificationComplete.register();

            TestingJob driver1 = new TestingJob(ticker, new Phaser(), beginPhase, verificationComplete, 10, 0);
            TestingJob driver2 = new TestingJob(ticker, new Phaser(), beginPhase, verificationComplete, 10, 0);

            // force enqueue a split
            taskExecutor.enqueueSplits(taskHandle, true, ImmutableList.of(driver1));
            assertEquals(taskHandle.getRunningLeafSplits(), 0);

            // normal enqueue a split
            taskExecutor.enqueueSplits(taskHandle, false, ImmutableList.of(driver2));
            assertEquals(taskHandle.getRunningLeafSplits(), 1);

            // let the split continue to run
            beginPhase.arriveAndDeregister();
            verificationComplete.arriveAndDeregister();
        }
        finally {
            taskExecutor.stop();
        }
    }

    @Test
    public void testLevelContributionCap()
    {
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(false, 2);
        TaskHandle handle0 = new TaskHandle(new TaskId("test0", 0, 0), splitQueue, () -> 1, 1, new Duration(1, SECONDS));
        TaskHandle handle1 = new TaskHandle(new TaskId("test1", 0, 0), splitQueue, () -> 1, 1, new Duration(1, SECONDS));

        for (int i = 0; i < (LEVEL_THRESHOLD_SECONDS.length - 1); i++) {
            long levelAdvanceTime = SECONDS.toNanos(LEVEL_THRESHOLD_SECONDS[i + 1] - LEVEL_THRESHOLD_SECONDS[i]);
            handle0.addScheduledNanos(levelAdvanceTime);
            assertEquals(handle0.getPriority().getLevel(), i + 1);

            handle1.addScheduledNanos(levelAdvanceTime);
            assertEquals(handle1.getPriority().getLevel(), i + 1);

            assertEquals(splitQueue.getLevelScheduledTime(i), 2 * Math.min(levelAdvanceTime, LEVEL_CONTRIBUTION_CAP));
            assertEquals(splitQueue.getLevelScheduledTime(i + 1), 0);
        }
    }

    @Test
    public void testUpdateLevelWithCap()
    {
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(false, 2);
        TaskHandle handle0 = new TaskHandle(new TaskId("test0", 0, 0), splitQueue, () -> 1, 1, new Duration(1, SECONDS));

        long quantaNanos = MINUTES.toNanos(10);
        handle0.addScheduledNanos(quantaNanos);
        long cappedNanos = Math.min(quantaNanos, LEVEL_CONTRIBUTION_CAP);

        for (int i = 0; i < (LEVEL_THRESHOLD_SECONDS.length - 1); i++) {
            long thisLevelTime = Math.min(SECONDS.toNanos(LEVEL_THRESHOLD_SECONDS[i + 1] - LEVEL_THRESHOLD_SECONDS[i]), cappedNanos);
            assertEquals(splitQueue.getLevelScheduledTime(i), thisLevelTime);
            cappedNanos -= thisLevelTime;
        }
    }

    private static class TestingJob
            implements SplitRunner
    {
        private final TestingTicker ticker;
        private final Phaser globalPhaser;
        private final Phaser beginQuantaPhaser;
        private final Phaser endQuantaPhaser;
        private final int requiredPhases;
        private final int quantaTimeMillis;
        private final AtomicInteger completedPhases = new AtomicInteger();

        private final AtomicInteger firstPhase = new AtomicInteger(-1);
        private final AtomicInteger lastPhase = new AtomicInteger(-1);

        private final SettableFuture<?> completed = SettableFuture.create();

        public TestingJob(TestingTicker ticker, Phaser globalPhaser, Phaser beginQuantaPhaser, Phaser endQuantaPhaser, int requiredPhases, int quantaTimeMillis)
        {
            this.ticker = ticker;
            this.globalPhaser = globalPhaser;
            this.beginQuantaPhaser = beginQuantaPhaser;
            this.endQuantaPhaser = endQuantaPhaser;
            this.requiredPhases = requiredPhases;
            this.quantaTimeMillis = quantaTimeMillis;

            beginQuantaPhaser.register();
            endQuantaPhaser.register();

            if (globalPhaser.getRegisteredParties() == 0) {
                globalPhaser.register();
            }
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
        public ListenableFuture<?> processFor(Duration duration)
        {
            ticker.increment(quantaTimeMillis, MILLISECONDS);
            globalPhaser.arriveAndAwaitAdvance();
            int phase = beginQuantaPhaser.arriveAndAwaitAdvance();
            firstPhase.compareAndSet(-1, phase - 1);
            lastPhase.set(phase);
            endQuantaPhaser.arriveAndAwaitAdvance();
            if (completedPhases.incrementAndGet() >= requiredPhases) {
                endQuantaPhaser.arriveAndDeregister();
                beginQuantaPhaser.arriveAndDeregister();
                globalPhaser.arriveAndDeregister();
                completed.set(null);
            }

            return Futures.immediateFuture(null);
        }

        @Override
        public String getInfo()
        {
            return "testing-split";
        }

        @Override
        public boolean isFinished()
        {
            return completed.isDone();
        }

        @Override
        public void close()
        {
        }

        public Future<?> getCompletedFuture()
        {
            return completed;
        }
    }
}
