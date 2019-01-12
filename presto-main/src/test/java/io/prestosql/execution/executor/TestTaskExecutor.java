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
package io.prestosql.execution.executor;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import io.prestosql.execution.SplitRunner;
import io.prestosql.execution.TaskId;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.prestosql.execution.executor.MultilevelSplitQueue.LEVEL_CONTRIBUTION_CAP;
import static io.prestosql.execution.executor.MultilevelSplitQueue.LEVEL_THRESHOLD_SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTaskExecutor
{
    @Test(invocationCount = 100)
    public void testTasksComplete()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(4, 8, 3, 4, ticker);
        taskExecutor.start();
        ticker.increment(20, MILLISECONDS);

        try {
            TaskId taskId = new TaskId("test", 0, 0);
            TaskHandle taskHandle = taskExecutor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

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
            ticker.increment(60, SECONDS);
            assertEquals(taskExecutor.getRunAwaySplitCount(), 0);
            ticker.increment(600, SECONDS);
            assertEquals(taskExecutor.getRunAwaySplitCount(), 2);
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
            ticker.increment(610, SECONDS);
            assertEquals(taskExecutor.getRunAwaySplitCount(), 0);
        }
        finally {
            taskExecutor.stop();
        }
    }

    @Test(invocationCount = 100)
    public void testQuantaFairness()
    {
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(1, 2, 3, 4, ticker);
        taskExecutor.start();
        ticker.increment(20, MILLISECONDS);

        try {
            TaskHandle shortQuantaTaskHandle = taskExecutor.addTask(new TaskId("shortQuanta", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());
            TaskHandle longQuantaTaskHandle = taskExecutor.addTask(new TaskId("longQuanta", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

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
        TaskExecutor taskExecutor = new TaskExecutor(2, 2, 3, 4, ticker);
        taskExecutor.start();
        ticker.increment(20, MILLISECONDS);

        try {
            TaskHandle testTaskHandle = taskExecutor.addTask(new TaskId("test", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

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
    public void testLevelMultipliers()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(1, 3, 3, 4, new MultilevelSplitQueue(2), ticker);
        taskExecutor.start();
        ticker.increment(20, MILLISECONDS);

        try {
            for (int i = 0; i < (LEVEL_THRESHOLD_SECONDS.length - 1); i++) {
                TaskHandle[] taskHandles = {
                        taskExecutor.addTask(new TaskId("test1", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty()),
                        taskExecutor.addTask(new TaskId("test2", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty()),
                        taskExecutor.addTask(new TaskId("test3", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty())
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
        TaskExecutor taskExecutor = new TaskExecutor(4, 8, 3, 4, ticker);
        taskExecutor.start();

        try {
            TaskId taskId = new TaskId("test", 0, 0);
            TaskHandle taskHandle = taskExecutor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

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
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        TaskHandle handle0 = new TaskHandle(new TaskId("test0", 0, 0), splitQueue, () -> 1, 1, new Duration(1, SECONDS), OptionalInt.empty());
        TaskHandle handle1 = new TaskHandle(new TaskId("test1", 0, 0), splitQueue, () -> 1, 1, new Duration(1, SECONDS), OptionalInt.empty());

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
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        TaskHandle handle0 = new TaskHandle(new TaskId("test0", 0, 0), splitQueue, () -> 1, 1, new Duration(1, SECONDS), OptionalInt.empty());

        long quantaNanos = MINUTES.toNanos(10);
        handle0.addScheduledNanos(quantaNanos);
        long cappedNanos = Math.min(quantaNanos, LEVEL_CONTRIBUTION_CAP);

        for (int i = 0; i < (LEVEL_THRESHOLD_SECONDS.length - 1); i++) {
            long thisLevelTime = Math.min(SECONDS.toNanos(LEVEL_THRESHOLD_SECONDS[i + 1] - LEVEL_THRESHOLD_SECONDS[i]), cappedNanos);
            assertEquals(splitQueue.getLevelScheduledTime(i), thisLevelTime);
            cappedNanos -= thisLevelTime;
        }
    }

    @Test(timeOut = 30_000)
    public void testMinMaxDriversPerTask()
    {
        int maxDriversPerTask = 2;
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(4, 16, 1, maxDriversPerTask, splitQueue, ticker);
        taskExecutor.start();
        try {
            TaskHandle testTaskHandle = taskExecutor.addTask(new TaskId("test", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

            // enqueue all batches of splits
            int batchCount = 4;
            TestingJob[] splits = new TestingJob[8];
            Phaser[] phasers = new Phaser[batchCount];
            for (int batch = 0; batch < batchCount; batch++) {
                phasers[batch] = new Phaser();
                phasers[batch].register();
                TestingJob split1 = new TestingJob(ticker, new Phaser(), new Phaser(), phasers[batch], 1, 0);
                TestingJob split2 = new TestingJob(ticker, new Phaser(), new Phaser(), phasers[batch], 1, 0);
                splits[2 * batch] = split1;
                splits[2 * batch + 1] = split2;
                taskExecutor.enqueueSplits(testTaskHandle, false, ImmutableList.of(split1, split2));
            }

            // assert that the splits are processed in batches as expected
            for (int batch = 0; batch < batchCount; batch++) {
                // wait until the current batch starts
                waitUntilSplitsStart(ImmutableList.of(splits[2 * batch], splits[2 * batch + 1]));
                // assert that only the splits including and up to the current batch are running and the rest haven't started yet
                assertSplitStates(2 * batch + 1, splits);
                // complete the current batch
                phasers[batch].arriveAndDeregister();
            }
        }
        finally {
            taskExecutor.stop();
        }
    }

    @Test(timeOut = 30_000)
    public void testUserSpecifiedMaxDriversPerTask()
    {
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        TestingTicker ticker = new TestingTicker();
        // create a task executor with min/max drivers per task to be 2 and 4
        TaskExecutor taskExecutor = new TaskExecutor(4, 16, 2, 4, splitQueue, ticker);
        taskExecutor.start();
        try {
            // overwrite the max drivers per task to be 1
            TaskHandle testTaskHandle = taskExecutor.addTask(new TaskId("test", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.of(1));

            // enqueue all batches of splits
            int batchCount = 4;
            TestingJob[] splits = new TestingJob[4];
            Phaser[] phasers = new Phaser[batchCount];
            for (int batch = 0; batch < batchCount; batch++) {
                phasers[batch] = new Phaser();
                phasers[batch].register();
                TestingJob split = new TestingJob(ticker, new Phaser(), new Phaser(), phasers[batch], 1, 0);
                splits[batch] = split;
                taskExecutor.enqueueSplits(testTaskHandle, false, ImmutableList.of(split));
            }

            // assert that the splits are processed in batches as expected
            for (int batch = 0; batch < batchCount; batch++) {
                // wait until the current batch starts
                waitUntilSplitsStart(ImmutableList.of(splits[batch]));
                // assert that only the splits including and up to the current batch are running and the rest haven't started yet
                assertSplitStates(batch, splits);
                // complete the current batch
                phasers[batch].arriveAndDeregister();
            }
        }
        finally {
            taskExecutor.stop();
        }
    }

    private void assertSplitStates(int endIndex, TestingJob[] splits)
    {
        // assert that splits up to and including endIndex are all started
        for (int i = 0; i <= endIndex; i++) {
            assertTrue(splits[i].isStarted());
        }

        // assert that splits starting from endIndex haven't started yet
        for (int i = endIndex + 1; i < splits.length; i++) {
            assertFalse(splits[i].isStarted());
        }
    }

    private static void waitUntilSplitsStart(List<TestingJob> splits)
    {
        while (splits.stream().anyMatch(split -> !split.isStarted())) {
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
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

        private final AtomicBoolean started = new AtomicBoolean();
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
            started.set(true);
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

        public boolean isStarted()
        {
            return started.get();
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
