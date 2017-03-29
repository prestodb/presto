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
import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.stats.CpuTimer;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

class PrioritizedSplitRunner
        implements Comparable<PrioritizedSplitRunner>
{
    private static final AtomicLong NEXT_WORKER_ID = new AtomicLong();

    private static final Logger log = Logger.get(PrioritizedSplitRunner.class);

    // each time we run a split, run it for this length before returning to the pool
    private static final Duration SPLIT_RUN_QUANTA = new Duration(1, TimeUnit.SECONDS);

    private final long createdNanos = System.nanoTime();

    private final TaskHandle taskHandle;
    private final int splitId;
    private final long workerId;
    private final SplitRunner split;

    private final Ticker ticker;

    private final SettableFuture<?> finishedFuture = SettableFuture.create();

    private final AtomicBoolean destroyed = new AtomicBoolean();

    private final AtomicInteger priorityLevel = new AtomicInteger();
    private final AtomicLong taskScheduledNanos = new AtomicLong();

    private final AtomicLong lastRun = new AtomicLong();
    private final AtomicLong start = new AtomicLong();

    private final AtomicLong scheduledNanos = new AtomicLong();
    private final AtomicLong cpuTimeNanos = new AtomicLong();
    private final AtomicLong processCalls = new AtomicLong();

    private final CounterStat globalCpuTimeMicros;
    private final CounterStat globalScheduledTimeMicros;

    private final TimeStat blockedQuantaWallTime;
    private final TimeStat unblockedQuantaWallTime;

    PrioritizedSplitRunner(
            TaskHandle taskHandle,
            SplitRunner split,
            Ticker ticker,
            CounterStat globalCpuTimeMicros,
            CounterStat globalScheduledTimeMicros,
            TimeStat blockedQuantaWallTime,
            TimeStat unblockedQuantaWallTime)
    {
        this.taskHandle = taskHandle;
        this.splitId = taskHandle.getNextSplitId();
        this.split = split;
        this.ticker = ticker;
        this.workerId = NEXT_WORKER_ID.getAndIncrement();
        this.globalCpuTimeMicros = globalCpuTimeMicros;
        this.globalScheduledTimeMicros = globalScheduledTimeMicros;
        this.blockedQuantaWallTime = blockedQuantaWallTime;
        this.unblockedQuantaWallTime = unblockedQuantaWallTime;
    }

    public TaskHandle getTaskHandle()
    {
        return taskHandle;
    }

    public ListenableFuture<?> getFinishedFuture()
    {
        return finishedFuture;
    }

    public boolean isDestroyed()
    {
        return destroyed.get();
    }

    public void destroy()
    {
        destroyed.set(true);
        try {
            split.close();
        }
        catch (RuntimeException e) {
            log.error(e, "Error closing split for task %s", taskHandle.getTaskId());
        }
    }

    public long getCreatedNanos()
    {
        return createdNanos;
    }

    public boolean isFinished()
    {
        boolean finished = split.isFinished();
        if (finished) {
            finishedFuture.set(null);
        }
        return finished || destroyed.get() || taskHandle.isDestroyed();
    }

    public long getScheduledNanos()
    {
        return scheduledNanos.get();
    }

    public ListenableFuture<?> process()
            throws Exception
    {
        try {
            start.compareAndSet(0, ticker.read());
            processCalls.incrementAndGet();

            CpuTimer timer = new CpuTimer();
            ListenableFuture<?> blocked = split.processFor(SPLIT_RUN_QUANTA);
            CpuTimer.CpuDuration elapsed = timer.elapsedTime();

            // update priority level base on total thread usage of task
            long quantaScheduledNanos = elapsed.getWall().roundTo(NANOSECONDS);
            scheduledNanos.addAndGet(quantaScheduledNanos);

            long taskScheduledTimeNanos = taskHandle.addThreadUsageNanos(quantaScheduledNanos);
            taskScheduledNanos.set(taskScheduledTimeNanos);

            priorityLevel.set(calculatePriorityLevel(taskScheduledTimeNanos));

            // record last run for prioritization within a level
            lastRun.set(ticker.read());

            if (blocked == NOT_BLOCKED) {
                unblockedQuantaWallTime.add(elapsed.getWall());
            }
            else {
                blockedQuantaWallTime.add(elapsed.getWall());
            }

            long quantaCpuNanos = elapsed.getCpu().roundTo(NANOSECONDS);
            cpuTimeNanos.addAndGet(quantaCpuNanos);

            globalCpuTimeMicros.update(quantaCpuNanos / 1000);
            globalScheduledTimeMicros.update(quantaWallNanos / 1000);

            return blocked;
        }
        catch (Throwable e) {
            finishedFuture.setException(e);
            throw e;
        }
    }

    public boolean updatePriorityLevel()
    {
        int newPriority = calculatePriorityLevel(taskHandle.getThreadUsageNanos());
        if (newPriority == priorityLevel.getAndSet(newPriority)) {
            return false;
        }

        // update thread usage while if level changed
        taskScheduledNanos.set(taskHandle.getThreadUsageNanos());
        return true;
    }

    @Override
    public int compareTo(PrioritizedSplitRunner o)
    {
        int level = priorityLevel.get();

        int result = Integer.compare(level, o.priorityLevel.get());
        if (result != 0) {
            return result;
        }

        if (level < 4) {
            result = Long.compare(taskScheduledNanos.get(), o.taskScheduledNanos.get());
        }
        else {
            result = Long.compare(lastRun.get(), o.lastRun.get());
        }
        if (result != 0) {
            return result;
        }

        return Long.compare(workerId, o.workerId);
    }

    public int getSplitId()
    {
        return splitId;
    }

    public AtomicInteger getPriorityLevel()
    {
        return priorityLevel;
    }

    public static int calculatePriorityLevel(long threadUsageNanos)
    {
        long millis = NANOSECONDS.toMillis(threadUsageNanos);

        int priorityLevel;
        if (millis < 1000) {
            priorityLevel = 0;
        }
        else if (millis < 10_000) {
            priorityLevel = 1;
        }
        else if (millis < 60_000) {
            priorityLevel = 2;
        }
        else if (millis < 300_000) {
            priorityLevel = 3;
        }
        else {
            priorityLevel = 4;
        }
        return priorityLevel;
    }

    public String getInfo()
    {
        return String.format("Split %-15s-%d %s (start = %s, wall = %s ms, cpu = %s ms, calls = %s)",
                taskHandle.getTaskId(),
                splitId,
                split.getInfo(),
                start.get() / 1.0e6,
                (int) ((ticker.read() - start.get()) / 1.0e6),
                (int) (cpuTimeNanos.get() / 1.0e6),
                processCalls.get());
    }

    @Override
    public String toString()
    {
        return String.format("Split %-15s-%d", taskHandle.getTaskId(), splitId);
    }
}
