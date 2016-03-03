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
package com.facebook.presto.operator;

import com.facebook.presto.Session;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class TaskContext
{
    private final QueryContext queryContext;
    private final TaskStateMachine taskStateMachine;
    private final Executor executor;
    private final Session session;

    private final DataSize operatorPreAllocatedMemory;
    private final AtomicLong peekMemoryReservation = new AtomicLong();
    private final AtomicLong memoryReservation = new AtomicLong();
    private final AtomicLong systemMemoryReservation = new AtomicLong();

    private final long createNanos = System.nanoTime();

    private final AtomicLong startNanos = new AtomicLong();
    private final AtomicLong endNanos = new AtomicLong();

    private final AtomicReference<DateTime> executionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> lastExecutionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> executionEndTime = new AtomicReference<>();

    private final List<PipelineContext> pipelineContexts = new CopyOnWriteArrayList<>();

    private final boolean verboseStats;
    private final boolean cpuTimerEnabled;

    private final Object cumulativeMemoryLock = new Object();
    private final AtomicDouble cumulativeMemory = new AtomicDouble(0.0);

    @GuardedBy("cumulativeMemoryLock")
    private long lastMemoryReservation = 0;

    @GuardedBy("cumulativeMemoryLock")
    private long lastTaskStatCallNanos = 0;

    public TaskContext(QueryContext queryContext,
            TaskStateMachine taskStateMachine,
            Executor executor,
            Session session,
            DataSize operatorPreAllocatedMemory,
            boolean verboseStats,
            boolean cpuTimerEnabled)
    {
        this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
        this.queryContext = requireNonNull(queryContext, "queryContext is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.session = session;
        this.operatorPreAllocatedMemory = requireNonNull(operatorPreAllocatedMemory, "operatorPreAllocatedMemory is null");
        taskStateMachine.addStateChangeListener(new StateChangeListener<TaskState>()
        {
            @Override
            public void stateChanged(TaskState newState)
            {
                if (newState.isDone()) {
                    executionEndTime.set(DateTime.now());
                    endNanos.set(System.nanoTime());
                }
            }
        });

        this.verboseStats = verboseStats;
        this.cpuTimerEnabled = cpuTimerEnabled;
    }

    public TaskId getTaskId()
    {
        return taskStateMachine.getTaskId();
    }

    public PipelineContext addPipelineContext(boolean inputPipeline, boolean outputPipeline)
    {
        PipelineContext pipelineContext = new PipelineContext(this, executor, inputPipeline, outputPipeline);
        pipelineContexts.add(pipelineContext);
        return pipelineContext;
    }

    public Session getSession()
    {
        return session;
    }

    public void start()
    {
        DateTime now = DateTime.now();
        executionStartTime.compareAndSet(null, now);
        startNanos.compareAndSet(0, System.nanoTime());

        // always update last execution start time
        lastExecutionStartTime.set(now);
    }

    public void failed(Throwable cause)
    {
        taskStateMachine.failed(cause);
    }

    public boolean isDone()
    {
        return taskStateMachine.getState().isDone();
    }

    public TaskState getState()
    {
        return taskStateMachine.getState();
    }

    public DataSize getOperatorPreAllocatedMemory()
    {
        return operatorPreAllocatedMemory;
    }

    public synchronized ListenableFuture<?> reserveMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        ListenableFuture<?> future = queryContext.reserveMemory(bytes);
        long reservation = memoryReservation.getAndAdd(bytes);
        if (peekMemoryReservation.get() < reservation) {
            peekMemoryReservation.set(reservation);
        }
        return future;
    }

    public synchronized ListenableFuture<?> reserveSystemMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        ListenableFuture<?> future = queryContext.reserveSystemMemory(bytes);
        systemMemoryReservation.getAndAdd(bytes);
        return future;
    }

    public synchronized boolean tryReserveMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        if (queryContext.tryReserveMemory(bytes)) {
            long reservation = memoryReservation.getAndAdd(bytes);
            if (peekMemoryReservation.get() < reservation) {
                peekMemoryReservation.set(reservation);
            }
            return true;
        }
        return false;
    }

    public synchronized void freeMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(bytes <= memoryReservation.get(), "tried to free more memory than is reserved");
        memoryReservation.getAndAdd(-bytes);
        queryContext.freeMemory(bytes);
    }

    public synchronized void freeSystemMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(bytes <= systemMemoryReservation.get(), "tried to free more memory than is reserved");
        systemMemoryReservation.getAndAdd(-bytes);
        queryContext.freeSystemMemory(bytes);
    }

    public void moreMemoryAvailable()
    {
        pipelineContexts.stream().forEach(PipelineContext::moreMemoryAvailable);
    }

    public boolean isVerboseStats()
    {
        return verboseStats;
    }

    public boolean isCpuTimerEnabled()
    {
        return cpuTimerEnabled;
    }

    public CounterStat getInputDataSize()
    {
        CounterStat stat = new CounterStat();
        for (PipelineContext pipelineContext : pipelineContexts) {
            if (pipelineContext.isInputPipeline()) {
                stat.merge(pipelineContext.getInputDataSize());
            }
        }
        return stat;
    }

    public CounterStat getInputPositions()
    {
        CounterStat stat = new CounterStat();
        for (PipelineContext pipelineContext : pipelineContexts) {
            if (pipelineContext.isInputPipeline()) {
                stat.merge(pipelineContext.getInputPositions());
            }
        }
        return stat;
    }

    public CounterStat getOutputDataSize()
    {
        CounterStat stat = new CounterStat();
        for (PipelineContext pipelineContext : pipelineContexts) {
            if (pipelineContext.isOutputPipeline()) {
                stat.merge(pipelineContext.getOutputDataSize());
            }
        }
        return stat;
    }

    public CounterStat getOutputPositions()
    {
        CounterStat stat = new CounterStat();
        for (PipelineContext pipelineContext : pipelineContexts) {
            if (pipelineContext.isOutputPipeline()) {
                stat.merge(pipelineContext.getOutputPositions());
            }
        }
        return stat;
    }

    @VisibleForTesting
    public long getPeekMemoryReservation()
    {
        return peekMemoryReservation.get();
    }

    public TaskStats getTaskStats()
    {
        // check for end state to avoid callback ordering problems
        if (taskStateMachine.getState().isDone()) {
            DateTime now = DateTime.now();
            if (executionEndTime.compareAndSet(null, now)) {
                lastExecutionStartTime.compareAndSet(null, now);
                endNanos.set(System.nanoTime());
            }
        }

        List<PipelineStats> pipelineStats = ImmutableList.copyOf(transform(pipelineContexts, PipelineContext::getPipelineStats));

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int queuedPartitionedDrivers = 0;
        int runningDrivers = 0;
        int runningPartitionedDrivers = 0;
        int completedDrivers = 0;

        long totalScheduledTime = 0;
        long totalCpuTime = 0;
        long totalUserTime = 0;
        long totalBlockedTime = 0;

        long rawInputDataSize = 0;
        long rawInputPositions = 0;

        long processedInputDataSize = 0;
        long processedInputPositions = 0;

        long outputDataSize = 0;
        long outputPositions = 0;

        for (PipelineStats pipeline : pipelineStats) {
            totalDrivers += pipeline.getTotalDrivers();
            queuedDrivers += pipeline.getQueuedDrivers();
            queuedPartitionedDrivers += pipeline.getQueuedPartitionedDrivers();
            runningDrivers += pipeline.getRunningDrivers();
            runningPartitionedDrivers += pipeline.getRunningPartitionedDrivers();
            completedDrivers += pipeline.getCompletedDrivers();

            totalScheduledTime += pipeline.getTotalScheduledTime().roundTo(NANOSECONDS);
            totalCpuTime += pipeline.getTotalCpuTime().roundTo(NANOSECONDS);
            totalUserTime += pipeline.getTotalUserTime().roundTo(NANOSECONDS);
            totalBlockedTime += pipeline.getTotalBlockedTime().roundTo(NANOSECONDS);

            if (pipeline.isInputPipeline()) {
                rawInputDataSize += pipeline.getRawInputDataSize().toBytes();
                rawInputPositions += pipeline.getRawInputPositions();

                processedInputDataSize += pipeline.getProcessedInputDataSize().toBytes();
                processedInputPositions += pipeline.getProcessedInputPositions();
            }

            if (pipeline.isOutputPipeline()) {
                outputDataSize += pipeline.getOutputDataSize().toBytes();
                outputPositions += pipeline.getOutputPositions();
            }
        }

        long startNanos = this.startNanos.get();
        if (startNanos < createNanos) {
            startNanos = System.nanoTime();
        }
        Duration queuedTime = new Duration(startNanos - createNanos, NANOSECONDS);

        long endNanos = this.endNanos.get();
        Duration elapsedTime;
        if (endNanos >= startNanos) {
            elapsedTime = new Duration(endNanos - createNanos, NANOSECONDS);
        }
        else {
            elapsedTime = new Duration(0, NANOSECONDS);
        }

        synchronized (cumulativeMemoryLock) {
            double sinceLastPeriodMillis = (System.nanoTime() - lastTaskStatCallNanos) / 1_000_000.0;
            long currentSystemMemory = systemMemoryReservation.get();
            long averageMemoryForLastPeriod = (currentSystemMemory + lastMemoryReservation) / 2;
            cumulativeMemory.addAndGet(averageMemoryForLastPeriod * sinceLastPeriodMillis);

            lastTaskStatCallNanos = System.nanoTime();
            lastMemoryReservation = currentSystemMemory;
        }

        boolean fullyBlocked = pipelineStats.stream()
                .filter(pipeline -> pipeline.getRunningDrivers() > 0 || pipeline.getRunningPartitionedDrivers() > 0)
                .allMatch(PipelineStats::isFullyBlocked);
        ImmutableSet<BlockedReason> blockedReasons = pipelineStats.stream()
                .filter(pipeline -> pipeline.getRunningDrivers() > 0 || pipeline.getRunningPartitionedDrivers() > 0)
                .flatMap(pipeline -> pipeline.getBlockedReasons().stream())
                .collect(ImmutableCollectors.toImmutableSet());
        return new TaskStats(
                taskStateMachine.getCreatedTime(),
                executionStartTime.get(),
                lastExecutionStartTime.get(),
                executionEndTime.get(),
                elapsedTime.convertToMostSuccinctTimeUnit(),
                queuedTime.convertToMostSuccinctTimeUnit(),
                totalDrivers,
                queuedDrivers,
                queuedPartitionedDrivers,
                runningDrivers,
                runningPartitionedDrivers,
                completedDrivers,
                cumulativeMemory.get(),
                new DataSize(memoryReservation.get(), BYTE).convertToMostSuccinctDataSize(),
                new DataSize(systemMemoryReservation.get(), BYTE).convertToMostSuccinctDataSize(),
                new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalUserTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                fullyBlocked && (runningDrivers > 0 || runningPartitionedDrivers > 0),
                blockedReasons,
                new DataSize(rawInputDataSize, BYTE).convertToMostSuccinctDataSize(),
                rawInputPositions,
                new DataSize(processedInputDataSize, BYTE).convertToMostSuccinctDataSize(),
                processedInputPositions,
                new DataSize(outputDataSize, BYTE).convertToMostSuccinctDataSize(),
                outputPositions,
                pipelineStats);
    }
}
