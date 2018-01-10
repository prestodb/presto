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
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.memory.QueryContextVisitor;
import com.facebook.presto.memory.Reservations;
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
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.memory.Reservations.checkFreedBytes;
import static com.facebook.presto.memory.Reservations.checkReservedBytes;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class TaskContext
{
    private final QueryContext queryContext;
    private final TaskStateMachine taskStateMachine;
    private final Executor notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final Session session;

    private final AtomicLong memoryReservation = new AtomicLong();
    private final AtomicLong systemMemoryReservation = new AtomicLong();
    private final AtomicLong revocableMemoryReservation = new AtomicLong();

    private final long createNanos = System.nanoTime();

    private final AtomicLong startNanos = new AtomicLong();
    private final AtomicLong endNanos = new AtomicLong();

    private final AtomicReference<DateTime> executionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> lastExecutionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> executionEndTime = new AtomicReference<>();

    private final Set<Lifespan> completedDriverGroups = newConcurrentHashSet();

    private final List<PipelineContext> pipelineContexts = new CopyOnWriteArrayList<>();

    private final boolean verboseStats;
    private final boolean cpuTimerEnabled;

    private final Object cumulativeMemoryLock = new Object();
    private final AtomicDouble cumulativeMemory = new AtomicDouble(0.0);

    @GuardedBy("cumulativeMemoryLock")
    private long lastMemoryReservation;

    @GuardedBy("cumulativeMemoryLock")
    private long lastTaskStatCallNanos;

    public static TaskContext createTaskContext(
            QueryContext queryContext,
            TaskStateMachine taskStateMachine,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            Session session,
            boolean verboseStats,
            boolean cpuTimerEnabled)
    {
        TaskContext taskContext = new TaskContext(queryContext, taskStateMachine, notificationExecutor, yieldExecutor, session, verboseStats, cpuTimerEnabled);
        taskContext.initialize();
        return taskContext;
    }

    private TaskContext(
            QueryContext queryContext,
            TaskStateMachine taskStateMachine,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            Session session,
            boolean verboseStats,
            boolean cpuTimerEnabled)
    {
        this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
        this.queryContext = requireNonNull(queryContext, "queryContext is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.yieldExecutor = requireNonNull(yieldExecutor, "yieldExecutor is null");
        this.session = session;
        this.verboseStats = verboseStats;
        this.cpuTimerEnabled = cpuTimerEnabled;
    }

    // the state change listener is added here in a separate initialize() method
    // instead of the constructor to prevent leaking the "this" reference to
    // another thread, which will cause unsafe publication of this instance.
    private void initialize()
    {
        taskStateMachine.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                executionEndTime.set(DateTime.now());
                endNanos.set(System.nanoTime());
            }
        });
    }

    public TaskId getTaskId()
    {
        return taskStateMachine.getTaskId();
    }

    public PipelineContext addPipelineContext(int pipelineId, boolean inputPipeline, boolean outputPipeline)
    {
        PipelineContext pipelineContext = new PipelineContext(pipelineId, this, notificationExecutor, yieldExecutor, inputPipeline, outputPipeline);
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

    public DataSize getMemoryReservation()
    {
        return new DataSize(memoryReservation.get(), BYTE);
    }

    /**
     * Returns the completed driver groups (excluding taskWide).
     * A driver group is considered complete if all drivers associated with it
     * has completed, and no new drivers associated with it will be created.
     */
    public Set<Lifespan> getCompletedDriverGroups()
    {
        return completedDriverGroups;
    }

    public void addCompletedDriverGroup(Lifespan driverGroup)
    {
        checkArgument(!driverGroup.isTaskWide(), "driverGroup is task-wide, not a driver group.");
        completedDriverGroups.add(driverGroup);
    }

    public List<PipelineContext> getPipelineContexts()
    {
        return pipelineContexts;
    }

    public synchronized ListenableFuture<?> reserveMemory(long bytes)
    {
        checkReservedBytes(bytes);
        ListenableFuture<?> future = queryContext.reserveMemory(bytes);
        memoryReservation.accumulateAndGet(bytes, Reservations::sum);
        return future;
    }

    public synchronized ListenableFuture<?> reserveRevocableMemory(long bytes)
    {
        checkReservedBytes(bytes);
        ListenableFuture<?> future = queryContext.reserveRevocableMemory(bytes);
        revocableMemoryReservation.accumulateAndGet(bytes, Reservations::sum);
        return future;
    }

    public synchronized ListenableFuture<?> reserveSystemMemory(long bytes)
    {
        checkReservedBytes(bytes);
        ListenableFuture<?> future = queryContext.reserveSystemMemory(bytes);
        systemMemoryReservation.accumulateAndGet(bytes, Reservations::sum);
        return future;
    }

    public synchronized ListenableFuture<?> reserveSpill(long bytes)
    {
        checkReservedBytes(bytes);
        return queryContext.reserveSpill(bytes);
    }

    public synchronized boolean tryReserveMemory(long bytes)
    {
        checkReservedBytes(bytes);
        if (queryContext.tryReserveMemory(bytes)) {
            memoryReservation.accumulateAndGet(bytes, Reservations::sum);
            return true;
        }
        return false;
    }

    public synchronized void freeMemory(long bytes)
    {
        checkFreedBytes(bytes, memoryReservation.get());
        memoryReservation.getAndAdd(-bytes);
        queryContext.freeMemory(bytes);
    }

    public synchronized void freeRevocableMemory(long bytes)
    {
        checkFreedBytes(bytes, revocableMemoryReservation.get());
        revocableMemoryReservation.getAndAdd(-bytes);
        queryContext.freeRevocableMemory(bytes);
    }

    public synchronized void freeSystemMemory(long bytes)
    {
        checkFreedBytes(bytes, systemMemoryReservation.get());
        systemMemoryReservation.getAndAdd(-bytes);
        queryContext.freeSystemMemory(bytes);
    }

    public synchronized void freeSpill(long bytes)
    {
        queryContext.freeSpill(bytes);
    }

    public void moreMemoryAvailable()
    {
        pipelineContexts.forEach(PipelineContext::moreMemoryAvailable);
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

        long lastExecutionEndTime = 0;

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int queuedPartitionedDrivers = 0;
        int runningDrivers = 0;
        int runningPartitionedDrivers = 0;
        int blockedDrivers = 0;
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

        long physicalWrittenDataSize = 0;

        for (PipelineStats pipeline : pipelineStats) {
            if (pipeline.getLastEndTime() != null) {
                lastExecutionEndTime = max(pipeline.getLastEndTime().getMillis(), lastExecutionEndTime);
            }

            totalDrivers += pipeline.getTotalDrivers();
            queuedDrivers += pipeline.getQueuedDrivers();
            queuedPartitionedDrivers += pipeline.getQueuedPartitionedDrivers();
            runningDrivers += pipeline.getRunningDrivers();
            runningPartitionedDrivers += pipeline.getRunningPartitionedDrivers();
            blockedDrivers += pipeline.getBlockedDrivers();
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

            physicalWrittenDataSize += pipeline.getPhysicalWrittenDataSize().toBytes();
        }

        long startNanos = this.startNanos.get();
        if (startNanos == 0) {
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
            long currentMemory = memoryReservation.get();
            long averageMemoryForLastPeriod = (currentMemory + lastMemoryReservation) / 2;
            cumulativeMemory.addAndGet(averageMemoryForLastPeriod * sinceLastPeriodMillis);

            lastTaskStatCallNanos = System.nanoTime();
            lastMemoryReservation = currentMemory;
        }

        Set<PipelineStats> runningPipelineStats = pipelineStats.stream()
                .filter(pipeline -> pipeline.getRunningDrivers() > 0 || pipeline.getRunningPartitionedDrivers() > 0 || pipeline.getBlockedDrivers() > 0)
                .collect(toImmutableSet());
        ImmutableSet<BlockedReason> blockedReasons = runningPipelineStats.stream()
                .flatMap(pipeline -> pipeline.getBlockedReasons().stream())
                .collect(toImmutableSet());

        boolean fullyBlocked = !runningPipelineStats.isEmpty() && runningPipelineStats.stream().allMatch(PipelineStats::isFullyBlocked);

        return new TaskStats(
                taskStateMachine.getCreatedTime(),
                executionStartTime.get(),
                lastExecutionStartTime.get(),
                lastExecutionEndTime == 0 ? null : new DateTime(lastExecutionEndTime),
                executionEndTime.get(),
                elapsedTime.convertToMostSuccinctTimeUnit(),
                queuedTime.convertToMostSuccinctTimeUnit(),
                totalDrivers,
                queuedDrivers,
                queuedPartitionedDrivers,
                runningDrivers,
                runningPartitionedDrivers,
                blockedDrivers,
                completedDrivers,
                cumulativeMemory.get(),
                succinctBytes(memoryReservation.get()),
                succinctBytes(revocableMemoryReservation.get()),
                succinctBytes(systemMemoryReservation.get()),
                new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalUserTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                fullyBlocked && (runningDrivers > 0 || runningPartitionedDrivers > 0),
                blockedReasons,
                succinctBytes(rawInputDataSize),
                rawInputPositions,
                succinctBytes(processedInputDataSize),
                processedInputPositions,
                succinctBytes(outputDataSize),
                outputPositions,
                succinctBytes(physicalWrittenDataSize),
                pipelineStats);
    }

    public <C, R> R accept(QueryContextVisitor<C, R> visitor, C context)
    {
        return visitor.visitTaskContext(this, context);
    }

    public <C, R> List<R> acceptChildren(QueryContextVisitor<C, R> visitor, C context)
    {
        return pipelineContexts.stream()
                .map(pipelineContext -> pipelineContext.accept(visitor, context))
                .collect(toList());
    }
}
