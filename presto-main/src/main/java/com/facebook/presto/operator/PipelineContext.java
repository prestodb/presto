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
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.memory.MemoryTrackingContext;
import com.facebook.presto.memory.QueryContextVisitor;
import com.facebook.presto.spi.memory.LocalMemoryContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.stats.Distribution;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class PipelineContext
{
    private final TaskContext taskContext;
    private final Executor notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final int pipelineId;

    private final boolean inputPipeline;
    private final boolean outputPipeline;

    private final List<DriverContext> drivers = new CopyOnWriteArrayList<>();

    private final AtomicInteger completedDrivers = new AtomicInteger();

    private final AtomicReference<DateTime> executionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> lastExecutionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> lastExecutionEndTime = new AtomicReference<>();

    private final Distribution queuedTime = new Distribution();
    private final Distribution elapsedTime = new Distribution();

    private final AtomicLong totalScheduledTime = new AtomicLong();
    private final AtomicLong totalCpuTime = new AtomicLong();
    private final AtomicLong totalUserTime = new AtomicLong();
    private final AtomicLong totalBlockedTime = new AtomicLong();

    private final CounterStat rawInputDataSize = new CounterStat();
    private final CounterStat rawInputPositions = new CounterStat();

    private final CounterStat processedInputDataSize = new CounterStat();
    private final CounterStat processedInputPositions = new CounterStat();

    private final CounterStat outputDataSize = new CounterStat();
    private final CounterStat outputPositions = new CounterStat();

    private final ConcurrentMap<Integer, OperatorStats> operatorSummaries = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final MemoryTrackingContext pipelineMemoryContext;

    public PipelineContext(int pipelineId, TaskContext taskContext, Executor notificationExecutor, ScheduledExecutorService yieldExecutor, MemoryTrackingContext pipelineMemoryContext, boolean inputPipeline, boolean outputPipeline)
    {
        this.pipelineId = pipelineId;
        this.inputPipeline = inputPipeline;
        this.outputPipeline = outputPipeline;
        this.taskContext = requireNonNull(taskContext, "taskContext is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.yieldExecutor = requireNonNull(yieldExecutor, "yieldExecutor is null");
        this.pipelineMemoryContext = requireNonNull(pipelineMemoryContext, "pipelineMemoryContext is null");
        this.pipelineMemoryContext.localSystemMemoryContext().setMemoryNotificationListener(this::systemMemoryReservationChanged);
    }

    public TaskContext getTaskContext()
    {
        return taskContext;
    }

    public TaskId getTaskId()
    {
        return taskContext.getTaskId();
    }

    public int getPipelineId()
    {
        return pipelineId;
    }

    public boolean isInputPipeline()
    {
        return inputPipeline;
    }

    public boolean isOutputPipeline()
    {
        return outputPipeline;
    }

    public DriverContext addDriverContext()
    {
        return addDriverContext(false);
    }

    public DriverContext addDriverContext(boolean partitioned)
    {
        DriverContext driverContext = new DriverContext(
                this,
                notificationExecutor,
                yieldExecutor,
                pipelineMemoryContext.newMemoryTrackingContext(),
                partitioned);
        drivers.add(driverContext);
        return driverContext;
    }

    public Session getSession()
    {
        return taskContext.getSession();
    }

    public void driverFinished(DriverContext driverContext)
    {
        requireNonNull(driverContext, "driverContext is null");

        if (!drivers.remove(driverContext)) {
            throw new IllegalArgumentException("Unknown driver " + driverContext);
        }

        // always update last execution end time
        lastExecutionEndTime.set(DateTime.now());

        DriverStats driverStats = driverContext.getDriverStats();

        completedDrivers.getAndIncrement();

        queuedTime.add(driverStats.getQueuedTime().roundTo(NANOSECONDS));
        elapsedTime.add(driverStats.getElapsedTime().roundTo(NANOSECONDS));

        totalScheduledTime.getAndAdd(driverStats.getTotalScheduledTime().roundTo(NANOSECONDS));
        totalCpuTime.getAndAdd(driverStats.getTotalCpuTime().roundTo(NANOSECONDS));
        totalUserTime.getAndAdd(driverStats.getTotalUserTime().roundTo(NANOSECONDS));

        totalBlockedTime.getAndAdd(driverStats.getTotalBlockedTime().roundTo(NANOSECONDS));

        // merge the operator stats into the operator summary
        List<OperatorStats> operators = driverStats.getOperatorStats();
        for (OperatorStats operator : operators) {
            // TODO: replace with ConcurrentMap.compute() when we migrate to java 8
            OperatorStats updated;
            OperatorStats current;
            do {
                current = operatorSummaries.get(operator.getOperatorId());
                if (current != null) {
                    updated = current.add(operator);
                }
                else {
                    updated = operator;
                }
            }
            while (!compareAndSet(operatorSummaries, operator.getOperatorId(), current, updated));
        }

        rawInputDataSize.update(driverStats.getRawInputDataSize().toBytes());
        rawInputPositions.update(driverStats.getRawInputPositions());

        processedInputDataSize.update(driverStats.getProcessedInputDataSize().toBytes());
        processedInputPositions.update(driverStats.getProcessedInputPositions());

        outputDataSize.update(driverStats.getOutputDataSize().toBytes());
        outputPositions.update(driverStats.getOutputPositions());
    }

    public void start()
    {
        DateTime now = DateTime.now();
        executionStartTime.compareAndSet(null, now);
        // always update last execution start time
        lastExecutionStartTime.set(now);

        taskContext.start();
    }

    public void failed(Throwable cause)
    {
        taskContext.failed(cause);
    }

    public boolean isDone()
    {
        return taskContext.isDone();
    }

    public synchronized ListenableFuture<?> reserveMemory(long bytes)
    {
        return taskContext.reserveMemory(bytes);
    }

    public synchronized ListenableFuture<?> reserveRevocableMemory(long bytes)
    {
        return taskContext.reserveRevocableMemory(bytes);
    }

    public synchronized ListenableFuture<?> reserveSystemMemory(long bytes)
    {
        return taskContext.reserveSystemMemory(bytes);
    }

    public synchronized ListenableFuture<?> reserveSpill(long bytes)
    {
        return taskContext.reserveSpill(bytes);
    }

    public synchronized boolean tryReserveMemory(long bytes)
    {
        return taskContext.tryReserveMemory(bytes);
    }

    public synchronized void freeMemory(long bytes)
    {
        taskContext.freeMemory(bytes);
    }

    public synchronized void freeRevocableMemory(long bytes)
    {
        taskContext.freeRevocableMemory(bytes);
    }

    public synchronized void freeSystemMemory(long bytes)
    {
        taskContext.freeSystemMemory(bytes);
    }

    public synchronized void freeSpill(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        taskContext.freeSpill(bytes);
    }

    // we need this listener to reflect changes all the way up to the system memory pool
    private synchronized void systemMemoryReservationChanged(long oldUsage, long newUsage)
    {
        long delta = newUsage - oldUsage;
        if (delta >= 0) {
            taskContext.reserveSystemMemory(delta);
        }
        else {
            taskContext.freeSystemMemory(-delta);
        }
    }

    // this is OK because we already have a memory notification listener,
    // so we can keep track of all allocations.
    public LocalMemoryContext localSystemMemoryContext()
    {
        return pipelineMemoryContext.localSystemMemoryContext();
    }

    public void moreMemoryAvailable()
    {
        drivers.forEach(DriverContext::moreMemoryAvailable);
    }

    public boolean isVerboseStats()
    {
        return taskContext.isVerboseStats();
    }

    public boolean isCpuTimerEnabled()
    {
        return taskContext.isCpuTimerEnabled();
    }

    public CounterStat getInputDataSize()
    {
        CounterStat stat = new CounterStat();
        stat.merge(rawInputDataSize);
        for (DriverContext driver : drivers) {
            stat.merge(driver.getInputDataSize());
        }
        return stat;
    }

    public CounterStat getInputPositions()
    {
        CounterStat stat = new CounterStat();
        stat.merge(rawInputPositions);
        for (DriverContext driver : drivers) {
            stat.merge(driver.getInputPositions());
        }
        return stat;
    }

    public CounterStat getOutputDataSize()
    {
        CounterStat stat = new CounterStat();
        stat.merge(outputDataSize);
        for (DriverContext driver : drivers) {
            stat.merge(driver.getOutputDataSize());
        }
        return stat;
    }

    public CounterStat getOutputPositions()
    {
        CounterStat stat = new CounterStat();
        stat.merge(outputPositions);
        for (DriverContext driver : drivers) {
            stat.merge(driver.getOutputPositions());
        }
        return stat;
    }

    public PipelineStats getPipelineStats()
    {
        // check for end state to avoid callback ordering problems
        if (taskContext.getState().isDone()) {
            DateTime now = DateTime.now();
            executionStartTime.compareAndSet(null, now);
            lastExecutionStartTime.compareAndSet(null, now);
            lastExecutionEndTime.compareAndSet(null, now);
        }

        List<DriverContext> driverContexts = ImmutableList.copyOf(this.drivers);

        int totalDriers = completedDrivers.get() + driverContexts.size();
        int queuedDrivers = 0;
        int queuedPartitionedDrivers = 0;
        int runningDrivers = 0;
        int runningPartitionedDrivers = 0;
        int blockedDrivers = 0;
        int completedDrivers = this.completedDrivers.get();

        Distribution queuedTime = new Distribution(this.queuedTime);
        Distribution elapsedTime = new Distribution(this.elapsedTime);

        long totalScheduledTime = this.totalScheduledTime.get();
        long totalCpuTime = this.totalCpuTime.get();
        long totalUserTime = this.totalUserTime.get();
        long totalBlockedTime = this.totalBlockedTime.get();

        long rawInputDataSize = this.rawInputDataSize.getTotalCount();
        long rawInputPositions = this.rawInputPositions.getTotalCount();

        long processedInputDataSize = this.processedInputDataSize.getTotalCount();
        long processedInputPositions = this.processedInputPositions.getTotalCount();

        long outputDataSize = this.outputDataSize.getTotalCount();
        long outputPositions = this.outputPositions.getTotalCount();

        List<DriverStats> drivers = new ArrayList<>();

        Multimap<Integer, OperatorStats> runningOperators = ArrayListMultimap.create();
        for (DriverContext driverContext : driverContexts) {
            DriverStats driverStats = driverContext.getDriverStats();
            drivers.add(driverStats);

            if (driverStats.getStartTime() == null) {
                queuedDrivers++;
                if (driverContext.isPartitioned()) {
                    queuedPartitionedDrivers++;
                }
            }
            else if (driverStats.isFullyBlocked()) {
                blockedDrivers++;
            }
            else {
                runningDrivers++;
                if (driverContext.isPartitioned()) {
                    runningPartitionedDrivers++;
                }
            }

            queuedTime.add(driverStats.getQueuedTime().roundTo(NANOSECONDS));
            elapsedTime.add(driverStats.getElapsedTime().roundTo(NANOSECONDS));

            totalScheduledTime += driverStats.getTotalScheduledTime().roundTo(NANOSECONDS);
            totalCpuTime += driverStats.getTotalCpuTime().roundTo(NANOSECONDS);
            totalUserTime += driverStats.getTotalUserTime().roundTo(NANOSECONDS);
            totalBlockedTime += driverStats.getTotalBlockedTime().roundTo(NANOSECONDS);

            List<OperatorStats> operators = ImmutableList.copyOf(transform(driverContext.getOperatorContexts(), OperatorContext::getOperatorStats));
            for (OperatorStats operator : operators) {
                runningOperators.put(operator.getOperatorId(), operator);
            }

            rawInputDataSize += driverStats.getRawInputDataSize().toBytes();
            rawInputPositions += driverStats.getRawInputPositions();

            processedInputDataSize += driverStats.getProcessedInputDataSize().toBytes();
            processedInputPositions += driverStats.getProcessedInputPositions();

            outputDataSize += driverStats.getOutputDataSize().toBytes();
            outputPositions += driverStats.getOutputPositions();
        }

        // merge the running operator stats into the operator summary
        TreeMap<Integer, OperatorStats> operatorSummaries = new TreeMap<>(this.operatorSummaries);
        for (Entry<Integer, OperatorStats> entry : runningOperators.entries()) {
            OperatorStats current = operatorSummaries.get(entry.getKey());
            if (current == null) {
                current = entry.getValue();
            }
            else {
                current = current.add(entry.getValue());
            }
            operatorSummaries.put(entry.getKey(), current);
        }

        Set<DriverStats> runningDriverStats = drivers.stream()
                .filter(driver -> driver.getEndTime() == null && driver.getStartTime() != null)
                .collect(toImmutableSet());
        ImmutableSet<BlockedReason> blockedReasons = runningDriverStats.stream()
                .flatMap(driver -> driver.getBlockedReasons().stream())
                .collect(toImmutableSet());

        boolean fullyBlocked = !runningDriverStats.isEmpty() && runningDriverStats.stream().allMatch(DriverStats::isFullyBlocked);

        long userMemory;
        long revocableMemory;
        synchronized (this) {
            userMemory = pipelineMemoryContext.reservedUserMemory();
            revocableMemory = pipelineMemoryContext.reservedRevocableMemory();
        }

        return new PipelineStats(
                pipelineId,

                executionStartTime.get(),
                lastExecutionStartTime.get(),
                lastExecutionEndTime.get(),

                inputPipeline,
                outputPipeline,

                totalDriers,
                queuedDrivers,
                queuedPartitionedDrivers,
                runningDrivers,
                runningPartitionedDrivers,
                blockedDrivers,
                completedDrivers,

                succinctBytes(userMemory),
                succinctBytes(revocableMemory),

                queuedTime.snapshot(),
                elapsedTime.snapshot(),

                new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalUserTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                fullyBlocked,
                blockedReasons,

                succinctBytes(rawInputDataSize),
                rawInputPositions,

                succinctBytes(processedInputDataSize),
                processedInputPositions,

                succinctBytes(outputDataSize),
                outputPositions,

                ImmutableList.copyOf(operatorSummaries.values()),
                drivers);
    }

    public <C, R> R accept(QueryContextVisitor<C, R> visitor, C context)
    {
        return visitor.visitPipelineContext(this, context);
    }

    public <C, R> List<R> acceptChildren(QueryContextVisitor<C, R> visitor, C context)
    {
        return drivers.stream()
                .map(driver -> driver.accept(visitor, context))
                .collect(toList());
    }

    private static <K, V> boolean compareAndSet(ConcurrentMap<K, V> map, K key, V oldValue, V newValue)
    {
        if (oldValue == null) {
            return map.putIfAbsent(key, newValue) == null;
        }

        return map.replace(key, oldValue, newValue);
    }

    @VisibleForTesting
    public MemoryTrackingContext getPipelineMemoryContext()
    {
        return pipelineMemoryContext;
    }
}
