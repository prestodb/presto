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
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.stats.Distribution;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class PipelineContext
{
    private final TaskContext taskContext;
    private final Executor executor;

    private final boolean inputPipeline;
    private final boolean outputPipeline;

    private final List<DriverContext> drivers = new CopyOnWriteArrayList<>();

    private final AtomicInteger completedDrivers = new AtomicInteger();

    private final AtomicLong memoryReservation = new AtomicLong();
    private final AtomicLong systemMemoryReservation = new AtomicLong();

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

    public PipelineContext(TaskContext taskContext, Executor executor, boolean inputPipeline, boolean outputPipeline)
    {
        this.inputPipeline = inputPipeline;
        this.outputPipeline = outputPipeline;
        this.taskContext = requireNonNull(taskContext, "taskContext is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    public TaskContext getTaskContext()
    {
        return taskContext;
    }

    public TaskId getTaskId()
    {
        return taskContext.getTaskId();
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
        DriverContext driverContext = new DriverContext(this, executor, partitioned);
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

    public void transferMemoryToTaskContext(long bytes)
    {
        // The memory is already reserved in the task context, so just decrement our reservation
        checkArgument(memoryReservation.addAndGet(-bytes) >= 0, "Tried to transfer more memory than is reserved");
    }

    public synchronized ListenableFuture<?> reserveMemory(long bytes)
    {
        ListenableFuture<?> future = taskContext.reserveMemory(bytes);
        memoryReservation.getAndAdd(bytes);
        return future;
    }

    public synchronized ListenableFuture<?> reserveSystemMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        ListenableFuture<?> future = taskContext.reserveSystemMemory(bytes);
        systemMemoryReservation.getAndAdd(bytes);
        return future;
    }

    public synchronized boolean tryReserveMemory(long bytes)
    {
        if (taskContext.tryReserveMemory(bytes)) {
            memoryReservation.getAndAdd(bytes);
            return true;
        }
        return false;
    }

    public synchronized void freeMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(bytes <= memoryReservation.get(), "tried to free more memory than is reserved");
        taskContext.freeMemory(bytes);
        memoryReservation.getAndAdd(-bytes);
    }

    public synchronized void freeSystemMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(bytes <= systemMemoryReservation.get(), "tried to free more memory than is reserved");
        taskContext.freeSystemMemory(bytes);
        systemMemoryReservation.getAndAdd(-bytes);
    }

    public void moreMemoryAvailable()
    {
        drivers.stream().forEach(DriverContext::moreMemoryAvailable);
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

        ImmutableSet<BlockedReason> blockedReasons = drivers.stream()
                .filter(driver -> driver.getEndTime() == null && driver.getStartTime() != null)
                .flatMap(driver -> driver.getBlockedReasons().stream())
                .collect(ImmutableCollectors.toImmutableSet());
        boolean fullyBlocked = drivers.stream()
                .filter(driver -> driver.getEndTime() == null && driver.getStartTime() != null)
                .allMatch(DriverStats::isFullyBlocked);
        return new PipelineStats(
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
                completedDrivers,

                succinctBytes(memoryReservation.get()),
                succinctBytes(systemMemoryReservation.get()),

                queuedTime.snapshot(),
                elapsedTime.snapshot(),

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

                ImmutableList.copyOf(operatorSummaries.values()),
                drivers);
    }

    private static <K, V> boolean compareAndSet(ConcurrentMap<K, V> map, K key, V oldValue, V newValue)
    {
        if (oldValue == null) {
            return map.putIfAbsent(key, newValue) == null;
        }

        return map.replace(key, oldValue, newValue);
    }
}
