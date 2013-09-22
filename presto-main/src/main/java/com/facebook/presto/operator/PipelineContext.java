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

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

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

import static com.facebook.presto.operator.OperatorContext.operatorStatsGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.units.DataSize.Unit.BYTE;
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

    private final Distribution queuedTime = new Distribution();
    private final Distribution elapsedTime = new Distribution();

    private final AtomicLong totalScheduledTime = new AtomicLong();
    private final AtomicLong totalCpuTime = new AtomicLong();
    private final AtomicLong totalUserTime = new AtomicLong();
    private final AtomicLong totalBlockedTime = new AtomicLong();

    private final AtomicLong rawInputDataSize = new AtomicLong();
    private final AtomicLong rawInputPositions = new AtomicLong();

    private final AtomicLong processedInputDataSize = new AtomicLong();
    private final AtomicLong processedInputPositions = new AtomicLong();

    private final AtomicLong outputDataSize = new AtomicLong();
    private final AtomicLong outputPositions = new AtomicLong();

    private final ConcurrentMap<Integer, OperatorStats> operatorSummaries = new ConcurrentHashMap<>();

    public PipelineContext(TaskContext taskContext, Executor executor, boolean inputPipeline, boolean outputPipeline)
    {
        this.inputPipeline = inputPipeline;
        this.outputPipeline = outputPipeline;
        this.taskContext = checkNotNull(taskContext, "taskContext is null");
        this.executor = checkNotNull(executor, "executor is null");
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
        DriverContext driverContext = new DriverContext(this, executor);
        drivers.add(driverContext);
        return driverContext;
    }

    public List<DriverContext> getDrivers()
    {
        return ImmutableList.copyOf(drivers);
    }

    public Session getSession()
    {
        return taskContext.getSession();
    }

    public void driverFinished(DriverContext driverContext)
    {
        checkNotNull(driverContext, "driverContext is null");

        if (!drivers.remove(driverContext)) {
            throw new IllegalArgumentException("Unknown driver " + driverContext);
        }

        DriverStats driverStats = driverContext.getDriverStats();

        completedDrivers.getAndIncrement();

        // remove the memory reservation
        memoryReservation.getAndAdd(-driverStats.getMemoryReservation().toBytes());

        queuedTime.add(driverStats.getQueuedTime().roundTo(NANOSECONDS));
        elapsedTime.add(driverStats.getElapsedTime().roundTo(NANOSECONDS));

        totalScheduledTime.getAndAdd((driverStats.getTotalScheduledTime().roundTo(NANOSECONDS)));
        totalCpuTime.getAndAdd((driverStats.getTotalCpuTime().roundTo(NANOSECONDS)));
        totalUserTime.getAndAdd((driverStats.getTotalUserTime().roundTo(NANOSECONDS)));

        totalBlockedTime.getAndAdd((driverStats.getTotalBlockedTime().roundTo(NANOSECONDS)));

        // merge the operator stats into the operator summary
        List<OperatorStats> operators = driverStats.getOperatorStats();
        for (OperatorStats operator : operators) {
            OperatorStats operatorSummary = operatorSummaries.get(operator.getOperatorId());
            if (operatorSummary != null) {
                operatorSummary = operatorSummary.add(operator);
            }
            else {
                operatorSummary = operator;
            }
            operatorSummaries.put(operator.getOperatorId(), operatorSummary);
        }

        rawInputDataSize.getAndAdd(driverStats.getRawInputDataSize().toBytes());
        rawInputPositions.getAndAdd(driverStats.getRawInputPositions());

        processedInputDataSize.getAndAdd(driverStats.getProcessedInputDataSize().toBytes());
        processedInputPositions.getAndAdd(driverStats.getProcessedInputPositions());

        outputDataSize.getAndAdd(driverStats.getOutputDataSize().toBytes());
        outputPositions.getAndAdd(driverStats.getOutputPositions());
    }

    public void start()
    {
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

    public DataSize getMaxMemorySize()
    {
        return taskContext.getMaxMemorySize();
    }

    public DataSize getOperatorPreAllocatedMemory()
    {
        return taskContext.getOperatorPreAllocatedMemory();
    }

    public synchronized boolean reserveMemory(long bytes)
    {
        boolean result = taskContext.reserveMemory(bytes);
        if (result) {
            memoryReservation.getAndAdd((bytes));
        }
        return result;
    }

    public boolean isCpuTimerEnabled()
    {
        return taskContext.isCpuTimerEnabled();
    }

    @Deprecated
    public void addOutputItems(PlanNodeId id, Iterable<?> outputItems)
    {
        taskContext.addOutputItems(id, outputItems);
    }

    public PipelineStats getPipelineStats()
    {
        List<DriverContext> driverContexts = ImmutableList.copyOf(this.drivers);

        int totalDriers = completedDrivers.get() + driverContexts.size();
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int completedDrivers = this.completedDrivers.get();

        Distribution queuedTime = new Distribution(this.queuedTime);
        Distribution elapsedTime = new Distribution(this.elapsedTime);

        long totalScheduledTime = this.totalScheduledTime.get();
        long totalCpuTime = this.totalCpuTime.get();
        long totalUserTime = this.totalUserTime.get();
        long totalBlockedTime = this.totalBlockedTime.get();

        long rawInputDataSize = this.rawInputDataSize.get();
        long rawInputPositions = this.rawInputPositions.get();

        long processedInputDataSize = this.processedInputDataSize.get();
        long processedInputPositions = this.processedInputPositions.get();

        long outputDataSize = this.outputDataSize.get();
        long outputPositions = this.outputPositions.get();

        List<DriverStats> drivers = new ArrayList<>();

        Multimap<Integer, OperatorStats> runningOperators = ArrayListMultimap.create();
        for (DriverContext driverContext : driverContexts) {
            DriverStats driverStats = driverContext.getDriverStats();
            drivers.add(driverStats);

            if (driverStats.getStartTime() == null) {
                queuedDrivers++;
            }
            else {
                runningDrivers++;
            }

            queuedTime.add(driverStats.getQueuedTime().roundTo(NANOSECONDS));
            elapsedTime.add(driverStats.getElapsedTime().roundTo(NANOSECONDS));

            totalScheduledTime += driverStats.getTotalScheduledTime().roundTo(NANOSECONDS);
            totalCpuTime += driverStats.getTotalCpuTime().roundTo(NANOSECONDS);
            totalUserTime += driverStats.getTotalUserTime().roundTo(NANOSECONDS);
            totalBlockedTime += driverStats.getTotalBlockedTime().roundTo(NANOSECONDS);

            List<OperatorStats> operators = ImmutableList.copyOf(transform(driverContext.getOperatorContexts(), operatorStatsGetter()));
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

        // merge the operator stats into the operator summary
        TreeMap<Integer, OperatorStats> operatorSummaries = new TreeMap<>();
        for (Entry<Integer, OperatorStats> entry : this.operatorSummaries.entrySet()) {
            OperatorStats operator = entry.getValue();
            operator.add(runningOperators.get(entry.getKey()));
            operatorSummaries.put(entry.getKey(), operator);
        }

        return new PipelineStats(
                inputPipeline,
                outputPipeline,

                totalDriers,
                queuedDrivers,
                runningDrivers,
                completedDrivers,

                new DataSize(memoryReservation.get(), BYTE).convertToMostSuccinctDataSize(),

                queuedTime.snapshot(),
                elapsedTime.snapshot(),

                new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalUserTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),

                new DataSize(rawInputDataSize, BYTE).convertToMostSuccinctDataSize(),
                rawInputPositions,

                new DataSize(processedInputDataSize, BYTE).convertToMostSuccinctDataSize(),
                processedInputPositions,

                new DataSize(outputDataSize, BYTE).convertToMostSuccinctDataSize(),
                outputPositions,

                ImmutableList.copyOf(operatorSummaries.values()),
                drivers);
    }

    public static Function<PipelineContext, PipelineStats> pipelineStatsGetter()
    {
        return new Function<PipelineContext, PipelineStats>()
        {
            public PipelineStats apply(PipelineContext pipelineContext)
            {
                return pipelineContext.getPipelineStats();
            }
        };
    }
}
