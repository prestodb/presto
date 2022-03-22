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

import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.Session;
import com.facebook.presto.execution.FragmentResultCacheContext;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.memory.QueryContextVisitor;
import com.facebook.presto.memory.context.MemoryTrackingContext;
import com.facebook.presto.operator.OperationTimer.OperationTiming;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Only calling getDriverStats is ThreadSafe
 */
public class DriverContext
{
    private final PipelineContext pipelineContext;
    private final Executor notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;

    private final AtomicBoolean finished = new AtomicBoolean();

    private final DateTime createdTime = DateTime.now();
    private final long createNanos = System.nanoTime();

    private final AtomicLong startNanos = new AtomicLong();
    private final AtomicLong endNanos = new AtomicLong();

    private final OperationTiming overallTiming = new OperationTiming();

    private final AtomicReference<BlockedMonitor> blockedMonitor = new AtomicReference<>();
    private final AtomicLong blockedWallNanos = new AtomicLong();

    private final AtomicReference<DateTime> executionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> executionEndTime = new AtomicReference<>();

    private final MemoryTrackingContext driverMemoryContext;

    private final DriverYieldSignal yieldSignal;

    private final List<OperatorContext> operatorContexts = new CopyOnWriteArrayList<>();
    private final Lifespan lifespan;
    private final Optional<FragmentResultCacheContext> fragmentResultCacheContext;

    public DriverContext(
            PipelineContext pipelineContext,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            MemoryTrackingContext driverMemoryContext,
            Lifespan lifespan,
            Optional<FragmentResultCacheContext> fragmentResultCacheContext)
    {
        this.pipelineContext = requireNonNull(pipelineContext, "pipelineContext is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.yieldExecutor = requireNonNull(yieldExecutor, "scheduler is null");
        this.driverMemoryContext = requireNonNull(driverMemoryContext, "driverMemoryContext is null");
        this.lifespan = requireNonNull(lifespan, "lifespan is null");
        this.fragmentResultCacheContext = requireNonNull(fragmentResultCacheContext, "fragmentResultCacheContext is null");
        this.yieldSignal = new DriverYieldSignal();
    }

    public TaskId getTaskId()
    {
        return pipelineContext.getTaskId();
    }

    public OperatorContext addOperatorContext(int operatorId, PlanNodeId planNodeId, String operatorType)
    {
        checkArgument(operatorId >= 0, "operatorId is negative");

        for (OperatorContext operatorContext : operatorContexts) {
            checkArgument(operatorId != operatorContext.getOperatorId(), "A context already exists for operatorId %s", operatorId);
        }

        OperatorContext operatorContext = new OperatorContext(
                operatorId,
                planNodeId,
                operatorType,
                this,
                notificationExecutor,
                driverMemoryContext.newMemoryTrackingContext());
        operatorContexts.add(operatorContext);
        return operatorContext;
    }

    public List<OperatorContext> getOperatorContexts()
    {
        return ImmutableList.copyOf(operatorContexts);
    }

    public PipelineContext getPipelineContext()
    {
        return pipelineContext;
    }

    public Session getSession()
    {
        return pipelineContext.getSession();
    }

    public void startProcessTimer()
    {
        // Must update startNanos first so that the value is valid once executionStartTime is not null
        if (executionStartTime.get() == null && startNanos.compareAndSet(0, System.nanoTime())) {
            executionStartTime.set(DateTime.now());
            pipelineContext.start();
        }
    }

    public void recordProcessed(OperationTimer operationTimer)
    {
        operationTimer.end(overallTiming);
    }

    public void recordBlocked(ListenableFuture<?> blocked)
    {
        requireNonNull(blocked, "blocked is null");

        BlockedMonitor monitor = new BlockedMonitor();

        BlockedMonitor oldMonitor = blockedMonitor.getAndSet(monitor);
        if (oldMonitor != null) {
            oldMonitor.run();
        }

        blocked.addListener(monitor, notificationExecutor);
    }

    public void finished()
    {
        if (!finished.compareAndSet(false, true)) {
            // already finished
            return;
        }
        // Must update endNanos first, so that the value is valid after executionEndTime is not null
        endNanos.set(System.nanoTime());
        executionEndTime.set(DateTime.now());

        pipelineContext.driverFinished(this);
    }

    public void failed(Throwable cause)
    {
        pipelineContext.failed(cause);
        finished.set(true);
    }

    public boolean isDone()
    {
        return finished.get() || pipelineContext.isDone();
    }

    public ListenableFuture<?> reserveSpill(long bytes)
    {
        return pipelineContext.reserveSpill(bytes);
    }

    public void freeSpill(long bytes)
    {
        if (bytes == 0) {
            return;
        }
        checkArgument(bytes > 0, "bytes is negative");
        pipelineContext.freeSpill(bytes);
    }

    public DriverYieldSignal getYieldSignal()
    {
        return yieldSignal;
    }

    public long getSystemMemoryUsage()
    {
        return driverMemoryContext.getSystemMemory();
    }

    public long getMemoryUsage()
    {
        return driverMemoryContext.getUserMemory();
    }

    public long getRevocableMemoryUsage()
    {
        return driverMemoryContext.getRevocableMemory();
    }

    public void moreMemoryAvailable()
    {
        operatorContexts.forEach(OperatorContext::moreMemoryAvailable);
    }

    public boolean isPerOperatorCpuTimerEnabled()
    {
        return pipelineContext.isPerOperatorCpuTimerEnabled();
    }

    public boolean isCpuTimerEnabled()
    {
        return pipelineContext.isCpuTimerEnabled();
    }

    public boolean isPerOperatorAllocationTrackingEnabled()
    {
        return pipelineContext.isPerOperatorAllocationTrackingEnabled();
    }

    public boolean isAllocationTrackingEnabled()
    {
        return pipelineContext.isAllocationTrackingEnabled();
    }

    public CounterStat getInputDataSize()
    {
        OperatorContext inputOperator = getFirst(operatorContexts, null);
        if (inputOperator != null) {
            return inputOperator.getInputDataSize();
        }
        else {
            return new CounterStat();
        }
    }

    public CounterStat getInputPositions()
    {
        OperatorContext inputOperator = getFirst(operatorContexts, null);
        if (inputOperator != null) {
            return inputOperator.getInputPositions();
        }
        else {
            return new CounterStat();
        }
    }

    public CounterStat getOutputDataSize()
    {
        OperatorContext inputOperator = getLast(operatorContexts, null);
        if (inputOperator != null) {
            return inputOperator.getOutputDataSize();
        }
        else {
            return new CounterStat();
        }
    }

    public CounterStat getOutputPositions()
    {
        OperatorContext inputOperator = getLast(operatorContexts, null);
        if (inputOperator != null) {
            return inputOperator.getOutputPositions();
        }
        else {
            return new CounterStat();
        }
    }

    public long getPphysicalWrittenDataSize()
    {
        return operatorContexts.stream()
                .mapToLong(OperatorContext::getPhysicalWrittenDataSize)
                .sum();
    }

    public boolean isExecutionStarted()
    {
        return executionStartTime.get() != null;
    }

    public boolean isFullyBlocked()
    {
        return blockedMonitor.get() != null;
    }

    public DriverStats getDriverStats()
    {
        long totalScheduledTime = overallTiming.getWallNanos();
        long totalCpuTime = overallTiming.getCpuNanos();
        long totalAllocation = overallTiming.getAllocationBytes();

        long totalBlockedTime = blockedWallNanos.get();
        BlockedMonitor blockedMonitor = this.blockedMonitor.get();
        if (blockedMonitor != null) {
            totalBlockedTime += blockedMonitor.getBlockedTime();
        }

        // startNanos is always valid once executionStartTime is not null
        DateTime executionStartTime = this.executionStartTime.get();
        Duration queuedTime = new Duration(nanosBetween(createNanos, executionStartTime == null ? System.nanoTime() : startNanos.get()), NANOSECONDS);

        // endNanos is always valid once executionStartTime is not null
        DateTime executionEndTime = this.executionEndTime.get();
        Duration elapsedTime = new Duration(nanosBetween(createNanos, executionEndTime == null ? System.nanoTime() : endNanos.get()), NANOSECONDS);

        List<OperatorStats> operators = ImmutableList.copyOf(transform(operatorContexts, OperatorContext::getOperatorStats));
        OperatorStats inputOperator = getFirst(operators, null);
        DataSize rawInputDataSize;
        long rawInputPositions;
        Duration rawInputReadTime;
        DataSize processedInputDataSize;
        long processedInputPositions;
        DataSize outputDataSize;
        long outputPositions;
        if (inputOperator != null) {
            rawInputDataSize = inputOperator.getRawInputDataSize();
            rawInputPositions = inputOperator.getInputPositions();
            rawInputReadTime = inputOperator.getAddInputWall();

            processedInputDataSize = inputOperator.getInputDataSize();
            processedInputPositions = inputOperator.getInputPositions();

            OperatorStats outputOperator = requireNonNull(getLast(operators, null));
            outputDataSize = outputOperator.getOutputDataSize();
            outputPositions = outputOperator.getOutputPositions();
        }
        else {
            rawInputDataSize = new DataSize(0, BYTE);
            rawInputPositions = 0;
            rawInputReadTime = new Duration(0, MILLISECONDS);

            processedInputDataSize = new DataSize(0, BYTE);
            processedInputPositions = 0;

            outputDataSize = new DataSize(0, BYTE);
            outputPositions = 0;
        }

        ImmutableSet.Builder<BlockedReason> builder = ImmutableSet.builder();
        long physicalWrittenDataSize = 0;
        for (OperatorStats operator : operators) {
            physicalWrittenDataSize += operator.getPhysicalWrittenDataSize().toBytes();
            if (operator.getBlockedReason().isPresent()) {
                builder.add(operator.getBlockedReason().get());
            }
            totalCpuTime += operator.getAdditionalCpu().roundTo(NANOSECONDS);
        }

        return new DriverStats(
                lifespan,
                createdTime,
                executionStartTime,
                executionEndTime,
                queuedTime.convertToMostSuccinctTimeUnit(),
                elapsedTime.convertToMostSuccinctTimeUnit(),
                succinctBytes(driverMemoryContext.getUserMemory()),
                succinctBytes(driverMemoryContext.getRevocableMemory()),
                succinctBytes(driverMemoryContext.getSystemMemory()),
                succinctNanos(totalScheduledTime),
                succinctNanos(totalCpuTime),
                succinctNanos(totalBlockedTime),
                blockedMonitor != null,
                builder.build(),
                succinctBytes(totalAllocation),
                rawInputDataSize.convertToMostSuccinctDataSize(),
                rawInputPositions,
                rawInputReadTime,
                processedInputDataSize.convertToMostSuccinctDataSize(),
                processedInputPositions,
                outputDataSize.convertToMostSuccinctDataSize(),
                outputPositions,
                succinctBytes(physicalWrittenDataSize),
                operators);
    }

    public <C, R> R accept(QueryContextVisitor<C, R> visitor, C context)
    {
        return visitor.visitDriverContext(this, context);
    }

    public <C, R> List<R> acceptChildren(QueryContextVisitor<C, R> visitor, C context)
    {
        return operatorContexts.stream()
                .map(operatorContext -> operatorContext.accept(visitor, context))
                .collect(toList());
    }

    public Lifespan getLifespan()
    {
        return lifespan;
    }

    public Optional<FragmentResultCacheContext> getFragmentResultCacheContext()
    {
        return fragmentResultCacheContext;
    }

    public ScheduledExecutorService getYieldExecutor()
    {
        return yieldExecutor;
    }

    private static long nanosBetween(long start, long end)
    {
        return max(0, end - start);
    }

    private class BlockedMonitor
            implements Runnable
    {
        private final long start = System.nanoTime();
        private boolean finished;

        @Override
        public void run()
        {
            synchronized (this) {
                if (finished) {
                    return;
                }
                finished = true;
                blockedMonitor.compareAndSet(this, null);
                blockedWallNanos.getAndAdd(getBlockedTime());
            }
        }

        public long getBlockedTime()
        {
            return nanosBetween(start, System.nanoTime());
        }
    }

    @VisibleForTesting
    public MemoryTrackingContext getDriverMemoryContext()
    {
        return driverMemoryContext;
    }

    @VisibleForTesting
    public void addOperatorContext(OperatorContext operatorContext)
    {
        operatorContexts.add(operatorContext);
    }
}
