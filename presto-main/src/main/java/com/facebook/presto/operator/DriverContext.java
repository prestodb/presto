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
import com.facebook.presto.spi.Session;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.operator.OperatorContext.operatorStatsGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class DriverContext
{
    private final PipelineContext pipelineContext;
    private final Executor executor;

    private final AtomicBoolean finished = new AtomicBoolean();

    private final DateTime createdTime = DateTime.now();
    private final long createNanos = System.nanoTime();

    private final AtomicLong startNanos = new AtomicLong();
    private final AtomicLong endNanos = new AtomicLong();

    private final AtomicReference<DateTime> executionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> executionEndTime = new AtomicReference<>();

    private final AtomicLong memoryReservation = new AtomicLong();

    private final List<OperatorContext> operatorContexts = new CopyOnWriteArrayList<>();

    public DriverContext(PipelineContext pipelineContext, Executor executor)
    {
        this.pipelineContext = checkNotNull(pipelineContext, "pipelineContext is null");
        this.executor = checkNotNull(executor, "executor is null");
    }

    public TaskId getTaskId()
    {
        return pipelineContext.getTaskId();
    }

    public OperatorContext addOperatorContext(int operatorId, String operatorType)
    {
        checkArgument(operatorId >= 0, "operatorId is negative");

        for (OperatorContext operatorContext : operatorContexts) {
            checkArgument(operatorId != operatorContext.getOperatorId(), "A context already exists for operatorId %s", operatorId);
        }

        OperatorContext operatorContext = new OperatorContext(operatorId, operatorType, this, executor);
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

    public void start()
    {
        if (!startNanos.compareAndSet(0, System.nanoTime())) {
            // already started
            return;
        }
        pipelineContext.start();
        executionStartTime.set(DateTime.now());
    }

    public void finished()
    {
        if (!finished.compareAndSet(false, true)) {
            // already finished
            return;
        }
        executionEndTime.set(DateTime.now());
        endNanos.set(System.nanoTime());

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

    public DataSize getMaxMemorySize()
    {
        return pipelineContext.getMaxMemorySize();
    }

    public DataSize getOperatorPreAllocatedMemory()
    {
        return pipelineContext.getMaxMemorySize();
    }

    public boolean reserveMemory(long bytes)
    {
        boolean result = pipelineContext.reserveMemory(bytes);
        if (result) {
            memoryReservation.getAndAdd(bytes);
        }
        return result;
    }

    public boolean isCpuTimerEnabled()
    {
        return pipelineContext.isCpuTimerEnabled();
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

    public DriverStats getDriverStats()
    {
        long totalScheduledTime = 0;
        long totalCpuTime = 0;
        long totalUserTime = 0;
        long totalBlockedTime = 0;

        List<OperatorStats> operators = ImmutableList.copyOf(transform(operatorContexts, operatorStatsGetter()));
        for (OperatorStats operator : operators) {
            totalScheduledTime += operator.getGetOutputWall().roundTo(NANOSECONDS);
            totalCpuTime += operator.getGetOutputCpu().roundTo(NANOSECONDS);
            totalUserTime += operator.getGetOutputUser().roundTo(NANOSECONDS);

            totalScheduledTime += operator.getAddInputWall().roundTo(NANOSECONDS);
            totalCpuTime += operator.getAddInputCpu().roundTo(NANOSECONDS);
            totalUserTime += operator.getAddInputUser().roundTo(NANOSECONDS);

            totalScheduledTime += operator.getFinishWall().roundTo(NANOSECONDS);
            totalCpuTime += operator.getFinishCpu().roundTo(NANOSECONDS);
            totalUserTime += operator.getFinishUser().roundTo(NANOSECONDS);

            totalBlockedTime += operator.getBlockedWall().roundTo(NANOSECONDS);
        }

        OperatorStats inputOperator = getFirst(operators, null);
        DataSize rawInputDataSize;
        long rawInputPositions;
        Duration rawInputReadTime;
        DataSize processedInputDataSize;
        long processedInputPositions;
        DataSize outputDataSize;
        long outputPositions;
        if (inputOperator != null) {
            rawInputDataSize = inputOperator.getInputDataSize();
            rawInputPositions = inputOperator.getInputPositions();
            rawInputReadTime = inputOperator.getAddInputWall();

            processedInputDataSize = inputOperator.getOutputDataSize();
            processedInputPositions = inputOperator.getOutputPositions();

            OperatorStats outputOperator = checkNotNull(getLast(operators, null));
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

        return new DriverStats(
                createdTime,
                executionStartTime.get(),
                executionEndTime.get(),
                queuedTime.convertToMostSuccinctTimeUnit(),
                elapsedTime.convertToMostSuccinctTimeUnit(),
                new DataSize(memoryReservation.get(), BYTE).convertToMostSuccinctDataSize(),
                new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalUserTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                rawInputDataSize.convertToMostSuccinctDataSize(),
                rawInputPositions,
                rawInputReadTime,
                processedInputDataSize.convertToMostSuccinctDataSize(),
                processedInputPositions,
                outputDataSize.convertToMostSuccinctDataSize(),
                outputPositions,
                ImmutableList.copyOf(Iterables.transform(operatorContexts, operatorStatsGetter())));
    }

    public static Function<DriverContext, DriverStats> driverStatsGetter()
    {
        return new Function<DriverContext, DriverStats>()
        {
            public DriverStats apply(DriverContext driverContext)
            {
                return driverContext.getDriverStats();
            }
        };
    }
}
