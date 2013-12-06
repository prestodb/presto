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

import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.operator.PipelineContext.pipelineStatsGetter;
import static com.facebook.presto.util.Threads.checkNotSameThreadExecutor;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class TaskContext
{
    private final TaskStateMachine taskStateMachine;
    private final Executor executor;
    private final Session session;

    private final long maxMemory;
    private final DataSize operatorPreAllocatedMemory;

    private final AtomicLong memoryReservation = new AtomicLong();

    private final DateTime createdTime = DateTime.now();
    private final long createNanos = System.nanoTime();

    private final AtomicLong startNanos = new AtomicLong();
    private final AtomicLong endNanos = new AtomicLong();

    private final AtomicReference<DateTime> executionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> lastExecutionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> executionEndTime = new AtomicReference<>();

    private final SetMultimap<PlanNodeId, Object> outputItems = Multimaps.synchronizedSetMultimap(HashMultimap.<PlanNodeId, Object>create());

    private final List<PipelineContext> pipelineContexts = new CopyOnWriteArrayList<>();

    private final boolean cpuTimerEnabled;

    public TaskContext(TaskId taskId, Executor executor, Session session)
    {
        this(
                checkNotNull(taskId, "taskId is null"),
                checkNotSameThreadExecutor(executor, "executor is null"),
                session,
                new DataSize(256, MEGABYTE));
    }

    public TaskContext(TaskId taskId, Executor executor, Session session, DataSize maxMemory)
    {
        this(
                new TaskStateMachine(checkNotNull(taskId, "taskId is null"), checkNotSameThreadExecutor(executor, "executor is null")),
                executor,
                session,
                checkNotNull(maxMemory, "maxMemory is null"),
                new DataSize(1, MEGABYTE),
                true);
    }

    public TaskContext(TaskStateMachine taskStateMachine, Executor executor, Session session, DataSize maxMemory, DataSize operatorPreAllocatedMemory, boolean cpuTimerEnabled)
    {
        this.taskStateMachine = checkNotNull(taskStateMachine, "taskStateMachine is null");
        this.executor = checkNotNull(executor, "executor is null");
        this.session = session;
        this.maxMemory = checkNotNull(maxMemory, "maxMemory is null").toBytes();
        this.operatorPreAllocatedMemory = checkNotNull(operatorPreAllocatedMemory, "operatorPreAllocatedMemory is null");

        taskStateMachine.addStateChangeListener(new StateChangeListener<TaskState>()
        {
            @Override
            public void stateChanged(TaskState newValue)
            {
                if (newValue.isDone()) {
                    executionEndTime.set(DateTime.now());
                    endNanos.set(System.nanoTime());
                }
            }
        });

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

    public List<PipelineContext> getPipelineContexts()
    {
        return ImmutableList.copyOf(pipelineContexts);
    }

    public Session getSession()
    {
        return session;
    }

    public void start()
    {
        if (!startNanos.compareAndSet(0, System.nanoTime())) {
            // already started
            return;
        }
        DateTime now = DateTime.now();
        executionStartTime.compareAndSet(null, now);
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

    public DataSize getMaxMemorySize()
    {
        return new DataSize(maxMemory, BYTE).convertToMostSuccinctDataSize();
    }

    public DataSize getOperatorPreAllocatedMemory()
    {
        return operatorPreAllocatedMemory;
    }

    public synchronized boolean reserveMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        if (memoryReservation.get() + bytes > maxMemory) {
            return false;
        }
        memoryReservation.getAndAdd(bytes);
        return true;
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

    @Deprecated
    public Map<PlanNodeId, Set<?>> getOutputItems()
    {
        // generics are broken
        return (Map<PlanNodeId, Set<?>>) (Object) outputItems.asMap();
    }

    @Deprecated
    public void addOutputItems(PlanNodeId id, Iterable<?> outputItems)
    {
        checkNotNull(id, "id is null");
        checkNotNull(outputItems, "outputItems is null");

        this.outputItems.putAll(id, outputItems);
    }

    public TaskStats getTaskStats()
    {
        // check for end state to avoid callback ordering problems
        if (taskStateMachine.getState().isDone()) {
            DateTime now = DateTime.now();
            if (executionEndTime.compareAndSet(null, now)) {
                lastExecutionStartTime.set(now);
                endNanos.set(System.nanoTime());
            }
        }

        List<PipelineStats> pipelineStats = ImmutableList.copyOf(transform(pipelineContexts, pipelineStatsGetter()));

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
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
            runningDrivers += pipeline.getRunningDrivers();
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

        return new TaskStats(
                createdTime,
                executionStartTime.get(),
                lastExecutionStartTime.get(),
                executionEndTime.get(),
                elapsedTime.convertToMostSuccinctTimeUnit(),
                queuedTime.convertToMostSuccinctTimeUnit(),
                totalDrivers,
                queuedDrivers,
                runningDrivers,
                completedDrivers,
                new DataSize(memoryReservation.get(), BYTE).convertToMostSuccinctDataSize(),
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
                pipelineStats);
    }

    public static Function<TaskContext, TaskStats> taskStatsGetter()
    {
        return new Function<TaskContext, TaskStats>()
        {
            public TaskStats apply(TaskContext taskContext)
            {
                return taskContext.getTaskStats();
            }
        };
    }
}
