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
import com.facebook.airlift.stats.GcMonitor;
import com.facebook.presto.Session;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskMetadataContext;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.execution.buffer.LazyOutputBuffer;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.memory.QueryContextVisitor;
import com.facebook.presto.memory.VoidTraversingQueryContextVisitor;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.memory.context.MemoryTrackingContext;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class TaskContext
{
    private final QueryContext queryContext;
    private final TaskStateMachine taskStateMachine;
    private final GcMonitor gcMonitor;
    private final Executor notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final Session session;

    private final long createNanos = System.nanoTime();

    private final AtomicLong startNanos = new AtomicLong();
    private final AtomicLong startFullGcCount = new AtomicLong(-1);
    private final AtomicLong startFullGcTimeNanos = new AtomicLong(-1);
    private final AtomicLong endNanos = new AtomicLong();
    private final AtomicLong endFullGcCount = new AtomicLong(-1);
    private final AtomicLong endFullGcTimeNanos = new AtomicLong(-1);

    private final AtomicReference<DateTime> executionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> lastExecutionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> executionEndTime = new AtomicReference<>();

    private final Set<Lifespan> completedDriverGroups = newConcurrentHashSet();

    private final List<PipelineContext> pipelineContexts = new CopyOnWriteArrayList<>();

    private final boolean perOperatorCpuTimerEnabled;
    private final boolean cpuTimerEnabled;

    private final boolean perOperatorAllocationTrackingEnabled;
    private final boolean allocationTrackingEnabled;

    private final boolean legacyLifespanCompletionCondition;

    private final Object cumulativeMemoryLock = new Object();
    private final AtomicDouble cumulativeUserMemory = new AtomicDouble(0.0);
    private final AtomicDouble cumulativeTotalMemory = new AtomicDouble(0.0);

    private final AtomicLong peakTotalMemoryInBytes = new AtomicLong(0);
    private final AtomicLong peakUserMemoryInBytes = new AtomicLong(0);

    @GuardedBy("cumulativeMemoryLock")
    private long lastUserMemoryReservation;

    @GuardedBy("cumulativeMemoryLock")
    private long lastTotalMemoryReservation;

    @GuardedBy("cumulativeMemoryLock")
    private long lastTaskStatCallNanos;

    private final MemoryTrackingContext taskMemoryContext;

    private final TaskMetadataContext taskMetadataContext;

    private final Optional<PlanNode> taskPlan;

    // Only contains metrics exposed in this task. Doesn't contain the metrics exposed in the operators.
    // This is merged with the operator metrics when generating the TaskStats in {@link #getTaskStats}.
    private final RuntimeStats runtimeStats = new RuntimeStats();

    public static TaskContext createTaskContext(
            QueryContext queryContext,
            TaskStateMachine taskStateMachine,
            GcMonitor gcMonitor,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            Session session,
            MemoryTrackingContext taskMemoryContext,
            Optional<PlanNode> taskPlan,
            boolean perOperatorCpuTimerEnabled,
            boolean cpuTimerEnabled,
            boolean perOperatorAllocationTrackingEnabled,
            boolean allocationTrackingEnabled,
            boolean legacyLifespanCompletionCondition)
    {
        TaskContext taskContext = new TaskContext(
                queryContext,
                taskStateMachine,
                gcMonitor,
                notificationExecutor,
                yieldExecutor,
                session,
                taskMemoryContext,
                taskPlan,
                perOperatorCpuTimerEnabled,
                cpuTimerEnabled,
                perOperatorAllocationTrackingEnabled,
                allocationTrackingEnabled,
                legacyLifespanCompletionCondition);
        taskContext.initialize();
        return taskContext;
    }

    private TaskContext(
            QueryContext queryContext,
            TaskStateMachine taskStateMachine,
            GcMonitor gcMonitor,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            Session session,
            MemoryTrackingContext taskMemoryContext,
            Optional<PlanNode> taskPlan,
            boolean perOperatorCpuTimerEnabled,
            boolean cpuTimerEnabled,
            boolean perOperatorAllocationTrackingEnabled,
            boolean allocationTrackingEnabled,
            boolean legacyLifespanCompletionCondition)
    {
        this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
        this.gcMonitor = requireNonNull(gcMonitor, "gcMonitor is null");
        this.queryContext = requireNonNull(queryContext, "queryContext is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.yieldExecutor = requireNonNull(yieldExecutor, "yieldExecutor is null");
        this.session = session;
        this.taskMemoryContext = requireNonNull(taskMemoryContext, "taskMemoryContext is null");
        this.taskPlan = requireNonNull(taskPlan, "taskPlan is null");
        // Initialize the local memory contexts with the LazyOutputBuffer tag as LazyOutputBuffer will do the local memory allocations
        taskMemoryContext.initializeLocalMemoryContexts(LazyOutputBuffer.class.getSimpleName());
        this.perOperatorCpuTimerEnabled = perOperatorCpuTimerEnabled;
        this.cpuTimerEnabled = cpuTimerEnabled;
        this.perOperatorAllocationTrackingEnabled = perOperatorAllocationTrackingEnabled;
        this.allocationTrackingEnabled = allocationTrackingEnabled;
        this.legacyLifespanCompletionCondition = legacyLifespanCompletionCondition;
        this.taskMetadataContext = new TaskMetadataContext();
    }

    // the state change listener is added here in a separate initialize() method
    // instead of the constructor to prevent leaking the "this" reference to
    // another thread, which will cause unsafe publication of this instance.
    private void initialize()
    {
        taskStateMachine.addStateChangeListener(this::updateStatsIfDone);
    }

    public TaskId getTaskId()
    {
        return taskStateMachine.getTaskId();
    }

    public PipelineContext addPipelineContext(int pipelineId, boolean inputPipeline, boolean outputPipeline, boolean partitioned)
    {
        PipelineContext pipelineContext = new PipelineContext(
                pipelineId,
                this,
                notificationExecutor,
                yieldExecutor,
                taskMemoryContext.newMemoryTrackingContext(),
                inputPipeline,
                outputPipeline,
                partitioned);
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
        startFullGcCount.compareAndSet(-1, gcMonitor.getMajorGcCount());
        startFullGcTimeNanos.compareAndSet(-1, gcMonitor.getMajorGcTime().roundTo(NANOSECONDS));

        // always update last execution start time
        lastExecutionStartTime.set(now);
    }

    private void updateStatsIfDone(TaskState newState)
    {
        if (newState.isDone()) {
            DateTime now = DateTime.now();
            long majorGcCount = gcMonitor.getMajorGcCount();
            long majorGcTime = gcMonitor.getMajorGcTime().roundTo(NANOSECONDS);

            // before setting the end times, make sure a start has been recorded
            executionStartTime.compareAndSet(null, now);
            startNanos.compareAndSet(0, System.nanoTime());
            startFullGcCount.compareAndSet(-1, majorGcCount);
            startFullGcTimeNanos.compareAndSet(-1, majorGcTime);

            // Only update last start time, if the nothing was started
            lastExecutionStartTime.compareAndSet(null, now);

            // use compare and set from initial value to avoid overwriting if there
            // were a duplicate notification, which shouldn't happen
            executionEndTime.compareAndSet(null, now);
            endNanos.compareAndSet(0, System.nanoTime());
            endFullGcCount.compareAndSet(-1, majorGcCount);
            endFullGcTimeNanos.compareAndSet(-1, majorGcTime);
        }
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

    public TaskMetadataContext getTaskMetadataContext()
    {
        return taskMetadataContext;
    }

    public DataSize getMemoryReservation()
    {
        return new DataSize(taskMemoryContext.getUserMemory(), BYTE);
    }

    public DataSize getSystemMemoryReservation()
    {
        return new DataSize(taskMemoryContext.getSystemMemory(), BYTE);
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

    public synchronized ListenableFuture<?> reserveSpill(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        return queryContext.reserveSpill(bytes);
    }

    public synchronized void freeSpill(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        queryContext.freeSpill(bytes);
    }

    public LocalMemoryContext localSystemMemoryContext()
    {
        return taskMemoryContext.localSystemMemoryContext();
    }

    public void moreMemoryAvailable()
    {
        pipelineContexts.forEach(PipelineContext::moreMemoryAvailable);
    }

    public boolean isPerOperatorAllocationTrackingEnabled()
    {
        return perOperatorAllocationTrackingEnabled;
    }

    public boolean isAllocationTrackingEnabled()
    {
        return allocationTrackingEnabled;
    }

    public boolean isPerOperatorCpuTimerEnabled()
    {
        return perOperatorCpuTimerEnabled;
    }

    public boolean isCpuTimerEnabled()
    {
        return cpuTimerEnabled;
    }

    public boolean isLegacyLifespanCompletionCondition()
    {
        return legacyLifespanCompletionCondition;
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

    public Duration getFullGcTime()
    {
        long startFullGcTimeNanos = this.startFullGcTimeNanos.get();
        if (startFullGcTimeNanos < 0) {
            return new Duration(0, MILLISECONDS);
        }

        long endFullGcTimeNanos = this.endFullGcTimeNanos.get();
        if (endFullGcTimeNanos < 0) {
            endFullGcTimeNanos = gcMonitor.getMajorGcTime().roundTo(NANOSECONDS);
        }
        return new Duration(max(0, endFullGcTimeNanos - startFullGcTimeNanos), NANOSECONDS);
    }

    public int getFullGcCount()
    {
        long startFullGcCount = this.startFullGcCount.get();
        if (startFullGcCount < 0) {
            return 0;
        }

        long endFullGcCount = this.endFullGcCount.get();
        if (endFullGcCount <= 0) {
            endFullGcCount = gcMonitor.getMajorGcCount();
        }
        return toIntExact(max(0, endFullGcCount - startFullGcCount));
    }

    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    public TaskStats getTaskStats()
    {
        // check for end state to avoid callback ordering problems
        updateStatsIfDone(taskStateMachine.getState());

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
        long totalBlockedTime = 0;

        long totalAllocation = 0;

        long rawInputDataSize = 0;
        long rawInputPositions = 0;

        long processedInputDataSize = 0;
        long processedInputPositions = 0;

        long outputDataSize = 0;
        long outputPositions = 0;

        long physicalWrittenDataSize = 0;
        RuntimeStats mergedRuntimeStats = RuntimeStats.copyOf(runtimeStats);

        ImmutableSet.Builder<BlockedReason> blockedReasons = ImmutableSet.builder();
        boolean hasRunningPipelines = false;
        boolean runningPipelinesFullyBlocked = true;

        for (PipelineStats pipeline : pipelineStats) {
            if (pipeline.getLastEndTime() != null) {
                lastExecutionEndTime = max(pipeline.getLastEndTime().getMillis(), lastExecutionEndTime);
            }
            if (pipeline.getRunningDrivers() > 0 || pipeline.getRunningPartitionedDrivers() > 0 || pipeline.getBlockedDrivers() > 0) {
                // pipeline is running
                hasRunningPipelines = true;
                runningPipelinesFullyBlocked &= pipeline.isFullyBlocked();
                blockedReasons.addAll(pipeline.getBlockedReasons());
            }

            totalDrivers += pipeline.getTotalDrivers();
            queuedDrivers += pipeline.getQueuedDrivers();
            queuedPartitionedDrivers += pipeline.getQueuedPartitionedDrivers();
            runningDrivers += pipeline.getRunningDrivers();
            runningPartitionedDrivers += pipeline.getRunningPartitionedDrivers();
            blockedDrivers += pipeline.getBlockedDrivers();
            completedDrivers += pipeline.getCompletedDrivers();

            totalScheduledTime += pipeline.getTotalScheduledTimeInNanos();
            totalCpuTime += pipeline.getTotalCpuTimeInNanos();
            totalBlockedTime += pipeline.getTotalBlockedTimeInNanos();

            totalAllocation += pipeline.getTotalAllocationInBytes();

            if (pipeline.isInputPipeline()) {
                rawInputDataSize += pipeline.getRawInputDataSizeInBytes();
                rawInputPositions += pipeline.getRawInputPositions();

                processedInputDataSize += pipeline.getProcessedInputDataSizeInBytes();
                processedInputPositions += pipeline.getProcessedInputPositions();
            }

            if (pipeline.isOutputPipeline()) {
                outputDataSize += pipeline.getOutputDataSizeInBytes();
                outputPositions += pipeline.getOutputPositions();
            }

            physicalWrittenDataSize += pipeline.getPhysicalWrittenDataSizeInBytes();
            pipeline.getOperatorSummaries().stream().forEach(stats -> mergedRuntimeStats.mergeWith(stats.getRuntimeStats()));
        }

        long startNanos = this.startNanos.get();
        if (startNanos == 0) {
            startNanos = System.nanoTime();
        }
        long queuedTimeInNanos = startNanos - createNanos;

        long endNanos = this.endNanos.get();
        long elapsedTimeInNanos;
        if (endNanos >= startNanos) {
            elapsedTimeInNanos = endNanos - createNanos;
        }
        else {
            elapsedTimeInNanos = 0;
        }

        int fullGcCount = getFullGcCount();
        Duration fullGcTime = getFullGcTime();

        long userMemory = taskMemoryContext.getUserMemory();
        long systemMemory = taskMemoryContext.getSystemMemory();

        updatePeakMemory();

        synchronized (cumulativeMemoryLock) {
            if (lastTaskStatCallNanos == 0) {
                lastTaskStatCallNanos = startNanos;
            }
            double sinceLastPeriodMillis = (System.nanoTime() - lastTaskStatCallNanos) / 1_000_000.0;
            long averageUserMemoryForLastPeriod = (userMemory + lastUserMemoryReservation) / 2;
            long averageTotalMemoryForLastPeriod = (userMemory + systemMemory + lastTotalMemoryReservation) / 2;
            cumulativeUserMemory.addAndGet(averageUserMemoryForLastPeriod * sinceLastPeriodMillis);
            cumulativeTotalMemory.addAndGet(averageTotalMemoryForLastPeriod * sinceLastPeriodMillis);

            lastTaskStatCallNanos = System.nanoTime();
            lastUserMemoryReservation = userMemory;
            lastTotalMemoryReservation = systemMemory + userMemory;
        }

        boolean fullyBlocked = hasRunningPipelines && runningPipelinesFullyBlocked;

        return new TaskStats(
                taskStateMachine.getCreatedTime(),
                executionStartTime.get(),
                lastExecutionStartTime.get(),
                lastExecutionEndTime == 0 ? null : new DateTime(lastExecutionEndTime),
                executionEndTime.get(),
                elapsedTimeInNanos,
                queuedTimeInNanos,
                totalDrivers,
                queuedDrivers,
                queuedPartitionedDrivers,
                runningDrivers,
                runningPartitionedDrivers,
                blockedDrivers,
                completedDrivers,
                cumulativeUserMemory.get(),
                cumulativeTotalMemory.get(),
                userMemory,
                taskMemoryContext.getRevocableMemory(),
                systemMemory,
                peakTotalMemoryInBytes.get(),
                peakUserMemoryInBytes.get(),
                queryContext.getPeakNodeTotalMemory(),
                totalScheduledTime,
                totalCpuTime,
                totalBlockedTime,
                fullyBlocked && (runningDrivers > 0 || runningPartitionedDrivers > 0),
                blockedReasons.build(),
                totalAllocation,
                rawInputDataSize,
                rawInputPositions,
                processedInputDataSize,
                processedInputPositions,
                outputDataSize,
                outputPositions,
                physicalWrittenDataSize,
                fullGcCount,
                fullGcTime.toMillis(),
                pipelineStats,
                mergedRuntimeStats);
    }

    public void updatePeakMemory()
    {
        long userMemory = taskMemoryContext.getUserMemory();
        long systemMemory = taskMemoryContext.getSystemMemory();

        peakTotalMemoryInBytes.accumulateAndGet(userMemory + systemMemory, Math::max);
        peakUserMemoryInBytes.accumulateAndGet(userMemory, Math::max);
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

    @VisibleForTesting
    public synchronized MemoryTrackingContext getTaskMemoryContext()
    {
        return taskMemoryContext;
    }

    @VisibleForTesting
    public QueryContext getQueryContext()
    {
        return queryContext;
    }

    public TaskMemoryReservationSummary getMemoryReservationSummary()
    {
        List<OperatorMemoryReservationSummary> operatorMemoryReservations = getOperatorMemoryReservations();
        long totalTaskMemoryReservationInBytes = operatorMemoryReservations.stream()
                .map(OperatorMemoryReservationSummary::getTotal)
                .mapToLong(DataSize::toBytes)
                .sum();
        List<OperatorMemoryReservationSummary> topConsumers = operatorMemoryReservations.stream()
                .filter(summary -> summary.getTotal().toBytes() > 0)
                .sorted(comparing(OperatorMemoryReservationSummary::getTotal).reversed())
                .limit(3)
                .collect(toImmutableList());
        return new TaskMemoryReservationSummary(
                getShortTaskId(getTaskId()),
                succinctBytes(totalTaskMemoryReservationInBytes),
                topConsumers);
    }

    /**
     * Short task id representation doesn't include the query id
     */
    private static String getShortTaskId(TaskId taskId)
    {
        return taskId.getStageExecutionId().getStageId().getId() + "." + taskId.getStageExecutionId().getId() + "." + taskId.getId();
    }

    private List<OperatorMemoryReservationSummary> getOperatorMemoryReservations()
    {
        ListMultimap<List<Integer>, OperatorContext> operatorContexts = ArrayListMultimap.create();
        accept(new VoidTraversingQueryContextVisitor<Void>()
        {
            @Override
            public Void visitOperatorContext(OperatorContext operatorContext, Void nothing)
            {
                operatorContexts.put(
                        ImmutableList.of(operatorContext.getDriverContext().getPipelineContext().getPipelineId(), operatorContext.getOperatorId()),
                        operatorContext);
                return null;
            }
        }, null);
        ImmutableList.Builder<OperatorMemoryReservationSummary> result = ImmutableList.builder();
        for (Collection<OperatorContext> operators : operatorContexts.asMap().values()) {
            OperatorContext lastContext = getLast(operators);
            long totalOperatorMemoryReservationInBytes = 0;
            List<DataSize> reservations = new ArrayList<>();
            for (OperatorContext context : operators) {
                long reservedTotalMemoryInBytes = context.getCurrentTotalMemoryReservationInBytes();
                totalOperatorMemoryReservationInBytes += reservedTotalMemoryInBytes;
                reservations.add(succinctBytes(reservedTotalMemoryInBytes));
            }
            reservations.sort(reverseOrder());
            result.add(new OperatorMemoryReservationSummary(
                    lastContext.getOperatorType(),
                    lastContext.getPlanNodeId(),
                    ImmutableList.copyOf(reservations),
                    succinctBytes(totalOperatorMemoryReservationInBytes),
                    getAdditionalOperatorInfo(lastContext)));
        }

        return result.build();
    }

    private Optional<String> getAdditionalOperatorInfo(OperatorContext context)
    {
        if (!taskPlan.isPresent()) {
            return Optional.empty();
        }

        if (context.getOperatorType().equals(HashBuilderOperator.class.getSimpleName())) {
            Optional<JoinNode> planNode = findPlanNode(context.getPlanNodeId(), JoinNode.class);
            if (!planNode.isPresent()) {
                return Optional.empty();
            }
            String info = planNode.get().getType().toString() + ";";
            if (planNode.get().getDistributionType().isPresent()) {
                info += planNode.get().getDistributionType().get() + ";";
            }
            return Optional.of(info);
        }

        if (context.getOperatorType().equals(HashAggregationOperator.class.getSimpleName()) ||
                context.getOperatorType().equals(AggregationOperator.class.getSimpleName())) {
            Optional<AggregationNode> planNode = findPlanNode(context.getPlanNodeId(), AggregationNode.class);
            if (!planNode.isPresent()) {
                return Optional.empty();
            }
            boolean isDistinct = planNode.get().getAggregations().values().stream().anyMatch(AggregationNode.Aggregation::isDistinct);
            boolean isOrderBy = planNode.get().getAggregations().values().stream().anyMatch(aggregation -> aggregation.getOrderBy().isPresent());
            String info = planNode.get().getStep() + ";";
            if (isDistinct) {
                info += "DISTINCT;";
            }
            if (isOrderBy) {
                info += "ORDER_BY;";
            }
            return Optional.of(info);
        }

        return Optional.empty();
    }

    private <T extends PlanNode> Optional<T> findPlanNode(PlanNodeId planNodeId, Class<T> nodeType)
    {
        checkState(taskPlan.isPresent(), "taskPlan is expected to be present");
        return searchFrom(taskPlan.get())
                .where(node -> node.getId().equals(planNodeId) && nodeType.isInstance(node))
                .findSingle();
    }
}
