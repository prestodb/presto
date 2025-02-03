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
package com.facebook.presto.execution;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.Distribution;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.scheduler.ScheduleResult;
import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.util.Failures;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.facebook.presto.common.RuntimeMetricName.GET_SPLITS_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.SCAN_STAGE_SCHEDULER_BLOCKED_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.SCAN_STAGE_SCHEDULER_CPU_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.SCAN_STAGE_SCHEDULER_WALL_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.SCHEDULER_BLOCKED_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.SCHEDULER_CPU_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.SCHEDULER_WALL_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.TASK_PLAN_SERIALIZED_CPU_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.TASK_UPDATE_DELIVERED_WALL_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.TASK_UPDATE_SERIALIZED_CPU_TIME_NANOS;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.execution.StageExecutionState.ABORTED;
import static com.facebook.presto.execution.StageExecutionState.CANCELED;
import static com.facebook.presto.execution.StageExecutionState.FAILED;
import static com.facebook.presto.execution.StageExecutionState.FINISHED;
import static com.facebook.presto.execution.StageExecutionState.FINISHED_TASK_SCHEDULING;
import static com.facebook.presto.execution.StageExecutionState.PLANNED;
import static com.facebook.presto.execution.StageExecutionState.RUNNING;
import static com.facebook.presto.execution.StageExecutionState.SCHEDULED;
import static com.facebook.presto.execution.StageExecutionState.SCHEDULING;
import static com.facebook.presto.execution.StageExecutionState.SCHEDULING_SPLITS;
import static com.facebook.presto.execution.StageExecutionState.TERMINAL_STAGE_STATES;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * <p>
 * This class now uses an event loop concurrency model to eliminate the need for explicit synchronization:
 * <ul>
 * <li>All mutable state access and modifications are performed on a single dedicated event loop thread</li>
 * <li>External threads submit operations to the event loop using {@code safeExecuteOnEventLoop()}</li>
 * <li>The event loop serializes all operations, eliminating race conditions without using locks</li>
 * </ul>
 */
public class StageExecutionStateMachine
        implements SchedulerStatsTracker
{
    private static final Logger log = Logger.get(StageExecutionStateMachine.class);

    private final StageExecutionId stageExecutionId;
    private final SplitSchedulerStats scheduledStats;
    private final boolean containsTableScans;

    private final StateMachine<StageExecutionState> state;
    private final StateMachine<Optional<StageExecutionInfo>> finalInfo;
    private ExecutionFailureInfo failureCause;

    private DateTime schedulingCompleteDateTime;
    private final Distribution getSplitDistribution = new Distribution();

    private long peakUserMemory;
    private long peakNodeTotalMemory;
    private long currentUserMemory;
    private long currentTotalMemory;

    private final RuntimeStats runtimeStats = new RuntimeStats();
    private final SafeEventLoopGroup.SafeEventLoop stageEventLoop;

    public StageExecutionStateMachine(
            StageExecutionId stageExecutionId,
            SafeEventLoopGroup.SafeEventLoop stageEventLoop,
            SplitSchedulerStats schedulerStats,
            boolean containsTableScans)
    {
        this.stageExecutionId = requireNonNull(stageExecutionId, "stageId is null");
        this.scheduledStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.containsTableScans = containsTableScans;
        this.stageEventLoop = requireNonNull(stageEventLoop, "stageEventLoop is null");

        state = new StateMachine<>("stage execution " + stageExecutionId, stageEventLoop, PLANNED, TERMINAL_STAGE_STATES);
        state.addStateChangeListener(state -> log.debug("Stage Execution %s is %s", stageExecutionId, state));

        finalInfo = new StateMachine<>("final stage execution " + stageExecutionId, stageEventLoop, Optional.empty());
    }

    public StageExecutionId getStageExecutionId()
    {
        return stageExecutionId;
    }

    public StageExecutionState getFutureState()
    {
        verify(stageEventLoop.inEventLoop());
        return state.get();
    }

    public StageExecutionState getState()
    {
        return state.get();
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateChangeListener<StageExecutionState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    public boolean transitionToScheduling()
    {
        verify(stageEventLoop.inEventLoop());
        return state.compareAndSet(PLANNED, SCHEDULING);
    }

    public boolean transitionToFinishedTaskScheduling()
    {
        verify(stageEventLoop.inEventLoop());
        return state.compareAndSet(SCHEDULING, FINISHED_TASK_SCHEDULING);
    }

    public boolean transitionToSchedulingSplits()
    {
        verify(stageEventLoop.inEventLoop());
        return state.setIf(SCHEDULING_SPLITS, currentState -> currentState == PLANNED || currentState == SCHEDULING || currentState == FINISHED_TASK_SCHEDULING);
    }

    public boolean transitionToScheduled()
    {
        verify(stageEventLoop.inEventLoop());
        if (schedulingCompleteDateTime == null) {
            schedulingCompleteDateTime = DateTime.now();
        }
        return state.setIf(SCHEDULED, currentState -> currentState == PLANNED || currentState == SCHEDULING || currentState == FINISHED_TASK_SCHEDULING || currentState == SCHEDULING_SPLITS);
    }

    public boolean transitionToRunning()
    {
        verify(stageEventLoop.inEventLoop());
        return state.setIf(RUNNING, currentState -> currentState != RUNNING && !currentState.isDone());
    }

    public boolean transitionToFinished()
    {
        verify(stageEventLoop.inEventLoop());
        return state.setIf(FINISHED, currentState -> !currentState.isDone());
    }

    public boolean transitionToCanceled()
    {
        verify(stageEventLoop.inEventLoop());
        return state.setIf(CANCELED, currentState -> !currentState.isDone());
    }

    public boolean transitionToAborted()
    {
        verify(stageEventLoop.inEventLoop());
        return state.setIf(ABORTED, currentState -> !currentState.isDone());
    }

    public boolean transitionToFailed(Throwable throwable)
    {
        verify(stageEventLoop.inEventLoop());
        requireNonNull(throwable, "throwable is null");

        if (failureCause == null) {
            failureCause = Failures.toFailure(throwable);
        }
        boolean failed = state.setIf(FAILED, currentState -> !currentState.isDone());
        if (failed) {
            log.error(throwable, "Stage execution %s failed", stageExecutionId);
        }
        else {
            log.debug(throwable, "Failure after stage execution %s finished", stageExecutionId);
        }
        return failed;
    }

    /**
     * Add a listener for the final stage info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addFinalStageInfoListener(StateChangeListener<StageExecutionInfo> finalStatusListener)
    {
        AtomicBoolean done = new AtomicBoolean();
        StateChangeListener<Optional<StageExecutionInfo>> fireOnceStateChangeListener = finalStageInfo -> {
            if (finalStageInfo.isPresent() && done.compareAndSet(false, true)) {
                finalStatusListener.stateChanged(finalStageInfo.get());
            }
        };
        finalInfo.addStateChangeListener(fireOnceStateChangeListener);
    }

    public void setAllTasksFinal(Iterable<TaskInfo> finalTaskInfos, int totalLifespans)
    {
        requireNonNull(finalTaskInfos, "finalTaskInfos is null");
        checkState(state.get().isDone());
        StageExecutionInfo stageInfo = getStageExecutionInfo(() -> finalTaskInfos, totalLifespans, totalLifespans);
        checkArgument(stageInfo.isFinal(), "finalTaskInfos are not all done");
        finalInfo.compareAndSet(Optional.empty(), Optional.of(stageInfo));
    }

    public long getUserMemoryReservation()
    {
        return currentUserMemory;
    }

    public long getTotalMemoryReservation()
    {
        return currentTotalMemory;
    }

    public void updateMemoryUsage(long deltaUserMemoryInBytes, long deltaTotalMemoryInBytes, long peakNodeTotalMemoryReservationInBytes)
    {
        currentTotalMemory += deltaTotalMemoryInBytes;
        currentUserMemory += deltaUserMemoryInBytes;
        peakUserMemory = max(peakUserMemory, currentUserMemory);
        peakNodeTotalMemory = max(peakNodeTotalMemory, peakNodeTotalMemoryReservationInBytes);
    }

    public BasicStageExecutionStats getBasicStageStats(Supplier<Iterable<TaskInfo>> taskInfosSupplier)
    {
        Optional<StageExecutionInfo> finalStageInfo = this.finalInfo.get();
        if (finalStageInfo.isPresent()) {
            return finalStageInfo.get()
                    .getStats()
                    .toBasicStageStats(finalStageInfo.get().getState());
        }

        // stage state must be captured first in order to provide a
        // consistent view of the stage. For example, building this
        // information, the stage could finish, and the task states would
        // never be visible.
        StageExecutionState state = this.state.get();
        boolean isScheduled = (state == RUNNING) || state.isDone();

        List<TaskInfo> taskInfos = ImmutableList.copyOf(taskInfosSupplier.get());

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int completedDrivers = 0;

        double cumulativeUserMemory = 0;
        double cumulativeTotalMemory = 0;
        long userMemoryReservation = 0;
        long totalMemoryReservation = 0;

        long totalScheduledTime = 0;
        long totalCpuTime = 0;

        long rawInputDataSize = 0;
        long rawInputPositions = 0;

        boolean fullyBlocked = true;
        Set<BlockedReason> blockedReasons = new HashSet<>();

        long totalAllocation = 0;

        for (TaskInfo taskInfo : taskInfos) {
            TaskState taskState = taskInfo.getTaskStatus().getState();
            TaskStats taskStats = taskInfo.getStats();

            totalDrivers += taskStats.getTotalDrivers();
            queuedDrivers += taskStats.getQueuedDrivers();
            runningDrivers += taskStats.getRunningDrivers();
            completedDrivers += taskStats.getCompletedDrivers();

            cumulativeUserMemory += taskStats.getCumulativeUserMemory();

            long taskUserMemory = taskStats.getUserMemoryReservationInBytes();
            long taskSystemMemory = taskStats.getSystemMemoryReservationInBytes();
            userMemoryReservation += taskUserMemory;
            totalMemoryReservation += taskUserMemory + taskSystemMemory;

            totalScheduledTime += taskStats.getTotalScheduledTimeInNanos();
            totalCpuTime += taskStats.getTotalCpuTimeInNanos();
            if (!taskState.isDone()) {
                fullyBlocked &= taskStats.isFullyBlocked();
                blockedReasons.addAll(taskStats.getBlockedReasons());
            }

            totalAllocation += taskStats.getTotalAllocationInBytes();

            if (containsTableScans) {
                rawInputDataSize += taskStats.getRawInputDataSizeInBytes();
                rawInputPositions += taskStats.getRawInputPositions();
            }
        }

        OptionalDouble progressPercentage = OptionalDouble.empty();
        if (isScheduled && totalDrivers != 0) {
            progressPercentage = OptionalDouble.of(min(100, (completedDrivers * 100.0) / totalDrivers));
        }

        return new BasicStageExecutionStats(
                isScheduled,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                completedDrivers,

                succinctBytes(rawInputDataSize),
                rawInputPositions,

                cumulativeUserMemory,
                cumulativeTotalMemory,
                succinctBytes(userMemoryReservation),
                succinctBytes(totalMemoryReservation),

                succinctNanos(totalCpuTime),
                succinctNanos(totalScheduledTime),

                fullyBlocked,
                blockedReasons,

                succinctBytes(totalAllocation),

                progressPercentage);
    }

    public StageExecutionInfo getStageExecutionInfo(Supplier<Iterable<TaskInfo>> taskInfosSupplier, int finishedLifespans, int totalLifespans)
    {
        Optional<StageExecutionInfo> finalStageInfo = this.finalInfo.get();
        if (finalStageInfo.isPresent()) {
            return finalStageInfo.get();
        }

        // stage state must be captured first in order to provide a
        // consistent view of the stage. For example, building this
        // information, the stage could finish, and the task states would
        // never be visible.
        StageExecutionState state = this.state.get();

        List<TaskInfo> taskInfos = ImmutableList.copyOf(taskInfosSupplier.get());
        Optional<ExecutionFailureInfo> failureInfo = Optional.empty();
        if (state == FAILED) {
            failureInfo = Optional.of(failureCause);
        }
        return StageExecutionInfo.create(
                stageExecutionId,
                state,
                failureInfo,
                taskInfos,
                schedulingCompleteDateTime,
                getSplitDistribution.snapshot(),
                runtimeStats,
                succinctBytes(peakUserMemory),
                succinctBytes(peakNodeTotalMemory),
                finishedLifespans,
                totalLifespans);
    }

    public void recordGetSplitTime(long startNanos)
    {
        long elapsedNanos = System.nanoTime() - startNanos;
        getSplitDistribution.add(elapsedNanos);
        scheduledStats.getGetSplitTime().add(elapsedNanos, NANOSECONDS);
        runtimeStats.addMetricValue(GET_SPLITS_TIME_NANOS, NANO, elapsedNanos);
    }

    public void recordSchedulerRunningTime(long cpuTimeNanos, long wallTimeNanos)
    {
        runtimeStats.addMetricValue(SCHEDULER_CPU_TIME_NANOS, NANO, max(cpuTimeNanos, 0));
        runtimeStats.addMetricValue(SCHEDULER_WALL_TIME_NANOS, NANO, max(wallTimeNanos, 0));
    }

    public void recordSchedulerBlockedTime(ScheduleResult.BlockedReason reason, long nanos)
    {
        requireNonNull(reason, "reason is null");
        runtimeStats.addMetricValue(SCHEDULER_BLOCKED_TIME_NANOS + "-" + reason, NANO, max(nanos, 0));
    }

    public void recordLeafStageSchedulerRunningTime(long cpuTimeNanos, long wallTimeNanos)
    {
        runtimeStats.addMetricValue(SCAN_STAGE_SCHEDULER_CPU_TIME_NANOS, NANO, max(cpuTimeNanos, 0));
        runtimeStats.addMetricValue(SCAN_STAGE_SCHEDULER_WALL_TIME_NANOS, NANO, max(wallTimeNanos, 0));
    }

    public void recordLeafStageSchedulerBlockedTime(ScheduleResult.BlockedReason reason, long nanos)
    {
        requireNonNull(reason, "reason is null");
        runtimeStats.addMetricValue(SCAN_STAGE_SCHEDULER_BLOCKED_TIME_NANOS + "-" + reason, NANO, max(nanos, 0));
    }

    @Override
    public void recordTaskUpdateDeliveredTime(long nanos)
    {
        runtimeStats.addMetricValue(TASK_UPDATE_DELIVERED_WALL_TIME_NANOS, NANO, max(nanos, 0));
    }

    @Override
    public void recordTaskUpdateSerializedCpuTime(long nanos)
    {
        runtimeStats.addMetricValue(TASK_UPDATE_SERIALIZED_CPU_TIME_NANOS, NANO, max(nanos, 0));
    }

    @Override
    public void recordTaskPlanSerializedCpuTime(long nanos)
    {
        runtimeStats.addMetricValue(TASK_PLAN_SERIALIZED_CPU_TIME_NANOS, NANO, max(nanos, 0));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stageExecutionId", stageExecutionId)
                .add("state", state)
                .toString();
    }
}
