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
import com.facebook.presto.common.QueryTracer;
import com.facebook.presto.common.RuntimeMetricName;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.scheduler.ScheduleResult;
import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.util.Failures;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import jakarta.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.facebook.airlift.units.Duration.succinctNanos;
import static com.facebook.presto.common.RuntimeMetricName.EVENT_LOOP_METHOD_EXECUTION_CPU_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.GET_SPLITS_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.SCAN_STAGE_SCHEDULER_BLOCKED_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.SCAN_STAGE_SCHEDULER_CPU_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.SCAN_STAGE_SCHEDULER_WALL_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.SCHEDULER_BLOCKED_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.SCHEDULER_CPU_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.SCHEDULER_WALL_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.TASK_PLAN_SERIALIZED_CPU_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.TASK_START_WAIT_FOR_EVENT_LOOP;
import static com.facebook.presto.common.RuntimeMetricName.TASK_UPDATE_DELIVERED_WALL_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.TASK_UPDATE_SERIALIZED_CPU_TIME_NANOS;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
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
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class StageExecutionStateMachine
        implements SchedulerStatsTracker
{
    private static final Logger log = Logger.get(StageExecutionStateMachine.class);

    private final StageExecutionId stageExecutionId;
    private final SplitSchedulerStats scheduledStats;
    private final boolean containsTableScans;

    private final StateMachine<StageExecutionState> state;
    private final StateMachine<Optional<StageExecutionInfo>> finalInfo;
    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

    private final AtomicLong schedulingComplete = new AtomicLong();
    private final Distribution getSplitDistribution = new Distribution();

    private final AtomicLong peakUserMemory = new AtomicLong();
    private final AtomicLong peakNodeTotalMemory = new AtomicLong();
    private final AtomicLong currentUserMemory = new AtomicLong();
    private final AtomicLong currentTotalMemory = new AtomicLong();

    private final RuntimeStats runtimeStats = new RuntimeStats();
    @Nullable
    private final StageTraceState stageTraceState;

    public StageExecutionStateMachine(
            StageExecutionId stageExecutionId,
            ExecutorService executor,
            SplitSchedulerStats schedulerStats,
            boolean containsTableScans)
    {
        this(stageExecutionId, executor, schedulerStats, containsTableScans, null);
    }

    public StageExecutionStateMachine(
            StageExecutionId stageExecutionId,
            ExecutorService executor,
            SplitSchedulerStats schedulerStats,
            boolean containsTableScans,
            @Nullable QueryTracer queryTracer)
    {
        this.stageExecutionId = requireNonNull(stageExecutionId, "stageId is null");
        this.scheduledStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.containsTableScans = containsTableScans;
        this.stageTraceState = queryTracer != null ?
                new StageTraceState(queryTracer, stageExecutionId, System.nanoTime()) :
                null;

        state = new StateMachine<>("stage execution " + stageExecutionId, executor, PLANNED, TERMINAL_STAGE_STATES);
        state.addStateChangeListener(state -> log.debug("Stage Execution %s is %s", stageExecutionId, state));

        finalInfo = new StateMachine<>("final stage execution " + stageExecutionId, executor, Optional.empty());
    }

    public StageExecutionId getStageExecutionId()
    {
        return stageExecutionId;
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

    public synchronized boolean transitionToScheduling()
    {
        boolean scheduling = state.compareAndSet(PLANNED, SCHEDULING);
        if (scheduling && stageTraceState != null) {
            stageTraceState.transitionTo(SCHEDULING, System.nanoTime());
        }
        return scheduling;
    }

    public synchronized boolean transitionToFinishedTaskScheduling()
    {
        boolean finishedTaskScheduling = state.compareAndSet(SCHEDULING, FINISHED_TASK_SCHEDULING);
        if (finishedTaskScheduling && stageTraceState != null) {
            stageTraceState.transitionTo(FINISHED_TASK_SCHEDULING, System.nanoTime());
        }
        return finishedTaskScheduling;
    }

    public synchronized boolean transitionToSchedulingSplits()
    {
        boolean scheduling = state.setIf(
                SCHEDULING_SPLITS,
                currentState -> currentState == PLANNED ||
                        currentState == SCHEDULING ||
                        currentState == FINISHED_TASK_SCHEDULING);
        if (scheduling && stageTraceState != null) {
            stageTraceState.transitionTo(SCHEDULING_SPLITS, System.nanoTime());
        }
        return scheduling;
    }

    public synchronized boolean transitionToScheduled()
    {
        schedulingComplete.compareAndSet(0, currentTimeMillis());
        boolean scheduled = state.setIf(
                SCHEDULED,
                currentState -> currentState == PLANNED ||
                        currentState == SCHEDULING ||
                        currentState == FINISHED_TASK_SCHEDULING ||
                        currentState == SCHEDULING_SPLITS);
        if (scheduled && stageTraceState != null) {
            stageTraceState.transitionTo(SCHEDULED, System.nanoTime());
        }
        return scheduled;
    }

    public boolean transitionToRunning()
    {
        if (stageTraceState == null) {
            return state.setIf(RUNNING, currentState -> currentState != RUNNING && !currentState.isDone());
        }
        synchronized (this) {
            boolean running = state.setIf(RUNNING, currentState -> currentState != RUNNING && !currentState.isDone());
            if (running) {
                stageTraceState.transitionTo(RUNNING, System.nanoTime());
            }
            return running;
        }
    }

    public boolean transitionToFinished()
    {
        return transitionToDoneState(FINISHED);
    }

    public boolean transitionToCanceled()
    {
        return transitionToDoneState(CANCELED);
    }

    public boolean transitionToAborted()
    {
        return transitionToDoneState(ABORTED);
    }

    public boolean transitionToFailed(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");

        failureCause.compareAndSet(null, Failures.toFailure(throwable));
        boolean failed = transitionToDoneState(FAILED);
        if (failed) {
            log.error(throwable, "Stage execution %s failed", stageExecutionId);
        }
        else {
            log.debug(throwable, "Failure after stage execution %s finished", stageExecutionId);
        }
        return failed;
    }

    private boolean transitionToDoneState(StageExecutionState doneState)
    {
        if (stageTraceState == null) {
            return state.setIf(doneState, currentState -> !currentState.isDone());
        }
        synchronized (this) {
            if (state.get().isDone()) {
                return false;
            }
            // Record the terminal boundary before publishing the state because a listener can finish the query trace.
            long transitionTimeNanos = System.nanoTime();
            stageTraceState.transitionTo(doneState, transitionTimeNanos);
            return state.setIf(doneState, currentState -> !currentState.isDone());
        }
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
        return currentUserMemory.get();
    }

    public long getTotalMemoryReservation()
    {
        return currentTotalMemory.get();
    }

    public void updateMemoryUsage(long deltaUserMemoryInBytes, long deltaTotalMemoryInBytes, long peakNodeTotalMemoryReservationInBytes)
    {
        currentTotalMemory.addAndGet(deltaTotalMemoryInBytes);
        currentUserMemory.addAndGet(deltaUserMemoryInBytes);
        peakUserMemory.updateAndGet(currentPeakValue -> max(currentUserMemory.get(), currentPeakValue));
        peakNodeTotalMemory.accumulateAndGet(peakNodeTotalMemoryReservationInBytes, Math::max);
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

        int totalNewDrivers = 0;
        int queuedNewDrivers = 0;
        int runningNewDrivers = 0;
        int completedNewDrivers = 0;

        int totalSplits = 0;
        int queuedSplits = 0;
        int runningSplits = 0;
        int completedSplits = 0;

        double cumulativeUserMemory = 0;
        double cumulativeTotalMemory = 0;
        long userMemoryReservationInBytes = 0;
        long totalMemoryReservationInBytes = 0;

        long totalScheduledTime = 0;
        long totalCpuTime = 0;

        long rawInputDataSizeInBytes = 0;
        long scanRawInputDataSizeInBytes = 0;
        long rawInputPositions = 0;

        boolean fullyBlocked = true;
        Set<BlockedReason> blockedReasons = new HashSet<>();

        long totalAllocationInBytes = 0;

        for (TaskInfo taskInfo : taskInfos) {
            TaskState taskState = taskInfo.getTaskStatus().getState();
            TaskStats taskStats = taskInfo.getStats();

            totalDrivers += taskStats.getTotalDrivers();
            queuedDrivers += taskStats.getQueuedDrivers();
            runningDrivers += taskStats.getRunningDrivers();
            completedDrivers += taskStats.getCompletedDrivers();

            totalNewDrivers += taskStats.getTotalNewDrivers();
            queuedNewDrivers += taskStats.getQueuedNewDrivers();
            runningNewDrivers += taskStats.getRunningNewDrivers();
            completedNewDrivers += taskStats.getCompletedNewDrivers();

            totalSplits += taskStats.getTotalSplits();
            queuedSplits += taskStats.getQueuedSplits();
            runningSplits += taskStats.getRunningSplits();
            completedSplits += taskStats.getCompletedSplits();

            cumulativeUserMemory += taskStats.getCumulativeUserMemory();

            long taskUserMemory = taskStats.getUserMemoryReservationInBytes();
            long taskSystemMemory = taskStats.getSystemMemoryReservationInBytes();
            userMemoryReservationInBytes += taskUserMemory;
            totalMemoryReservationInBytes += taskUserMemory + taskSystemMemory;

            totalScheduledTime += taskStats.getTotalScheduledTimeInNanos();
            totalCpuTime += taskStats.getTotalCpuTimeInNanos();
            if (!taskState.isDone()) {
                fullyBlocked &= taskStats.isFullyBlocked();
                blockedReasons.addAll(taskStats.getBlockedReasons());
            }

            totalAllocationInBytes += taskStats.getTotalAllocationInBytes();

            scanRawInputDataSizeInBytes += taskStats.getScanRawInputDataSizeInBytes();

            if (containsTableScans) {
                rawInputDataSizeInBytes += taskStats.getRawInputDataSizeInBytes();
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

                totalNewDrivers,
                queuedNewDrivers,
                runningNewDrivers,
                completedNewDrivers,

                totalSplits,
                queuedSplits,
                runningSplits,
                completedSplits,

                rawInputDataSizeInBytes,
                scanRawInputDataSizeInBytes,
                rawInputPositions,

                cumulativeUserMemory,
                cumulativeTotalMemory,
                userMemoryReservationInBytes,
                totalMemoryReservationInBytes,

                succinctNanos(totalCpuTime),
                succinctNanos(totalScheduledTime),

                fullyBlocked,
                blockedReasons,

                totalAllocationInBytes,

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
            failureInfo = Optional.of(failureCause.get());
        }
        return StageExecutionInfo.create(
                stageExecutionId,
                state,
                failureInfo,
                taskInfos,
                schedulingComplete.get(),
                getSplitDistribution.snapshot(),
                runtimeStats,
                peakUserMemory.get(),
                peakNodeTotalMemory.get(),
                finishedLifespans,
                totalLifespans);
    }

    public void recordGetSplitTime(long startNanos)
    {
        long endNanos = System.nanoTime();
        long elapsedNanos = max(endNanos - startNanos, 0);
        getSplitDistribution.add(elapsedNanos);
        scheduledStats.getGetSplitTime().add(elapsedNanos, NANOSECONDS);
        recordDurationMetric(GET_SPLITS_TIME_NANOS, startNanos, endNanos);
    }

    public void recordSchedulerRunningTime(long cpuTimeNanos, long startWallTimeNanos, long endWallTimeNanos)
    {
        runtimeStats.addMetricValue(SCHEDULER_CPU_TIME_NANOS, NANO, max(cpuTimeNanos, 0));
        recordDurationMetric(SCHEDULER_WALL_TIME_NANOS, startWallTimeNanos, endWallTimeNanos);
    }

    public void recordSchedulerBlockedTime(ScheduleResult.BlockedReason reason, long startTimeNanos, long endTimeNanos)
    {
        requireNonNull(reason, "reason is null");
        recordDurationMetric(SCHEDULER_BLOCKED_TIME_NANOS + "-" + reason, startTimeNanos, endTimeNanos);
    }

    public void recordLeafStageSchedulerRunningTime(long cpuTimeNanos, long startWallTimeNanos, long endWallTimeNanos)
    {
        runtimeStats.addMetricValue(SCAN_STAGE_SCHEDULER_CPU_TIME_NANOS, NANO, max(cpuTimeNanos, 0));
        recordDurationMetric(SCAN_STAGE_SCHEDULER_WALL_TIME_NANOS, startWallTimeNanos, endWallTimeNanos);
    }

    public void recordLeafStageSchedulerBlockedTime(ScheduleResult.BlockedReason reason, long startTimeNanos, long endTimeNanos)
    {
        requireNonNull(reason, "reason is null");
        recordDurationMetric(SCAN_STAGE_SCHEDULER_BLOCKED_TIME_NANOS + "-" + reason, startTimeNanos, endTimeNanos);
    }

    @Override
    public void recordTaskUpdateDeliveredTime(long nanos)
    {
        runtimeStats.addMetricValue(TASK_UPDATE_DELIVERED_WALL_TIME_NANOS, NANO, max(nanos, 0));
    }

    @Override
    public void recordTaskUpdateDeliveredTime(long startTimeNanos, long endTimeNanos)
    {
        recordDurationMetric(TASK_UPDATE_DELIVERED_WALL_TIME_NANOS, startTimeNanos, endTimeNanos);
    }

    @Override
    public void recordStartWaitForEventLoop(long nanos)
    {
        runtimeStats.addMetricValue(TASK_START_WAIT_FOR_EVENT_LOOP, NANO, max(nanos, 0));
    }

    @Override
    public void recordStartWaitForEventLoop(long startTimeNanos, long endTimeNanos)
    {
        recordDurationMetric(TASK_START_WAIT_FOR_EVENT_LOOP, startTimeNanos, endTimeNanos);
    }

    public void recordDeliveredUpdates(int updates)
    {
        runtimeStats.addMetricValue(RuntimeMetricName.TASK_UPDATE_DELIVERED_UPDATES, NONE, max(updates, 0));
    }

    @Override
    public void recordRoundTripTime(long nanos)
    {
        runtimeStats.addMetricValue(RuntimeMetricName.TASK_UPDATE_ROUND_TRIP_TIME, NANO, max(nanos, 0));
    }

    @Override
    public void recordRoundTripTime(long roundTripNanos, long startTimeNanos, long endTimeNanos, boolean failed)
    {
        recordRoundTripTime(roundTripNanos);
        if (stageTraceState != null) {
            stageTraceState.recordEvent(RuntimeMetricName.TASK_UPDATE_ROUND_TRIP_TIME, startTimeNanos, endTimeNanos, failed);
        }
    }

    private void recordDurationMetric(String name, long startTimeNanos, long endTimeNanos)
    {
        recordDurationMetric(name, startTimeNanos, endTimeNanos, false);
    }

    private void recordDurationMetric(String name, long startTimeNanos, long endTimeNanos, boolean failed)
    {
        long durationNanos = max(endTimeNanos - startTimeNanos, 0);
        runtimeStats.addMetricValue(name, NANO, durationNanos);
        if (stageTraceState != null) {
            stageTraceState.recordEvent(name, startTimeNanos, endTimeNanos, failed);
        }
    }

    /**
     * Lifecycle mutations are serialized by the owning StageExecutionStateMachine.
     */
    private static final class StageTraceState
    {
        private final QueryTracer queryTracer;
        private final String stageId;

        private StageExecutionState currentState = PLANNED;
        private long currentStateStartTimeNanos;
        private long currentStateStartThreadId;
        private String currentStateStartThreadName;

        private StageTraceState(QueryTracer queryTracer, StageExecutionId stageExecutionId, long startTimeNanos)
        {
            Thread startThread = Thread.currentThread();
            this.queryTracer = queryTracer;
            this.stageId = "S" + stageExecutionId.getStageId().getId();
            this.currentStateStartTimeNanos = startTimeNanos;
            this.currentStateStartThreadId = startThread.getId();
            this.currentStateStartThreadName = startThread.getName();
        }

        private void recordEvent(
                String name,
                long startTimeNanos,
                long endTimeNanos,
                boolean failed)
        {
            queryTracer.recordTraceEvent(getTraceName(name), startTimeNanos, endTimeNanos, failed);
        }

        private void transitionTo(StageExecutionState newState, long transitionTimeNanos)
        {
            Thread transitionThread = Thread.currentThread();
            boolean failed = newState.isFailure();
            recordCurrentState(transitionTimeNanos, failed);
            currentState = newState;
            currentStateStartTimeNanos = transitionTimeNanos;
            currentStateStartThreadId = transitionThread.getId();
            currentStateStartThreadName = transitionThread.getName();
            if (newState.isDone()) {
                // Terminal states are retained as zero-duration lifecycle markers.
                recordCurrentState(transitionTimeNanos, failed);
            }
        }

        private void recordCurrentState(long endTimeNanos, boolean failed)
        {
            queryTracer.recordTraceEvent(
                    getTraceName(getStageName(currentState)),
                    currentStateStartTimeNanos,
                    endTimeNanos,
                    currentStateStartThreadId,
                    currentStateStartThreadName,
                    failed);
        }

        private String getTraceName(String name)
        {
            return stageId + "-" + name;
        }

        private static String getStageName(StageExecutionState state)
        {
            return "stage" + UPPER_UNDERSCORE.to(UPPER_CAMEL, state.name());
        }
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
    public void recordEventLoopMethodExecutionCpuTime(long nanos)
    {
        runtimeStats.addMetricValue(EVENT_LOOP_METHOD_EXECUTION_CPU_TIME_NANOS, NANO, max(nanos, 0));
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
