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

import com.facebook.presto.Session;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.util.Failures;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.stats.Distribution;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.facebook.presto.execution.StageState.ABORTED;
import static com.facebook.presto.execution.StageState.CANCELED;
import static com.facebook.presto.execution.StageState.FAILED;
import static com.facebook.presto.execution.StageState.FINISHED;
import static com.facebook.presto.execution.StageState.PLANNED;
import static com.facebook.presto.execution.StageState.RUNNING;
import static com.facebook.presto.execution.StageState.SCHEDULED;
import static com.facebook.presto.execution.StageState.SCHEDULING;
import static com.facebook.presto.execution.StageState.SCHEDULING_SPLITS;
import static com.facebook.presto.execution.StageState.TERMINAL_STAGE_STATES;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class StageStateMachine
{
    private static final Logger log = Logger.get(StageStateMachine.class);

    private final StageId stageId;
    private final URI location;
    private final PlanFragment fragment;
    private final Session session;

    private final StateMachine<StageState> stageState;
    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

    private final AtomicReference<DateTime> schedulingComplete = new AtomicReference<>();
    private final Distribution getSplitDistribution = new Distribution();
    private final Distribution scheduleTaskDistribution = new Distribution();
    private final Distribution addSplitDistribution = new Distribution();

    private final AtomicLong peakMemory = new AtomicLong();
    private final AtomicLong currentMemory = new AtomicLong();

    public StageStateMachine(StageId stageId, URI location, Session session, PlanFragment fragment, ExecutorService executor)
    {
        this.stageId = requireNonNull(stageId, "stageId is null");
        this.location = requireNonNull(location, "location is null");
        this.session = requireNonNull(session, "session is null");
        this.fragment = requireNonNull(fragment, "fragment is null");

        stageState = new StateMachine<>("stage " + stageId, executor, PLANNED, TERMINAL_STAGE_STATES);
        stageState.addStateChangeListener(state -> log.debug("Stage %s is %s", stageId, state));
    }

    public StageId getStageId()
    {
        return stageId;
    }

    public URI getLocation()
    {
        return location;
    }

    public Session getSession()
    {
        return session;
    }

    public StageState getState()
    {
        return stageState.get();
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public void addStateChangeListener(StateChangeListener<StageState> stateChangeListener)
    {
        stageState.addStateChangeListener(stateChangeListener);
    }

    public synchronized boolean transitionToScheduling()
    {
        return stageState.compareAndSet(PLANNED, SCHEDULING);
    }

    public synchronized boolean transitionToSchedulingSplits()
    {
        return stageState.setIf(SCHEDULING_SPLITS, currentState -> currentState == PLANNED || currentState == SCHEDULING);
    }

    public synchronized boolean transitionToScheduled()
    {
        schedulingComplete.compareAndSet(null, DateTime.now());
        return stageState.setIf(SCHEDULED, currentState -> currentState == PLANNED || currentState == SCHEDULING || currentState == SCHEDULING_SPLITS);
    }

    public boolean transitionToRunning()
    {
        return stageState.setIf(RUNNING, currentState -> currentState != RUNNING && !currentState.isDone());
    }

    public boolean transitionToFinished()
    {
        return stageState.setIf(FINISHED, currentState -> !currentState.isDone());
    }

    public boolean transitionToCanceled()
    {
        return stageState.setIf(CANCELED, currentState -> !currentState.isDone());
    }

    public boolean transitionToAborted()
    {
        return stageState.setIf(ABORTED, currentState -> !currentState.isDone());
    }

    public boolean transitionToFailed(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");

        failureCause.compareAndSet(null, Failures.toFailure(throwable));
        boolean failed = stageState.setIf(FAILED, currentState -> !currentState.isDone());
        if (failed) {
            log.error(throwable, "Stage %s failed", stageId);
        }
        else {
            log.debug(throwable, "Failure after stage %s finished", stageId);
        }
        return failed;
    }

    public long getPeakMemoryInBytes()
    {
        return peakMemory.get();
    }

    public void updateMemoryUsage(long deltaMemoryInBytes)
    {
        long currentMemoryValue = currentMemory.addAndGet(deltaMemoryInBytes);
        if (currentMemoryValue > peakMemory.get()) {
            peakMemory.updateAndGet(x -> currentMemoryValue > x ? currentMemoryValue : x);
        }
    }

    public StageInfo getStageInfo(Supplier<Iterable<TaskInfo>> taskInfosSupplier, Supplier<Iterable<StageInfo>> subStageInfosSupplier)
    {
        // stage state must be captured first in order to provide a
        // consistent view of the stage. For example, building this
        // information, the stage could finish, and the task states would
        // never be visible.
        StageState state = stageState.get();

        List<TaskInfo> taskInfos = ImmutableList.copyOf(taskInfosSupplier.get());
        List<StageInfo> subStageInfos = ImmutableList.copyOf(subStageInfosSupplier.get());

        int totalTasks = taskInfos.size();
        int runningTasks = 0;
        int completedTasks = 0;

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int completedDrivers = 0;

        long cumulativeMemory = 0;
        long totalMemoryReservation = 0;
        long peakMemoryReservation = getPeakMemoryInBytes();

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

        boolean fullyBlocked = true;
        Set<BlockedReason> blockedReasons = new HashSet<>();

        for (TaskInfo taskInfo : taskInfos) {
            TaskState taskState = taskInfo.getTaskStatus().getState();
            if (taskState.isDone()) {
                completedTasks++;
            }
            else {
                runningTasks++;
            }

            TaskStats taskStats = taskInfo.getStats();

            totalDrivers += taskStats.getTotalDrivers();
            queuedDrivers += taskStats.getQueuedDrivers();
            runningDrivers += taskStats.getRunningDrivers();
            completedDrivers += taskStats.getCompletedDrivers();

            cumulativeMemory += taskStats.getCumulativeMemory();
            totalMemoryReservation += taskStats.getMemoryReservation().toBytes();

            totalScheduledTime += taskStats.getTotalScheduledTime().roundTo(NANOSECONDS);
            totalCpuTime += taskStats.getTotalCpuTime().roundTo(NANOSECONDS);
            totalUserTime += taskStats.getTotalUserTime().roundTo(NANOSECONDS);
            totalBlockedTime += taskStats.getTotalBlockedTime().roundTo(NANOSECONDS);
            if (!taskState.isDone()) {
                fullyBlocked &= taskStats.isFullyBlocked();
                blockedReasons.addAll(taskStats.getBlockedReasons());
            }

            rawInputDataSize += taskStats.getRawInputDataSize().toBytes();
            rawInputPositions += taskStats.getRawInputPositions();

            processedInputDataSize += taskStats.getProcessedInputDataSize().toBytes();
            processedInputPositions += taskStats.getProcessedInputPositions();

            outputDataSize += taskStats.getOutputDataSize().toBytes();
            outputPositions += taskStats.getOutputPositions();
        }

        StageStats stageStats = new StageStats(
                schedulingComplete.get(),
                getSplitDistribution.snapshot(),
                scheduleTaskDistribution.snapshot(),
                addSplitDistribution.snapshot(),

                totalTasks,
                runningTasks,
                completedTasks,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                completedDrivers,

                cumulativeMemory,
                succinctBytes(totalMemoryReservation),
                succinctBytes(peakMemoryReservation),
                succinctDuration(totalScheduledTime, NANOSECONDS),
                succinctDuration(totalCpuTime, NANOSECONDS),
                succinctDuration(totalUserTime, NANOSECONDS),
                succinctDuration(totalBlockedTime, NANOSECONDS),
                fullyBlocked && runningTasks > 0,
                blockedReasons,

                succinctBytes(rawInputDataSize),
                rawInputPositions,
                succinctBytes(processedInputDataSize),
                processedInputPositions,
                succinctBytes(outputDataSize),
                outputPositions);

        ExecutionFailureInfo failureInfo = null;
        if (state == FAILED) {
            failureInfo = failureCause.get();
        }
        return new StageInfo(stageId,
                state,
                location,
                fragment,
                fragment.getTypes(),
                stageStats,
                taskInfos,
                subStageInfos,
                failureInfo);
    }

    public void recordGetSplitTime(long startNanos)
    {
        getSplitDistribution.add(System.nanoTime() - startNanos);
    }

    public void recordScheduleTaskTime(long startNanos)
    {
        scheduleTaskDistribution.add(System.nanoTime() - startNanos);
    }

    public void recordAddSplit(long startNanos)
    {
        addSplitDistribution.add(System.nanoTime() - startNanos);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("stageId", stageId)
                .add("stageState", stageState)
                .toString();
    }
}
