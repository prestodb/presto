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

import com.facebook.airlift.stats.Distribution.DistributionSnapshot;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.units.Duration.succinctDuration;
import static com.facebook.presto.common.RuntimeMetricName.DRIVER_COUNT_PER_TASK;
import static com.facebook.presto.common.RuntimeMetricName.TASK_BLOCKED_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.TASK_ELAPSED_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.TASK_QUEUED_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.TASK_SCHEDULED_TIME_NANOS;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.execution.StageExecutionState.FINISHED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class StageExecutionInfo
{
    private final StageExecutionState state;
    private final StageExecutionStats stats;
    private final List<TaskInfo> tasks;
    private final Optional<ExecutionFailureInfo> failureCause;

    public static StageExecutionInfo create(
            StageExecutionId stageExecutionId,
            StageExecutionState state,
            Optional<ExecutionFailureInfo> failureInfo,
            List<TaskInfo> taskInfos,
            long schedulingCompleteInMillis,
            DistributionSnapshot getSplitDistribution,
            RuntimeStats stageRuntimeStats,
            long peakUserMemoryReservation,
            long peakNodeTotalMemoryReservation,
            int finishedLifespans,
            int totalLifespans)
    {
        TaskStatsAggregator taskStatsAggregator = new TaskStatsAggregator(taskInfos.size(), stageRuntimeStats);

        for (TaskInfo taskInfo : taskInfos) {
            TaskState taskState = taskInfo.getTaskStatus().getState();
            if (taskState.isDone()) {
                taskStatsAggregator.increaseCompleteTaskCount(1);
            }
            else {
                taskStatsAggregator.increaseRunningTaskCount(1);
            }

            TaskStats taskStats = taskInfo.getStats();

            if (state == FINISHED && taskInfo.getTaskStatus().getState() == TaskState.FAILED) {
                taskStatsAggregator.increaseRetriedCpuTime(taskStats.getTotalCpuTimeInNanos());
            }

            if (!taskState.isDone()) {
                taskStatsAggregator.updateFullyBlocked(taskStats.isFullyBlocked());
                taskStatsAggregator.addNewBlockedReasons(taskStats.getBlockedReasons());
            }

            taskStatsAggregator.increaseBufferedDataSize(taskInfo.getOutputBuffers().getTotalBufferedBytes());
            taskStatsAggregator.processTaskStats(taskStats);
        }

        StageExecutionStats stageExecutionStats = new StageExecutionStats(
                schedulingCompleteInMillis,
                getSplitDistribution,

                taskStatsAggregator.totalTaskCount,
                taskStatsAggregator.runningTaskCount,
                taskStatsAggregator.completedTaskCount,

                totalLifespans,
                finishedLifespans,

                taskStatsAggregator.totalDrivers,
                taskStatsAggregator.queuedDrivers,
                taskStatsAggregator.runningDrivers,
                taskStatsAggregator.blockedDrivers,
                taskStatsAggregator.completedDrivers,

                taskStatsAggregator.totalNewDrivers,
                taskStatsAggregator.queuedNewDrivers,
                taskStatsAggregator.runningNewDrivers,
                taskStatsAggregator.completedNewDrivers,

                taskStatsAggregator.totalSplits,
                taskStatsAggregator.queuedSplits,
                taskStatsAggregator.runningSplits,
                taskStatsAggregator.completedSplits,

                taskStatsAggregator.cumulativeUserMemory,
                taskStatsAggregator.cumulativeTotalMemory,
                taskStatsAggregator.userMemoryReservation,
                taskStatsAggregator.totalMemoryReservation,
                peakUserMemoryReservation,
                peakNodeTotalMemoryReservation,
                succinctDuration(taskStatsAggregator.totalScheduledTime, NANOSECONDS),
                succinctDuration(taskStatsAggregator.totalCpuTime, NANOSECONDS),
                succinctDuration(taskStatsAggregator.retriedCpuTime, NANOSECONDS),
                succinctDuration(taskStatsAggregator.totalBlockedTime, NANOSECONDS),
                taskStatsAggregator.fullyBlocked && taskStatsAggregator.runningTaskCount > 0,
                taskStatsAggregator.blockedReasons,

                taskStatsAggregator.totalAllocation,

                taskStatsAggregator.rawInputDataSize,
                taskStatsAggregator.rawInputPositions,
                taskStatsAggregator.processedInputDataSize,
                taskStatsAggregator.processedInputPositions,
                taskStatsAggregator.bufferedDataSize,
                taskStatsAggregator.outputDataSize,
                taskStatsAggregator.outputPositions,
                taskStatsAggregator.physicalWrittenDataSize,

                new StageGcStatistics(
                        stageExecutionId.getStageId().getId(),
                        stageExecutionId.getId(),
                        taskStatsAggregator.totalTaskCount,
                        taskStatsAggregator.fullGcTaskCount,
                        taskStatsAggregator.minFullGcSec,
                        taskStatsAggregator.maxFullGcSec,
                        taskStatsAggregator.totalFullGcSec,
                        (int) (1.0 * taskStatsAggregator.totalFullGcSec / taskStatsAggregator.fullGcCount)),
                taskStatsAggregator.getOperatorSummaries(),
                taskStatsAggregator.getMergedRuntimeStats());

        return new StageExecutionInfo(
                state,
                stageExecutionStats,
                taskInfos,
                failureInfo);
    }

    @JsonCreator
    public StageExecutionInfo(
            @JsonProperty("state") StageExecutionState state,
            @JsonProperty("stats") StageExecutionStats stats,
            @JsonProperty("tasks") List<TaskInfo> tasks,
            @JsonProperty("failureCause") Optional<ExecutionFailureInfo> failureCause)
    {
        this.state = requireNonNull(state, "state is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.tasks = ImmutableList.copyOf(requireNonNull(tasks, "tasks is null"));
        this.failureCause = requireNonNull(failureCause, "failureCause is null");
    }

    @JsonProperty
    public StageExecutionState getState()
    {
        return state;
    }

    @JsonProperty
    public StageExecutionStats getStats()
    {
        return stats;
    }

    @JsonProperty
    public List<TaskInfo> getTasks()
    {
        return tasks;
    }

    @JsonProperty
    public Optional<ExecutionFailureInfo> getFailureCause()
    {
        return failureCause;
    }

    public boolean isFinal()
    {
        return state.isDone() && tasks.stream().allMatch(taskInfo -> taskInfo.getTaskStatus().getState().isDone());
    }

    private static class OperatorKey
    {
        private final int pipelineId;
        private final int operatorId;

        public OperatorKey(int pipelineId, int operatorId)
        {
            this.pipelineId = pipelineId;
            this.operatorId = operatorId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OperatorKey that = (OperatorKey) o;
            return pipelineId == that.pipelineId && operatorId == that.operatorId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(pipelineId, operatorId);
        }
    }

    private static class TaskStatsAggregator
    {
        private final int totalTaskCount;
        private int runningTaskCount;
        private int completedTaskCount;
        private long retriedCpuTime;
        private long bufferedDataSize;

        private boolean fullyBlocked = true;
        private final Set<BlockedReason> blockedReasons = new HashSet<>();

        private int totalDrivers;
        private int queuedDrivers;
        private int runningDrivers;
        private int blockedDrivers;
        private int completedDrivers;

        private int totalNewDrivers;
        private int queuedNewDrivers;
        private int runningNewDrivers;
        private int completedNewDrivers;

        private int totalSplits;
        private int queuedSplits;
        private int runningSplits;
        private int completedSplits;

        private double cumulativeUserMemory;
        private double cumulativeTotalMemory;
        private long userMemoryReservation;
        private long totalMemoryReservation;

        private long totalScheduledTime;
        private long totalCpuTime;
        private long totalBlockedTime;

        private long totalAllocation;

        private long rawInputDataSize;
        private long rawInputPositions;

        private long processedInputDataSize;
        private long processedInputPositions;

        private long outputDataSize;
        private long outputPositions;

        private long physicalWrittenDataSize;

        private int fullGcCount;
        private int fullGcTaskCount;
        private int minFullGcSec;
        private int maxFullGcSec;
        private int totalFullGcSec;

        private final RuntimeStats mergedRuntimeStats = new RuntimeStats();
        private final Map<OperatorKey, List<OperatorStats>> operatorStatsByKey = new HashMap<>();

        public TaskStatsAggregator(int totalTaskCount, RuntimeStats stageRuntimeStats)
        {
            this.totalTaskCount = totalTaskCount;
            this.mergedRuntimeStats.mergeWith(stageRuntimeStats);
        }

        public void processTaskStats(TaskStats taskStats)
        {
            totalDrivers += taskStats.getTotalDrivers();
            queuedDrivers += taskStats.getQueuedDrivers();
            runningDrivers += taskStats.getRunningDrivers();
            blockedDrivers += taskStats.getBlockedDrivers();
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
            cumulativeTotalMemory += taskStats.getCumulativeTotalMemory();

            long taskUserMemory = taskStats.getUserMemoryReservationInBytes();
            long taskSystemMemory = taskStats.getSystemMemoryReservationInBytes();
            userMemoryReservation += taskUserMemory;
            totalMemoryReservation += taskUserMemory + taskSystemMemory;

            totalScheduledTime += taskStats.getTotalScheduledTimeInNanos();
            totalCpuTime += taskStats.getTotalCpuTimeInNanos();
            totalBlockedTime += taskStats.getTotalBlockedTimeInNanos();

            totalAllocation += taskStats.getTotalAllocationInBytes();

            rawInputDataSize += taskStats.getRawInputDataSizeInBytes();
            rawInputPositions += taskStats.getRawInputPositions();

            processedInputDataSize += taskStats.getProcessedInputDataSizeInBytes();
            processedInputPositions += taskStats.getProcessedInputPositions();

            outputDataSize += taskStats.getOutputDataSizeInBytes();
            outputPositions += taskStats.getOutputPositions();

            physicalWrittenDataSize += taskStats.getPhysicalWrittenDataSizeInBytes();

            fullGcCount += taskStats.getFullGcCount();
            fullGcTaskCount += taskStats.getFullGcCount() > 0 ? 1 : 0;

            int gcSec = toIntExact(MILLISECONDS.toSeconds(taskStats.getFullGcTimeInMillis()));
            totalFullGcSec += gcSec;
            minFullGcSec = min(minFullGcSec, gcSec);
            maxFullGcSec = max(maxFullGcSec, gcSec);

            updateOperatorStats(taskStats);
            updateRuntimeStats(taskStats);
        }

        private void updateOperatorStats(TaskStats taskStats)
        {
            // Collect all operator stats by their key
            for (PipelineStats pipeline : taskStats.getPipelines()) {
                for (OperatorStats operatorStats : pipeline.getOperatorSummaries()) {
                    operatorStatsByKey.computeIfAbsent(new OperatorKey(pipeline.getPipelineId(), operatorStats.getOperatorId()), k -> new ArrayList<>()).add(operatorStats);
                }
            }
        }

        private void updateRuntimeStats(TaskStats taskStats)
        {
            mergedRuntimeStats.mergeWith(taskStats.getRuntimeStats());
            mergedRuntimeStats.addMetricValue(DRIVER_COUNT_PER_TASK, NONE, taskStats.getTotalDrivers());
            mergedRuntimeStats.addMetricValue(TASK_ELAPSED_TIME_NANOS, NANO, taskStats.getElapsedTimeInNanos());
            mergedRuntimeStats.addMetricValueIgnoreZero(TASK_QUEUED_TIME_NANOS, NANO, taskStats.getQueuedTimeInNanos());
            mergedRuntimeStats.addMetricValue(TASK_SCHEDULED_TIME_NANOS, NANO, taskStats.getTotalScheduledTimeInNanos());
            mergedRuntimeStats.addMetricValueIgnoreZero(TASK_BLOCKED_TIME_NANOS, NANO, taskStats.getTotalBlockedTimeInNanos());
        }

        public RuntimeStats getMergedRuntimeStats()
        {
            return mergedRuntimeStats;
        }

        public List<OperatorStats> getOperatorSummaries()
        {
            return operatorStatsByKey.values().stream()
                    .map(OperatorStats::merge)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableList());
        }

        public void increaseRunningTaskCount(int count)
        {
            runningTaskCount += count;
        }

        public void increaseCompleteTaskCount(int count)
        {
            completedTaskCount += count;
        }

        public void increaseRetriedCpuTime(long time)
        {
            retriedCpuTime += time;
        }

        public void updateFullyBlocked(boolean blocked)
        {
            fullyBlocked &= blocked;
        }

        public void addNewBlockedReasons(Set<BlockedReason> reasons)
        {
            blockedReasons.addAll(reasons);
        }

        public void increaseBufferedDataSize(long bytes)
        {
            bufferedDataSize += bytes;
        }
    }
}
