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
import com.facebook.presto.operator.OperatorStatsMerger;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;

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
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Builds the {@link StageExecutionStats} of a stage execution from its tasks, added one at a time
 * without being retained. Adding tasks in the order {@link StageExecutionInfo#create} gets them
 * gives the same stats as {@code create}. The aggregator must not be used after {@link #build}.
 */
public class StageTaskStatsAggregator
{
    private int totalTaskCount;
    private int runningTaskCount;
    private int completedTaskCount;
    private long failedTaskCpuTime;
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
    private long scanRawInputDataSize;
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
    private final Map<OperatorKey, OperatorStatsMerger> operatorStatsByKey = new HashMap<>();

    private boolean built;

    public StageTaskStatsAggregator(RuntimeStats stageRuntimeStats)
    {
        mergedRuntimeStats.mergeWith(stageRuntimeStats);
    }

    public void addTask(TaskInfo taskInfo)
    {
        checkState(!built, "build() has already been called");
        totalTaskCount++;

        TaskState taskState = taskInfo.getTaskStatus().getState();
        if (taskState.isDone()) {
            completedTaskCount++;
        }
        else {
            runningTaskCount++;
        }

        TaskStats taskStats = taskInfo.getStats();

        if (taskState == TaskState.FAILED) {
            failedTaskCpuTime += taskStats.getTotalCpuTimeInNanos();
        }

        if (!taskState.isDone()) {
            fullyBlocked &= taskStats.isFullyBlocked();
            blockedReasons.addAll(taskStats.getBlockedReasons());
        }

        bufferedDataSize += taskInfo.getOutputBuffers().getTotalBufferedBytes();

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
        scanRawInputDataSize += taskStats.getScanRawInputDataSizeInBytes();
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

        for (PipelineStats pipeline : taskStats.getPipelines()) {
            for (OperatorStats operatorStats : pipeline.getOperatorSummaries()) {
                operatorStatsByKey.computeIfAbsent(new OperatorKey(pipeline.getPipelineId(), operatorStats.getOperatorId()), key -> new OperatorStatsMerger()).add(operatorStats);
            }
        }

        mergedRuntimeStats.mergeWith(taskStats.getRuntimeStats());
        mergedRuntimeStats.addMetricValue(DRIVER_COUNT_PER_TASK, NONE, taskStats.getTotalDrivers());
        mergedRuntimeStats.addMetricValue(TASK_ELAPSED_TIME_NANOS, NANO, taskStats.getElapsedTimeInNanos());
        mergedRuntimeStats.addMetricValueIgnoreZero(TASK_QUEUED_TIME_NANOS, NANO, taskStats.getQueuedTimeInNanos());
        mergedRuntimeStats.addMetricValue(TASK_SCHEDULED_TIME_NANOS, NANO, taskStats.getTotalScheduledTimeInNanos());
        mergedRuntimeStats.addMetricValueIgnoreZero(TASK_BLOCKED_TIME_NANOS, NANO, taskStats.getTotalBlockedTimeInNanos());
    }

    public StageExecutionStats build(
            StageExecutionId stageExecutionId,
            StageExecutionState state,
            long schedulingCompleteInMillis,
            DistributionSnapshot getSplitDistribution,
            long peakUserMemoryReservation,
            long peakNodeTotalMemoryReservation,
            int finishedLifespans,
            int totalLifespans)
    {
        built = true;
        List<OperatorStats> operatorSummaries = operatorStatsByKey.values().stream()
                .map(OperatorStatsMerger::build)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        return new StageExecutionStats(
                schedulingCompleteInMillis,
                getSplitDistribution,

                totalTaskCount,
                runningTaskCount,
                completedTaskCount,

                totalLifespans,
                finishedLifespans,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                blockedDrivers,
                completedDrivers,

                totalNewDrivers,
                queuedNewDrivers,
                runningNewDrivers,
                completedNewDrivers,

                totalSplits,
                queuedSplits,
                runningSplits,
                completedSplits,

                cumulativeUserMemory,
                cumulativeTotalMemory,
                userMemoryReservation,
                totalMemoryReservation,
                peakUserMemoryReservation,
                peakNodeTotalMemoryReservation,
                succinctDuration(totalScheduledTime, NANOSECONDS),
                succinctDuration(totalCpuTime, NANOSECONDS),
                succinctDuration(state == FINISHED ? failedTaskCpuTime : 0, NANOSECONDS),
                succinctDuration(totalBlockedTime, NANOSECONDS),
                fullyBlocked && runningTaskCount > 0,
                blockedReasons,

                totalAllocation,

                rawInputDataSize,
                scanRawInputDataSize,
                rawInputPositions,
                processedInputDataSize,
                processedInputPositions,
                bufferedDataSize,
                outputDataSize,
                outputPositions,
                physicalWrittenDataSize,

                new StageGcStatistics(
                        stageExecutionId.getStageId().getId(),
                        stageExecutionId.getId(),
                        totalTaskCount,
                        fullGcTaskCount,
                        minFullGcSec,
                        maxFullGcSec,
                        totalFullGcSec,
                        (int) (1.0 * totalFullGcSec / fullGcCount)),
                operatorSummaries,
                mergedRuntimeStats);
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
}
