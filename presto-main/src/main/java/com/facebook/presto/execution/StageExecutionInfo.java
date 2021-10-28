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
import com.facebook.presto.common.RuntimeMetricName;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.execution.StageExecutionState.FINISHED;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
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
            DateTime schedulingComplete,
            DistributionSnapshot getSplitDistribution,
            DataSize peakUserMemoryReservation,
            DataSize peakNodeTotalMemoryReservation,
            int finishedLifespans,
            int totalLifespans)
    {
        int totalTasks = taskInfos.size();
        int runningTasks = 0;
        int completedTasks = 0;

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int blockedDrivers = 0;
        int completedDrivers = 0;

        double cumulativeUserMemory = 0;
        double cumulativeTotalMemory = 0;
        long userMemoryReservation = 0;
        long totalMemoryReservation = 0;

        long totalScheduledTime = 0;
        long totalCpuTime = 0;
        long retriedCpuTime = 0;
        long totalBlockedTime = 0;

        long totalAllocation = 0;

        long rawInputDataSize = 0;
        long rawInputPositions = 0;

        long processedInputDataSize = 0;
        long processedInputPositions = 0;

        long bufferedDataSize = 0;
        long outputDataSize = 0;
        long outputPositions = 0;

        long physicalWrittenDataSize = 0;

        int fullGcCount = 0;
        int fullGcTaskCount = 0;
        int minFullGcSec = 0;
        int maxFullGcSec = 0;
        int totalFullGcSec = 0;

        boolean fullyBlocked = true;
        Set<BlockedReason> blockedReasons = new HashSet<>();

        Map<String, OperatorStats> operatorToStats = new HashMap<>();
        RuntimeStats mergedRuntimeStats = new RuntimeStats();
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
            blockedDrivers += taskStats.getBlockedDrivers();
            completedDrivers += taskStats.getCompletedDrivers();

            cumulativeUserMemory += taskStats.getCumulativeUserMemory();
            cumulativeTotalMemory += taskStats.getCumulativeTotalMemory();

            long taskUserMemory = taskStats.getUserMemoryReservationInBytes();
            long taskSystemMemory = taskStats.getSystemMemoryReservationInBytes();
            userMemoryReservation += taskUserMemory;
            totalMemoryReservation += taskUserMemory + taskSystemMemory;

            totalScheduledTime += taskStats.getTotalScheduledTimeInNanos();
            totalCpuTime += taskStats.getTotalCpuTimeInNanos();
            if (state == FINISHED && taskInfo.getTaskStatus().getState() == TaskState.FAILED) {
                retriedCpuTime += taskStats.getTotalCpuTimeInNanos();
            }
            totalBlockedTime += taskStats.getTotalBlockedTimeInNanos();
            if (!taskState.isDone()) {
                fullyBlocked &= taskStats.isFullyBlocked();
                blockedReasons.addAll(taskStats.getBlockedReasons());
            }

            totalAllocation += taskStats.getTotalAllocationInBytes();

            rawInputDataSize += taskStats.getRawInputDataSizeInBytes();
            rawInputPositions += taskStats.getRawInputPositions();

            processedInputDataSize += taskStats.getProcessedInputDataSizeInBytes();
            processedInputPositions += taskStats.getProcessedInputPositions();

            bufferedDataSize += taskInfo.getOutputBuffers().getTotalBufferedBytes();
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
                    String id = pipeline.getPipelineId() + "." + operatorStats.getOperatorId();
                    operatorToStats.compute(id, (k, v) -> v == null ? operatorStats : v.add(operatorStats));
                }
            }
            mergedRuntimeStats.mergeWith(taskStats.getRuntimeStats());
            mergedRuntimeStats.addMetricValue(RuntimeMetricName.DRIVER_COUNT_PER_TASK, taskStats.getTotalDrivers());
            mergedRuntimeStats.addMetricValue(RuntimeMetricName.TASK_ELAPSED_TIME_NANOS, taskStats.getElapsedTimeInNanos());
        }

        StageExecutionStats stageExecutionStats = new StageExecutionStats(
                schedulingComplete,
                getSplitDistribution,

                totalTasks,
                runningTasks,
                completedTasks,

                totalLifespans,
                finishedLifespans,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                blockedDrivers,
                completedDrivers,

                cumulativeUserMemory,
                cumulativeTotalMemory,
                succinctBytes(userMemoryReservation),
                succinctBytes(totalMemoryReservation),
                peakUserMemoryReservation,
                peakNodeTotalMemoryReservation,
                succinctDuration(totalScheduledTime, NANOSECONDS),
                succinctDuration(totalCpuTime, NANOSECONDS),
                succinctDuration(retriedCpuTime, NANOSECONDS),
                succinctDuration(totalBlockedTime, NANOSECONDS),
                fullyBlocked && runningTasks > 0,
                blockedReasons,

                succinctBytes(totalAllocation),

                succinctBytes(rawInputDataSize),
                rawInputPositions,
                succinctBytes(processedInputDataSize),
                processedInputPositions,
                succinctBytes(bufferedDataSize),
                succinctBytes(outputDataSize),
                outputPositions,
                succinctBytes(physicalWrittenDataSize),

                new StageGcStatistics(
                        stageExecutionId.getStageId().getId(),
                        stageExecutionId.getId(),
                        totalTasks,
                        fullGcTaskCount,
                        minFullGcSec,
                        maxFullGcSec,
                        totalFullGcSec,
                        (int) (1.0 * totalFullGcSec / fullGcCount)),

                ImmutableList.copyOf(operatorToStats.values()),
                mergedRuntimeStats);

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

    public static StageExecutionInfo unscheduledExecutionInfo(int stageId, boolean isQueryDone)
    {
        return new StageExecutionInfo(
                isQueryDone ? StageExecutionState.ABORTED : StageExecutionState.PLANNED,
                StageExecutionStats.zero(stageId),
                ImmutableList.of(),
                Optional.empty());
    }
}
