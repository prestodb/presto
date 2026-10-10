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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class StageExecutionInfo
{
    private final StageExecutionState state;
    private final StageExecutionStats stats;
    private final List<TaskInfo> tasks;
    private final Optional<ExecutionFailureInfo> failureCause;
    private final Optional<StageExecutionTaskSummary> taskSummary;

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
        StageTaskStatsAggregator taskStatsAggregator = new StageTaskStatsAggregator(stageRuntimeStats);
        for (TaskInfo taskInfo : taskInfos) {
            taskStatsAggregator.addTask(taskInfo);
        }

        StageExecutionStats stageExecutionStats = taskStatsAggregator.build(
                stageExecutionId,
                state,
                schedulingCompleteInMillis,
                getSplitDistribution,
                peakUserMemoryReservation,
                peakNodeTotalMemoryReservation,
                finishedLifespans,
                totalLifespans);

        return new StageExecutionInfo(
                state,
                stageExecutionStats,
                taskInfos,
                failureInfo);
    }

    /**
     * Creates a stage execution info that carries a summary of its tasks in place of the tasks.
     * {@link #getTasks()} of the result is empty.
     */
    public static StageExecutionInfo createWithTaskSummary(
            StageExecutionState state,
            StageExecutionStats stats,
            StageExecutionTaskSummary taskSummary,
            Optional<ExecutionFailureInfo> failureCause)
    {
        return new StageExecutionInfo(state, stats, ImmutableList.of(), failureCause, Optional.of(taskSummary));
    }

    @JsonCreator
    public StageExecutionInfo(
            @JsonProperty("state") StageExecutionState state,
            @JsonProperty("stats") StageExecutionStats stats,
            @JsonProperty("tasks") List<TaskInfo> tasks,
            @JsonProperty("failureCause") Optional<ExecutionFailureInfo> failureCause)
    {
        this(state, stats, tasks, failureCause, Optional.empty());
    }

    private StageExecutionInfo(
            StageExecutionState state,
            StageExecutionStats stats,
            List<TaskInfo> tasks,
            Optional<ExecutionFailureInfo> failureCause,
            Optional<StageExecutionTaskSummary> taskSummary)
    {
        this.state = requireNonNull(state, "state is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.tasks = ImmutableList.copyOf(requireNonNull(tasks, "tasks is null"));
        this.failureCause = requireNonNull(failureCause, "failureCause is null");
        this.taskSummary = requireNonNull(taskSummary, "taskSummary is null");
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

    /**
     * Summary of the tasks, present only when the tasks themselves were not retained.
     */
    @JsonIgnore
    public Optional<StageExecutionTaskSummary> getTaskSummary()
    {
        return taskSummary;
    }

    public boolean isFinal()
    {
        return state.isDone() && tasks.stream().allMatch(taskInfo -> taskInfo.getTaskStatus().getState().isDone());
    }
}
