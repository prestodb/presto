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

import com.facebook.airlift.stats.Distribution;
import com.facebook.airlift.stats.Distribution.DistributionSnapshot;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.planPrinter.PlanNodeStats;
import com.facebook.presto.sql.planner.planPrinter.PlanNodeStatsAccumulator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongBinaryOperator;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * What the consumers of {@link StageExecutionInfo#getTasks()} need from the tasks of a stage
 * execution, folded one task at a time so that the tasks need not be retained.
 */
public class StageExecutionTaskSummary
{
    private final int taskCount;
    private final DistributionSnapshot taskCpuTimeMillisDistribution;
    private final DistributionSnapshot taskPeakTotalMemoryDistribution;
    private final Optional<FailedTask> firstFailedTask;
    private final long minFirstStartTimeInMillis;
    private final long maxLastStartTimeInMillis;
    private final long maxEndTimeInMillis;
    private final long processedInputPositionsSum;
    private final double processedInputPositionsM2;
    private final Set<String> nodeIds;
    private final Map<PlanNodeId, PlanNodeStats> planNodeStats;
    private final Optional<RuntimeException> planNodeStatsFailure;

    private StageExecutionTaskSummary(
            int taskCount,
            DistributionSnapshot taskCpuTimeMillisDistribution,
            DistributionSnapshot taskPeakTotalMemoryDistribution,
            Optional<FailedTask> firstFailedTask,
            long minFirstStartTimeInMillis,
            long maxLastStartTimeInMillis,
            long maxEndTimeInMillis,
            long processedInputPositionsSum,
            double processedInputPositionsM2,
            Set<String> nodeIds,
            Map<PlanNodeId, PlanNodeStats> planNodeStats,
            Optional<RuntimeException> planNodeStatsFailure)
    {
        this.taskCount = taskCount;
        this.taskCpuTimeMillisDistribution = requireNonNull(taskCpuTimeMillisDistribution, "taskCpuTimeMillisDistribution is null");
        this.taskPeakTotalMemoryDistribution = requireNonNull(taskPeakTotalMemoryDistribution, "taskPeakTotalMemoryDistribution is null");
        this.firstFailedTask = requireNonNull(firstFailedTask, "firstFailedTask is null");
        this.minFirstStartTimeInMillis = minFirstStartTimeInMillis;
        this.maxLastStartTimeInMillis = maxLastStartTimeInMillis;
        this.maxEndTimeInMillis = maxEndTimeInMillis;
        this.processedInputPositionsSum = processedInputPositionsSum;
        this.processedInputPositionsM2 = processedInputPositionsM2;
        this.nodeIds = ImmutableSet.copyOf(requireNonNull(nodeIds, "nodeIds is null"));
        this.planNodeStats = ImmutableMap.copyOf(requireNonNull(planNodeStats, "planNodeStats is null"));
        this.planNodeStatsFailure = requireNonNull(planNodeStatsFailure, "planNodeStatsFailure is null");
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public int getTaskCount()
    {
        return taskCount;
    }

    /**
     * Distribution of the total CPU time of each task, in milliseconds.
     */
    public DistributionSnapshot getTaskCpuTimeMillisDistribution()
    {
        return taskCpuTimeMillisDistribution;
    }

    /**
     * Distribution of the peak total memory of each task, in bytes.
     */
    public DistributionSnapshot getTaskPeakTotalMemoryDistribution()
    {
        return taskPeakTotalMemoryDistribution;
    }

    /**
     * The first added task that is in the FAILED state.
     */
    public Optional<FailedTask> getFirstFailedTask()
    {
        return firstFailedTask;
    }

    /**
     * Smallest non-zero first start time of the tasks, or 0 if no task has one.
     */
    public long getMinFirstStartTimeInMillis()
    {
        return minFirstStartTimeInMillis;
    }

    /**
     * Largest non-zero last start time of the tasks, or 0 if no task has one.
     */
    public long getMaxLastStartTimeInMillis()
    {
        return maxLastStartTimeInMillis;
    }

    /**
     * Largest non-zero end time of the tasks, or 0 if no task has one.
     */
    public long getMaxEndTimeInMillis()
    {
        return maxEndTimeInMillis;
    }

    /**
     * Mean of the processed input positions of the tasks, NaN if there are no tasks.
     */
    public double getProcessedInputPositionsAverage()
    {
        if (taskCount == 0) {
            return Double.NaN;
        }
        return (double) processedInputPositionsSum / taskCount;
    }

    /**
     * Population standard deviation of the processed input positions of the tasks, NaN if there
     * are no tasks.
     */
    public double getProcessedInputPositionsStdDev()
    {
        return Math.sqrt(processedInputPositionsM2 / taskCount);
    }

    /**
     * Distinct ids of the nodes the tasks ran on.
     */
    public Set<String> getNodeIds()
    {
        return nodeIds;
    }

    /**
     * Plan node stats of the tasks, merged as by {@link PlanNodeStatsAccumulator}.
     *
     * @throws RuntimeException the exception that merging the plan node stats of the tasks failed with
     */
    public Map<PlanNodeId, PlanNodeStats> getPlanNodeStats()
    {
        if (planNodeStatsFailure.isPresent()) {
            throw planNodeStatsFailure.get();
        }
        return planNodeStats;
    }

    public static class FailedTask
    {
        private final TaskId taskId;
        private final URI self;
        private final String nodeId;

        public FailedTask(TaskId taskId, URI self, String nodeId)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.self = requireNonNull(self, "self is null");
            this.nodeId = requireNonNull(nodeId, "nodeId is null");
        }

        public static FailedTask from(TaskInfo taskInfo)
        {
            return new FailedTask(taskInfo.getTaskId(), taskInfo.getTaskStatus().getSelf(), taskInfo.getNodeId());
        }

        public TaskId getTaskId()
        {
            return taskId;
        }

        public URI getSelf()
        {
            return self;
        }

        public String getNodeId()
        {
            return nodeId;
        }
    }

    /**
     * The builder must not be used after {@link #build()}.
     */
    public static class Builder
    {
        private int taskCount;
        private final Distribution taskCpuTimeMillisDistribution = new Distribution();
        private final Distribution taskPeakTotalMemoryDistribution = new Distribution();
        private Optional<FailedTask> firstFailedTask = Optional.empty();
        private long minFirstStartTimeInMillis;
        private long maxLastStartTimeInMillis;
        private long maxEndTimeInMillis;
        private long processedInputPositionsSum;
        private double processedInputPositionsMean;
        private double processedInputPositionsM2;
        private final Set<String> nodeIds = new HashSet<>();
        // leaves the tasks unmodified, so a StageTaskStatsAggregator fed the same tasks gives the same stats whether it gets each task before or after this builder
        private final PlanNodeStatsAccumulator planNodeStats = new PlanNodeStatsAccumulator(true);
        private Optional<RuntimeException> planNodeStatsFailure = Optional.empty();
        private boolean built;

        private Builder() {}

        public Builder addTask(TaskInfo taskInfo)
        {
            checkState(!built, "build() has already been called");
            TaskStats taskStats = taskInfo.getStats();
            taskCount++;

            taskCpuTimeMillisDistribution.add(NANOSECONDS.toMillis(taskStats.getTotalCpuTimeInNanos()));
            taskPeakTotalMemoryDistribution.add(taskStats.getPeakTotalMemoryInBytes());

            if (!firstFailedTask.isPresent() && taskInfo.getTaskStatus().getState() == TaskState.FAILED) {
                firstFailedTask = Optional.of(FailedTask.from(taskInfo));
            }

            minFirstStartTimeInMillis = combineIgnoringZero(minFirstStartTimeInMillis, taskStats.getFirstStartTimeInMillis(), Math::min);
            maxLastStartTimeInMillis = combineIgnoringZero(maxLastStartTimeInMillis, taskStats.getLastStartTimeInMillis(), Math::max);
            maxEndTimeInMillis = combineIgnoringZero(maxEndTimeInMillis, taskStats.getEndTimeInMillis(), Math::max);

            long positions = taskStats.getProcessedInputPositions();
            processedInputPositionsSum += positions;
            double delta = positions - processedInputPositionsMean;
            processedInputPositionsMean += delta / taskCount;
            processedInputPositionsM2 += delta * (positions - processedInputPositionsMean);

            nodeIds.add(taskInfo.getNodeId());

            if (!planNodeStatsFailure.isPresent()) {
                try {
                    planNodeStats.add(taskInfo);
                }
                catch (RuntimeException e) {
                    // rethrown by getPlanNodeStats(), so the failure surfaces in the consumers of the plan node stats, as it would without a summary
                    planNodeStatsFailure = Optional.of(e);
                }
            }
            return this;
        }

        public StageExecutionTaskSummary build()
        {
            built = true;
            return new StageExecutionTaskSummary(
                    taskCount,
                    taskCpuTimeMillisDistribution.snapshot(),
                    taskPeakTotalMemoryDistribution.snapshot(),
                    firstFailedTask,
                    minFirstStartTimeInMillis,
                    maxLastStartTimeInMillis,
                    maxEndTimeInMillis,
                    processedInputPositionsSum,
                    processedInputPositionsM2,
                    nodeIds,
                    planNodeStats.build(),
                    planNodeStatsFailure);
        }

        private static long combineIgnoringZero(long current, long value, LongBinaryOperator combiner)
        {
            if (current == 0) {
                return value;
            }
            if (value == 0) {
                return current;
            }
            return combiner.applyAsLong(current, value);
        }
    }
}
