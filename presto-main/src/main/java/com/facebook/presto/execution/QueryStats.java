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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.ExchangeOperator;
import com.facebook.presto.operator.MergeOperator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.ScanFilterAndProjectOperator;
import com.facebook.presto.operator.TableScanOperator;
import com.facebook.presto.operator.TableWriterOperator;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.facebook.presto.sql.planner.PlanFragment;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.Duration.succinctDuration;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class QueryStats
{
    private final DateTime createTime;

    private final DateTime executionStartTime;
    private final DateTime lastHeartbeat;
    private final DateTime endTime;

    private final Duration elapsedTime;
    private final Duration waitingForPrerequisitesTime;
    private final Duration queuedTime;
    private final Duration resourceWaitingTime;
    private final Duration semanticAnalyzingTime;
    private final Duration columnAccessPermissionCheckingTime;
    private final Duration dispatchingTime;
    private final Duration executionTime;
    private final Duration analysisTime;
    private final Duration totalPlanningTime;
    private final Duration finishingTime;

    private final int totalTasks;
    private final int runningTasks;
    private final int peakRunningTasks;
    private final int completedTasks;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int blockedDrivers;
    private final int completedDrivers;

    private final double cumulativeUserMemory;
    private final double cumulativeTotalMemory;
    private final long userMemoryReservationInBytes;
    private final long totalMemoryReservationInBytes;
    private final long peakUserMemoryReservationInBytes;
    private final long peakTotalMemoryReservationInBytes;
    private final long peakTaskTotalMemoryInBytes;
    private final long peakTaskUserMemoryInBytes;
    private final long peakNodeTotalMemoryInBytes;

    private final boolean scheduled;
    private final Duration totalScheduledTime;
    private final Duration totalCpuTime;
    private final Duration retriedCpuTime;
    private final Duration totalBlockedTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final long totalAllocationInBytes;

    private final long rawInputDataSizeInBytes;
    private final long rawInputPositions;

    private final long processedInputDataSizeInBytes;
    private final long processedInputPositions;

    private final long shuffledDataSizeInBytes;
    private final long shuffledPositions;

    private final long outputDataSizeInBytes;
    private final long outputPositions;

    private final long writtenOutputPositions;
    private final long writtenOutputLogicalDataSizeInBytes;
    private final long writtenOutputPhysicalDataSizeInBytes;

    private final long writtenIntermediatePhysicalDataSizeInBytes;

    private final List<StageGcStatistics> stageGcStatistics;

    private final List<OperatorStats> operatorSummaries;

    // RuntimeStats aggregated at the query level including the metrics exposed in every task and every operator.
    private final RuntimeStats runtimeStats;

    @JsonCreator
    public QueryStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("executionStartTime") DateTime executionStartTime,
            @JsonProperty("lastHeartbeat") DateTime lastHeartbeat,
            @JsonProperty("endTime") DateTime endTime,

            @JsonProperty("elapsedTime") Duration elapsedTime,
            @JsonProperty("waitingForPrerequisitesTime") Duration waitingForPrerequisitesTime,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("resourceWaitingTime") Duration resourceWaitingTime,
            @JsonProperty("semanticAnalyzingTime") Duration semanticAnalyzingTime,
            @JsonProperty("columnAccessPermissionCheckingTime") Duration columnAccessPermissionCheckingTime,
            @JsonProperty("dispatchingTime") Duration dispatchingTime,
            @JsonProperty("executionTime") Duration executionTime,
            @JsonProperty("analysisTime") Duration analysisTime,
            @JsonProperty("totalPlanningTime") Duration totalPlanningTime,
            @JsonProperty("finishingTime") Duration finishingTime,

            @JsonProperty("totalTasks") int totalTasks,
            @JsonProperty("runningTasks") int runningTasks,
            @JsonProperty("peakRunningTasks") int peakRunningTasks,
            @JsonProperty("completedTasks") int completedTasks,

            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("blockedDrivers") int blockedDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,

            @JsonProperty("cumulativeUserMemory") double cumulativeUserMemory,
            @JsonProperty("cumulativeTotalMemory") double cumulativeTotalMemory,
            @JsonProperty("userMemoryReservationInBytes") long userMemoryReservationInBytes,
            @JsonProperty("totalMemoryReservationInBytes") long totalMemoryReservationInBytes,
            @JsonProperty("peakUserMemoryReservationInBytes") long peakUserMemoryReservationInBytes,
            @JsonProperty("peakTotalMemoryReservationInBytes") long peakTotalMemoryReservationInBytes,
            @JsonProperty("peakTaskUserMemoryInBytes") long peakTaskUserMemoryInBytes,
            @JsonProperty("peakTaskTotalMemoryInBytes") long peakTaskTotalMemoryInBytes,
            @JsonProperty("peakNodeTotalMemoryInBytes") long peakNodeTotalMemoryInBytes,

            @JsonProperty("scheduled") boolean scheduled,
            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("retriedCpuTime") Duration retriedCpuTime,
            @JsonProperty("totalBlockedTime") Duration totalBlockedTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

            @JsonProperty("totalAllocationInBytes") long totalAllocationInBytes,

            @JsonProperty("rawInputDataSizeInBytes") long rawInputDataSizeInBytes,
            @JsonProperty("rawInputPositions") long rawInputPositions,

            @JsonProperty("processedInputDataSizeInBytes") long processedInputDataSizeInBytes,
            @JsonProperty("processedInputPositions") long processedInputPositions,

            @JsonProperty("shuffledDataSizeInBytes") long shuffledDataSizeInBytes,
            @JsonProperty("shuffledPositions") long shuffledPositions,

            @JsonProperty("outputDataSizeInBytes") long outputDataSizeInBytes,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("writtenOutputPositions") long writtenOutputPositions,
            @JsonProperty("writtenOutputLogicalDataSizeInBytes") long writtenOutputLogicalDataSizeInBytes,
            @JsonProperty("writtenOutputPhysicalDataSizeInBytes") long writtenOutputPhysicalDataSizeInBytes,

            @JsonProperty("writtenIntermediatePhysicalDataSizeInBytes") long writtenIntermediatePhysicalDataSizeInBytes,

            @JsonProperty("stageGcStatistics") List<StageGcStatistics> stageGcStatistics,

            @JsonProperty("operatorSummaries") List<OperatorStats> operatorSummaries,

            @JsonProperty("runtimeStats") RuntimeStats runtimeStats)
    {
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.executionStartTime = executionStartTime;
        this.lastHeartbeat = requireNonNull(lastHeartbeat, "lastHeartbeat is null");
        this.endTime = endTime;

        this.elapsedTime = requireNonNull(elapsedTime, "elapsedTime is null");
        this.waitingForPrerequisitesTime = requireNonNull(waitingForPrerequisitesTime, "waitingForPrerequisitesTime is null");
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.resourceWaitingTime = requireNonNull(resourceWaitingTime, "resourceWaitingTime is null");
        this.semanticAnalyzingTime = requireNonNull(semanticAnalyzingTime, "semanticAnalyzingTime is null");
        this.columnAccessPermissionCheckingTime = requireNonNull(columnAccessPermissionCheckingTime, "columnAccessPermissionCheckingTime is null");
        this.dispatchingTime = requireNonNull(dispatchingTime, "dispatchingTime is null");
        this.executionTime = requireNonNull(executionTime, "executionTime is null");
        this.analysisTime = requireNonNull(analysisTime, "analysisTime is null");
        this.totalPlanningTime = requireNonNull(totalPlanningTime, "totalPlanningTime is null");
        this.finishingTime = requireNonNull(finishingTime, "finishingTime is null");

        checkArgument(totalTasks >= 0, "totalTasks is negative");
        this.totalTasks = totalTasks;
        checkArgument(runningTasks >= 0, "runningTasks is negative");
        this.runningTasks = runningTasks;
        checkArgument(peakRunningTasks >= 0, "peakRunningTasks is negative");
        this.peakRunningTasks = peakRunningTasks;
        checkArgument(completedTasks >= 0, "completedTasks is negative");
        this.completedTasks = completedTasks;

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
        checkArgument(blockedDrivers >= 0, "blockedDrivers is negative");
        this.blockedDrivers = blockedDrivers;
        checkArgument(completedDrivers >= 0, "completedDrivers is negative");
        this.completedDrivers = completedDrivers;
        checkArgument(cumulativeUserMemory >= 0, "cumulativeUserMemory is negative");
        this.cumulativeUserMemory = cumulativeUserMemory;
        checkArgument(cumulativeTotalMemory >= 0, "cumulativeTotalMemory is negative");
        this.cumulativeTotalMemory = cumulativeTotalMemory;
        checkArgument(userMemoryReservationInBytes >= 0, "userMemoryReservationInBytes is negative");
        this.userMemoryReservationInBytes = userMemoryReservationInBytes;
        checkArgument(totalMemoryReservationInBytes >= 0, "totalMemoryReservationInBytes is negative");
        this.totalMemoryReservationInBytes = totalMemoryReservationInBytes;
        checkArgument(peakUserMemoryReservationInBytes >= 0, "peakUserMemoryReservationInBytes is negative");
        this.peakUserMemoryReservationInBytes = peakUserMemoryReservationInBytes;
        checkArgument(peakTotalMemoryReservationInBytes >= 0, "peakTotalMemoryReservationInBytes is negative");
        this.peakTotalMemoryReservationInBytes = peakTotalMemoryReservationInBytes;
        checkArgument(peakTaskTotalMemoryInBytes >= 0, "peakTaskTotalMemoryInBytes is negative");
        this.peakTaskTotalMemoryInBytes = peakTaskTotalMemoryInBytes;
        checkArgument(peakTaskUserMemoryInBytes >= 0, "peakTaskUserMemoryInBytes is negative");
        this.peakTaskUserMemoryInBytes = peakTaskUserMemoryInBytes;
        checkArgument(peakNodeTotalMemoryInBytes >= 0, "peakNodeTotalMemoryInBytes is negative");
        this.peakNodeTotalMemoryInBytes = peakNodeTotalMemoryInBytes;
        this.scheduled = scheduled;
        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.retriedCpuTime = requireNonNull(retriedCpuTime, "totalCpuTime is null");
        this.totalBlockedTime = requireNonNull(totalBlockedTime, "totalBlockedTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        checkArgument(totalAllocationInBytes >= 0, "totalAllocationInBytes is negative");
        this.totalAllocationInBytes = totalAllocationInBytes;
        checkArgument(rawInputDataSizeInBytes >= 0, "rawInputDataSizeInBytes is negative");
        this.rawInputDataSizeInBytes = rawInputDataSizeInBytes;
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;

        checkArgument(processedInputDataSizeInBytes >= 0, "processedInputDataSizeInBytes is negative");
        this.processedInputDataSizeInBytes = processedInputDataSizeInBytes;
        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        checkArgument(shuffledDataSizeInBytes >= 0, "shuffledDataSizeInBytes is negative");
        this.shuffledDataSizeInBytes = shuffledDataSizeInBytes;
        checkArgument(shuffledPositions >= 0, "shuffledPositions is negative");
        this.shuffledPositions = shuffledPositions;

        checkArgument(outputDataSizeInBytes >= 0, "outputDataSizeInBytes is negative");
        this.outputDataSizeInBytes = outputDataSizeInBytes;
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        checkArgument(writtenOutputPositions >= 0, "writtenOutputPositions is negative");
        this.writtenOutputPositions = writtenOutputPositions;
        checkArgument(writtenOutputLogicalDataSizeInBytes >= 0, "writtenOutputLogicalDataSizeInBytes is negative");
        this.writtenOutputLogicalDataSizeInBytes = writtenOutputLogicalDataSizeInBytes;
        checkArgument(writtenOutputPhysicalDataSizeInBytes >= 0, "writtenOutputPhysicalDataSizeInBytes is negative");
        this.writtenOutputPhysicalDataSizeInBytes = writtenOutputPhysicalDataSizeInBytes;
        checkArgument(writtenIntermediatePhysicalDataSizeInBytes >= 0, "writtenIntermediatePhysicalDataSizeInBytes is negative");
        this.writtenIntermediatePhysicalDataSizeInBytes = writtenIntermediatePhysicalDataSizeInBytes;

        this.stageGcStatistics = ImmutableList.copyOf(requireNonNull(stageGcStatistics, "stageGcStatistics is null"));

        this.operatorSummaries = ImmutableList.copyOf(requireNonNull(operatorSummaries, "operatorSummaries is null"));

        this.runtimeStats = (runtimeStats == null) ? new RuntimeStats() : runtimeStats;
    }

    public static QueryStats create(
            QueryStateTimer queryStateTimer,
            Optional<StageInfo> rootStage,
            List<StageInfo> allStages,
            int peakRunningTasks,
            long peakUserMemoryReservation,
            long peakTotalMemoryReservation,
            long peakTaskUserMemory,
            long peakTaskTotalMemory,
            long peakNodeTotalMemory,
            RuntimeStats runtimeStats)
    {
        int totalTasks = 0;
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

        long shuffledDataSize = 0;
        long shuffledPositions = 0;

        long outputDataSize = 0;
        long outputPositions = 0;

        long writtenOutputPositions = 0;
        long writtenOutputLogicalDataSize = 0;
        long writtenOutputPhysicalDataSize = 0;

        long writtenIntermediatePhysicalDataSize = 0;

        ImmutableList.Builder<StageGcStatistics> stageGcStatistics = ImmutableList.builderWithExpectedSize(allStages.size());

        boolean fullyBlocked = rootStage.isPresent();
        Set<BlockedReason> blockedReasons = new HashSet<>();

        ImmutableList.Builder<OperatorStats> operatorStatsSummary = ImmutableList.builder();
        RuntimeStats mergedRuntimeStats = RuntimeStats.copyOf(runtimeStats);
        for (StageInfo stageInfo : allStages) {
            StageExecutionStats stageExecutionStats = stageInfo.getLatestAttemptExecutionInfo().getStats();
            totalTasks += stageExecutionStats.getTotalTasks();
            runningTasks += stageExecutionStats.getRunningTasks();
            completedTasks += stageExecutionStats.getCompletedTasks();

            totalDrivers += stageExecutionStats.getTotalDrivers();
            queuedDrivers += stageExecutionStats.getQueuedDrivers();
            runningDrivers += stageExecutionStats.getRunningDrivers();
            blockedDrivers += stageExecutionStats.getBlockedDrivers();
            completedDrivers += stageExecutionStats.getCompletedDrivers();

            cumulativeUserMemory += stageExecutionStats.getCumulativeUserMemory();
            cumulativeTotalMemory += stageExecutionStats.getCumulativeTotalMemory();
            userMemoryReservation += stageExecutionStats.getUserMemoryReservationInBytes();
            totalMemoryReservation += stageExecutionStats.getTotalMemoryReservationInBytes();
            totalScheduledTime += stageExecutionStats.getTotalScheduledTime().roundTo(MILLISECONDS);
            totalCpuTime += stageExecutionStats.getTotalCpuTime().roundTo(MILLISECONDS);
            retriedCpuTime += computeRetriedCpuTime(stageInfo);
            totalBlockedTime += stageExecutionStats.getTotalBlockedTime().roundTo(MILLISECONDS);
            if (!stageInfo.getLatestAttemptExecutionInfo().getState().isDone()) {
                fullyBlocked &= stageExecutionStats.isFullyBlocked();
                blockedReasons.addAll(stageExecutionStats.getBlockedReasons());
            }

            totalAllocation += stageExecutionStats.getTotalAllocationInBytes();

            if (stageInfo.getPlan().isPresent()) {
                PlanFragment plan = stageInfo.getPlan().get();
                for (OperatorStats operatorStats : stageExecutionStats.getOperatorSummaries()) {
                    // NOTE: we need to literally check each operator type to tell if the source is from table input or shuffled input. A stage can have input from both types of source.
                    String operatorType = operatorStats.getOperatorType();
                    if (operatorType.equals(ExchangeOperator.class.getSimpleName()) || operatorType.equals(MergeOperator.class.getSimpleName())) {
                        shuffledPositions += operatorStats.getRawInputPositions();
                        shuffledDataSize += operatorStats.getRawInputDataSizeInBytes();
                    }
                    else if (operatorType.equals(TableScanOperator.class.getSimpleName()) || operatorType.equals(ScanFilterAndProjectOperator.class.getSimpleName())) {
                        rawInputDataSize += operatorStats.getRawInputDataSizeInBytes();
                        rawInputPositions += operatorStats.getRawInputPositions();
                    }
                }
                processedInputDataSize += stageExecutionStats.getProcessedInputDataSizeInBytes();
                processedInputPositions += stageExecutionStats.getProcessedInputPositions();

                if (plan.isOutputTableWriterFragment()) {
                    writtenOutputPositions += stageExecutionStats.getOperatorSummaries().stream()
                            .filter(stats -> stats.getOperatorType().equals(TableWriterOperator.OPERATOR_TYPE))
                            .mapToLong(OperatorStats::getInputPositions)
                            .sum();
                    writtenOutputLogicalDataSize += stageExecutionStats.getOperatorSummaries().stream()
                            .filter(stats -> stats.getOperatorType().equals(TableWriterOperator.OPERATOR_TYPE))
                            .mapToLong(OperatorStats::getInputDataSizeInBytes)
                            .sum();
                    writtenOutputPhysicalDataSize += stageExecutionStats.getPhysicalWrittenDataSizeInBytes();
                }
                else {
                    writtenIntermediatePhysicalDataSize += stageExecutionStats.getPhysicalWrittenDataSizeInBytes();
                }
            }

            stageGcStatistics.add(stageExecutionStats.getGcInfo());

            operatorStatsSummary.addAll(stageExecutionStats.getOperatorSummaries());
            // We prepend each metric name with the stage id to avoid merging metrics across stages.
            int stageId = stageInfo.getStageId().getId();
            stageExecutionStats.getRuntimeStats().getMetrics().forEach((name, metric) -> {
                String metricName = String.format("S%d-%s", stageId, name);
                mergedRuntimeStats.mergeMetric(metricName, metric);
            });
        }

        if (rootStage.isPresent()) {
            StageExecutionStats outputStageStats = rootStage.get().getLatestAttemptExecutionInfo().getStats();
            outputDataSize += outputStageStats.getOutputDataSizeInBytes();
            outputPositions += outputStageStats.getOutputPositions();
        }

        boolean isScheduled = rootStage.isPresent() && allStages.stream()
                .map(StageInfo::getLatestAttemptExecutionInfo)
                .map(StageExecutionInfo::getState)
                .allMatch(state -> (state == StageExecutionState.RUNNING) || state.isDone());

        return new QueryStats(
                queryStateTimer.getCreateTime(),
                queryStateTimer.getExecutionStartTime().orElse(null),
                queryStateTimer.getLastHeartbeat(),
                queryStateTimer.getEndTime().orElse(null),

                queryStateTimer.getElapsedTime(),
                queryStateTimer.getWaitingForPrerequisitesTime(),
                queryStateTimer.getQueuedTime(),
                queryStateTimer.getResourceWaitingTime(),
                queryStateTimer.getSemanticAnalyzingTime(),
                queryStateTimer.getColumnAccessPermissionCheckingTime(),
                queryStateTimer.getDispatchingTime(),
                queryStateTimer.getExecutionTime(),
                queryStateTimer.getAnalysisTime(),
                queryStateTimer.getPlanningTime(),
                queryStateTimer.getFinishingTime(),

                totalTasks,
                runningTasks,
                peakRunningTasks,
                completedTasks,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                blockedDrivers,
                completedDrivers,

                cumulativeUserMemory,
                cumulativeTotalMemory,
                userMemoryReservation,
                totalMemoryReservation,
                peakUserMemoryReservation,
                peakTotalMemoryReservation,
                peakTaskUserMemory,
                peakTaskTotalMemory,
                peakNodeTotalMemory,

                isScheduled,

                succinctDuration(totalScheduledTime, MILLISECONDS),
                succinctDuration(totalCpuTime, MILLISECONDS),
                succinctDuration(retriedCpuTime, MILLISECONDS),
                succinctDuration(totalBlockedTime, MILLISECONDS),
                fullyBlocked,
                blockedReasons,

                totalAllocation,

                rawInputDataSize,
                rawInputPositions,
                processedInputDataSize,
                processedInputPositions,
                shuffledDataSize,
                shuffledPositions,
                outputDataSize,
                outputPositions,

                writtenOutputPositions,
                writtenOutputLogicalDataSize,
                writtenOutputPhysicalDataSize,

                writtenIntermediatePhysicalDataSize,

                stageGcStatistics.build(),

                operatorStatsSummary.build(),
                mergedRuntimeStats);
    }

    private static long computeRetriedCpuTime(StageInfo stageInfo)
    {
        long stageRetriedCpuTime = stageInfo.getPreviousAttemptsExecutionInfos().stream()
                .mapToLong(executionInfo -> executionInfo.getStats().getTotalCpuTime().roundTo(MILLISECONDS))
                .sum();
        long taskRetriedCpuTime = stageInfo.getLatestAttemptExecutionInfo().getStats().getRetriedCpuTime().roundTo(MILLISECONDS);
        return stageRetriedCpuTime + taskRetriedCpuTime;
    }

    public static QueryStats immediateFailureQueryStats()
    {
        DateTime now = DateTime.now();
        return new QueryStats(
                now,
                now,
                now,
                now,
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                false,
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                false,
                ImmutableSet.of(),
                0L,
                0L,
                0,
                0L,
                0,
                0L,
                0,
                0L,
                0,
                0,
                0L,
                0L,
                0L,
                ImmutableList.of(),
                ImmutableList.of(),
                new RuntimeStats());
    }

    @JsonProperty
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public DateTime getExecutionStartTime()
    {
        return executionStartTime;
    }

    @JsonProperty
    public DateTime getLastHeartbeat()
    {
        return lastHeartbeat;
    }

    @Nullable
    @JsonProperty
    public DateTime getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    public Duration getWaitingForPrerequisitesTime()
    {
        return waitingForPrerequisitesTime;
    }

    @JsonProperty
    public Duration getResourceWaitingTime()
    {
        return resourceWaitingTime;
    }

    @JsonProperty
    public Duration getSemanticAnalyzingTime()
    {
        return semanticAnalyzingTime;
    }

    @JsonProperty
    public Duration getColumnAccessPermissionCheckingTime()
    {
        return columnAccessPermissionCheckingTime;
    }

    @JsonProperty
    public Duration getDispatchingTime()
    {
        return dispatchingTime;
    }

    @JsonProperty
    public Duration getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    public Duration getExecutionTime()
    {
        return executionTime;
    }

    @JsonProperty
    public Duration getAnalysisTime()
    {
        return analysisTime;
    }

    @JsonProperty
    public Duration getTotalPlanningTime()
    {
        return totalPlanningTime;
    }

    @JsonProperty
    public Duration getFinishingTime()
    {
        return finishingTime;
    }

    @JsonProperty
    public int getTotalTasks()
    {
        return totalTasks;
    }

    @JsonProperty
    public int getRunningTasks()
    {
        return runningTasks;
    }

    @JsonProperty
    public int getPeakRunningTasks()
    {
        return peakRunningTasks;
    }

    @JsonProperty
    public int getCompletedTasks()
    {
        return completedTasks;
    }

    @JsonProperty
    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    @JsonProperty
    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    @JsonProperty
    public int getBlockedDrivers()
    {
        return blockedDrivers;
    }

    @JsonProperty
    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    @JsonProperty
    public double getCumulativeUserMemory()
    {
        return cumulativeUserMemory;
    }

    @JsonProperty
    public double getCumulativeTotalMemory()
    {
        return cumulativeTotalMemory;
    }

    @JsonProperty
    public long getUserMemoryReservationInBytes()
    {
        return userMemoryReservationInBytes;
    }

    @JsonProperty
    public long getTotalMemoryReservationInBytes()
    {
        return totalMemoryReservationInBytes;
    }

    @JsonProperty
    public long getPeakUserMemoryReservationInBytes()
    {
        return peakUserMemoryReservationInBytes;
    }

    @JsonProperty
    public long getPeakTotalMemoryReservationInBytes()
    {
        return peakTotalMemoryReservationInBytes;
    }

    @JsonProperty
    public long getPeakTaskTotalMemoryInBytes()
    {
        return peakTaskTotalMemoryInBytes;
    }

    @JsonProperty
    public long getPeakNodeTotalMemoryInBytes()
    {
        return peakNodeTotalMemoryInBytes;
    }

    @JsonProperty
    public long getPeakTaskUserMemoryInBytes()
    {
        return peakTaskUserMemoryInBytes;
    }

    @JsonProperty
    public boolean isScheduled()
    {
        return scheduled;
    }

    @JsonProperty
    public Duration getTotalScheduledTime()
    {
        return totalScheduledTime;
    }

    @JsonProperty
    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    @JsonProperty
    public Duration getRetriedCpuTime()
    {
        return retriedCpuTime;
    }

    @JsonProperty
    public Duration getTotalBlockedTime()
    {
        return totalBlockedTime;
    }

    @JsonProperty
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @JsonProperty
    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    @JsonProperty
    public long getTotalAllocationInBytes()
    {
        return totalAllocationInBytes;
    }

    @JsonProperty
    public long getRawInputDataSizeInBytes()
    {
        return rawInputDataSizeInBytes;
    }

    @JsonProperty
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    public long getProcessedInputDataSizeInBytes()
    {
        return processedInputDataSizeInBytes;
    }

    @JsonProperty
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    @JsonProperty
    public long getShuffledDataSizeInBytes()
    {
        return shuffledDataSizeInBytes;
    }

    @JsonProperty
    public long getShuffledPositions()
    {
        return shuffledPositions;
    }

    @JsonProperty
    public long getOutputDataSizeInBytes()
    {
        return outputDataSizeInBytes;
    }

    @JsonProperty
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    public long getWrittenOutputPositions()
    {
        return writtenOutputPositions;
    }

    @JsonProperty
    public long getWrittenOutputLogicalDataSizeInBytes()
    {
        return writtenOutputLogicalDataSizeInBytes;
    }

    @JsonProperty
    public long getWrittenOutputPhysicalDataSizeInBytes()
    {
        return writtenOutputPhysicalDataSizeInBytes;
    }

    @JsonProperty
    public long getWrittenIntermediatePhysicalDataSizeInBytes()
    {
        return writtenIntermediatePhysicalDataSizeInBytes;
    }

    @JsonProperty
    public List<StageGcStatistics> getStageGcStatistics()
    {
        return stageGcStatistics;
    }

    @JsonProperty
    public List<OperatorStats> getOperatorSummaries()
    {
        return operatorSummaries;
    }

    @JsonProperty
    public OptionalDouble getProgressPercentage()
    {
        if (!scheduled || totalDrivers == 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(min(100, (completedDrivers * 100.0) / totalDrivers));
    }

    @JsonProperty
    public long getSpilledDataSizeInBytes()
    {
        return operatorSummaries.stream()
                .mapToLong(OperatorStats::getSpilledDataSizeInBytes)
                .sum();
    }

    @JsonProperty
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }
}
