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
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.TableWriterOperator;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.facebook.presto.sql.planner.PlanFragment;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
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
    private final DataSize userMemoryReservation;
    private final DataSize totalMemoryReservation;
    private final DataSize peakUserMemoryReservation;
    private final DataSize peakTotalMemoryReservation;
    private final DataSize peakTaskTotalMemory;
    private final DataSize peakTaskUserMemory;
    private final DataSize peakNodeTotalMemory;

    private final boolean scheduled;
    private final Duration totalScheduledTime;
    private final Duration totalCpuTime;
    private final Duration retriedCpuTime;
    private final Duration totalBlockedTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final DataSize totalAllocation;

    private final DataSize rawInputDataSize;
    private final long rawInputPositions;

    private final DataSize processedInputDataSize;
    private final long processedInputPositions;

    private final DataSize outputDataSize;
    private final long outputPositions;

    private final long writtenOutputPositions;
    private final DataSize writtenOutputLogicalDataSize;
    private final DataSize writtenOutputPhysicalDataSize;

    private final DataSize writtenIntermediatePhysicalDataSize;

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
            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("totalMemoryReservation") DataSize totalMemoryReservation,
            @JsonProperty("peakUserMemoryReservation") DataSize peakUserMemoryReservation,
            @JsonProperty("peakTotalMemoryReservation") DataSize peakTotalMemoryReservation,
            @JsonProperty("peakTaskUserMemory") DataSize peakTaskUserMemory,
            @JsonProperty("peakTaskTotalMemory") DataSize peakTaskTotalMemory,
            @JsonProperty("peakNodeTotalMemory") DataSize peakNodeTotalMemory,

            @JsonProperty("scheduled") boolean scheduled,
            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("retriedCpuTime") Duration retriedCpuTime,
            @JsonProperty("totalBlockedTime") Duration totalBlockedTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

            @JsonProperty("totalAllocation") DataSize totalAllocation,

            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,

            @JsonProperty("processedInputDataSize") DataSize processedInputDataSize,
            @JsonProperty("processedInputPositions") long processedInputPositions,

            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("writtenOutputPositions") long writtenOutputPositions,
            @JsonProperty("writtenOutputLogicalDataSize") DataSize writtenOutputLogicalDataSize,
            @JsonProperty("writtenOutputPhysicalDataSize") DataSize writtenOutputPhysicalDataSize,

            @JsonProperty("writtenIntermediatePhysicalDataSize") DataSize writtenIntermediatePhysicalDataSize,

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
        this.userMemoryReservation = requireNonNull(userMemoryReservation, "userMemoryReservation is null");
        this.totalMemoryReservation = requireNonNull(totalMemoryReservation, "totalMemoryReservation is null");
        this.peakUserMemoryReservation = requireNonNull(peakUserMemoryReservation, "peakUserMemoryReservation is null");
        this.peakTotalMemoryReservation = requireNonNull(peakTotalMemoryReservation, "peakTotalMemoryReservation is null");
        this.peakTaskTotalMemory = requireNonNull(peakTaskTotalMemory, "peakTaskTotalMemory is null");
        this.peakTaskUserMemory = requireNonNull(peakTaskUserMemory, "peakTaskUserMemory is null");
        this.peakNodeTotalMemory = requireNonNull(peakNodeTotalMemory, "peakNodeTotalMemory is null");
        this.scheduled = scheduled;
        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.retriedCpuTime = requireNonNull(retriedCpuTime, "totalCpuTime is null");
        this.totalBlockedTime = requireNonNull(totalBlockedTime, "totalBlockedTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.totalAllocation = requireNonNull(totalAllocation, "totalAllocation is null");

        this.rawInputDataSize = requireNonNull(rawInputDataSize, "rawInputDataSize is null");
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;

        this.processedInputDataSize = requireNonNull(processedInputDataSize, "processedInputDataSize is null");
        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        this.outputDataSize = requireNonNull(outputDataSize, "outputDataSize is null");
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        checkArgument(writtenOutputPositions >= 0, "writtenOutputPositions is negative: %s", writtenOutputPositions);
        this.writtenOutputPositions = writtenOutputPositions;
        this.writtenOutputLogicalDataSize = requireNonNull(writtenOutputLogicalDataSize, "writtenOutputLogicalDataSize is null");
        this.writtenOutputPhysicalDataSize = requireNonNull(writtenOutputPhysicalDataSize, "writtenOutputPhysicalDataSize is null");
        this.writtenIntermediatePhysicalDataSize = requireNonNull(writtenIntermediatePhysicalDataSize, "writtenIntermediatePhysicalDataSize is null");

        this.stageGcStatistics = ImmutableList.copyOf(requireNonNull(stageGcStatistics, "stageGcStatistics is null"));

        this.operatorSummaries = ImmutableList.copyOf(requireNonNull(operatorSummaries, "operatorSummaries is null"));

        this.runtimeStats = (runtimeStats == null) ? new RuntimeStats() : runtimeStats;
    }

    public static QueryStats create(
            QueryStateTimer queryStateTimer,
            Optional<StageInfo> rootStage,
            int peakRunningTasks,
            DataSize peakUserMemoryReservation,
            DataSize peakTotalMemoryReservation,
            DataSize peakTaskUserMemory,
            DataSize peakTaskTotalMemory,
            DataSize peakNodeTotalMemory,
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

        long outputDataSize = 0;
        long outputPositions = 0;

        long writtenOutputPositions = 0;
        long writtenOutputLogicalDataSize = 0;
        long writtenOutputPhysicalDataSize = 0;

        long writtenIntermediatePhysicalDataSize = 0;

        ImmutableList.Builder<StageGcStatistics> stageGcStatistics = ImmutableList.builder();

        boolean fullyBlocked = rootStage.isPresent();
        Set<BlockedReason> blockedReasons = new HashSet<>();

        ImmutableList.Builder<OperatorStats> operatorStatsSummary = ImmutableList.builder();
        boolean completeInfo = true;
        RuntimeStats mergedRuntimeStats = RuntimeStats.copyOf(runtimeStats);
        for (StageInfo stageInfo : getAllStages(rootStage)) {
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
            userMemoryReservation += stageExecutionStats.getUserMemoryReservation().toBytes();
            totalMemoryReservation += stageExecutionStats.getTotalMemoryReservation().toBytes();
            totalScheduledTime += stageExecutionStats.getTotalScheduledTime().roundTo(MILLISECONDS);
            totalCpuTime += stageExecutionStats.getTotalCpuTime().roundTo(MILLISECONDS);
            retriedCpuTime += computeRetriedCpuTime(stageInfo);
            totalBlockedTime += stageExecutionStats.getTotalBlockedTime().roundTo(MILLISECONDS);
            if (!stageInfo.getLatestAttemptExecutionInfo().getState().isDone()) {
                fullyBlocked &= stageExecutionStats.isFullyBlocked();
                blockedReasons.addAll(stageExecutionStats.getBlockedReasons());
            }

            totalAllocation += stageExecutionStats.getTotalAllocation().toBytes();

            if (stageInfo.getPlan().isPresent()) {
                PlanFragment plan = stageInfo.getPlan().get();
                if (!plan.getTableScanSchedulingOrder().isEmpty()) {
                    rawInputDataSize += stageExecutionStats.getRawInputDataSize().toBytes();
                    rawInputPositions += stageExecutionStats.getRawInputPositions();

                    processedInputDataSize += stageExecutionStats.getProcessedInputDataSize().toBytes();
                    processedInputPositions += stageExecutionStats.getProcessedInputPositions();
                }

                if (plan.isOutputTableWriterFragment()) {
                    writtenOutputPositions += stageExecutionStats.getOperatorSummaries().stream()
                            .filter(stats -> stats.getOperatorType().equals(TableWriterOperator.class.getSimpleName()))
                            .mapToLong(OperatorStats::getInputPositions)
                            .sum();
                    writtenOutputLogicalDataSize += stageExecutionStats.getOperatorSummaries().stream()
                            .filter(stats -> stats.getOperatorType().equals(TableWriterOperator.class.getSimpleName()))
                            .mapToLong(stats -> stats.getInputDataSize().toBytes())
                            .sum();
                    writtenOutputPhysicalDataSize += stageExecutionStats.getPhysicalWrittenDataSize().toBytes();
                }
                else {
                    writtenIntermediatePhysicalDataSize += stageExecutionStats.getPhysicalWrittenDataSize().toBytes();
                }
            }

            stageGcStatistics.add(stageExecutionStats.getGcInfo());

            completeInfo = completeInfo && stageInfo.isFinalStageInfo();
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
            outputDataSize += outputStageStats.getOutputDataSize().toBytes();
            outputPositions += outputStageStats.getOutputPositions();
        }

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
                succinctBytes(userMemoryReservation),
                succinctBytes(totalMemoryReservation),
                peakUserMemoryReservation,
                peakTotalMemoryReservation,
                peakTaskUserMemory,
                peakTaskTotalMemory,
                peakNodeTotalMemory,

                isScheduled(rootStage),

                succinctDuration(totalScheduledTime, MILLISECONDS),
                succinctDuration(totalCpuTime, MILLISECONDS),
                succinctDuration(retriedCpuTime, MILLISECONDS),
                succinctDuration(totalBlockedTime, MILLISECONDS),
                fullyBlocked,
                blockedReasons,

                succinctBytes(totalAllocation),

                succinctBytes(rawInputDataSize),
                rawInputPositions,
                succinctBytes(processedInputDataSize),
                processedInputPositions,
                succinctBytes(outputDataSize),
                outputPositions,

                writtenOutputPositions,
                succinctBytes(writtenOutputLogicalDataSize),
                succinctBytes(writtenOutputPhysicalDataSize),

                succinctBytes(writtenIntermediatePhysicalDataSize),

                stageGcStatistics.build(),

                operatorStatsSummary.build(),
                mergedRuntimeStats);
    }

    private static boolean isScheduled(Optional<StageInfo> rootStage)
    {
        if (!rootStage.isPresent()) {
            return false;
        }
        return getAllStages(rootStage).stream()
                .map(StageInfo::getLatestAttemptExecutionInfo)
                .map(StageExecutionInfo::getState)
                .allMatch(state -> (state == StageExecutionState.RUNNING) || state.isDone());
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
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                false,
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                false,
                ImmutableSet.of(),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                0,
                new DataSize(0, BYTE),
                0,
                new DataSize(0, BYTE),
                0,
                0,
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
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
    public DataSize getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    @JsonProperty
    public DataSize getTotalMemoryReservation()
    {
        return totalMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakUserMemoryReservation()
    {
        return peakUserMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakTotalMemoryReservation()
    {
        return peakTotalMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakTaskTotalMemory()
    {
        return peakTaskTotalMemory;
    }

    @JsonProperty
    public DataSize getPeakNodeTotalMemory()
    {
        return peakNodeTotalMemory;
    }

    @JsonProperty
    public DataSize getPeakTaskUserMemory()
    {
        return peakTaskUserMemory;
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
    public DataSize getTotalAllocation()
    {
        return totalAllocation;
    }

    @JsonProperty
    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    @JsonProperty
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    public DataSize getProcessedInputDataSize()
    {
        return processedInputDataSize;
    }

    @JsonProperty
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    @JsonProperty
    public DataSize getOutputDataSize()
    {
        return outputDataSize;
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
    public DataSize getWrittenOutputLogicalDataSize()
    {
        return writtenOutputLogicalDataSize;
    }

    @JsonProperty
    public DataSize getWrittenOutputPhysicalDataSize()
    {
        return writtenOutputPhysicalDataSize;
    }

    @JsonProperty
    public DataSize getWrittenIntermediatePhysicalDataSize()
    {
        return writtenIntermediatePhysicalDataSize;
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
    public DataSize getSpilledDataSize()
    {
        return succinctBytes(operatorSummaries.stream()
                .mapToLong(stats -> stats.getSpilledDataSize().toBytes())
                .sum());
    }

    @JsonProperty
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }
}
