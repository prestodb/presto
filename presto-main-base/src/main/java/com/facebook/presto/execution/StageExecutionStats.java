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
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;

import java.util.List;
import java.util.OptionalDouble;
import java.util.Set;

import static com.facebook.presto.execution.StageExecutionState.RUNNING;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Immutable
public class StageExecutionStats
{
    private final long schedulingCompleteInMillis;

    private final DistributionSnapshot getSplitDistribution;

    private final int totalTasks;
    private final int runningTasks;
    private final int completedTasks;

    private final int totalLifespans;
    private final int completedLifespans;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int blockedDrivers;
    private final int completedDrivers;

    private final int totalNewDrivers;
    private final int queuedNewDrivers;
    private final int runningNewDrivers;
    private final int completedNewDrivers;

    private final int totalSplits;
    private final int queuedSplits;
    private final int runningSplits;
    private final int completedSplits;

    private final double cumulativeUserMemory;
    private final double cumulativeTotalMemory;
    private final long userMemoryReservationInBytes;
    private final long totalMemoryReservationInBytes;
    private final long peakUserMemoryReservationInBytes;
    private final long peakNodeTotalMemoryReservationInBytes;

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

    private final long bufferedDataSizeInBytes;
    private final long outputDataSizeInBytes;
    private final long outputPositions;

    private final long physicalWrittenDataSizeInBytes;

    private final StageGcStatistics gcInfo;

    private final List<OperatorStats> operatorSummaries;

    // RuntimeStats aggregated at the stage level including the metrics exposed in each task and each operator of this stage.
    private final RuntimeStats runtimeStats;

    @JsonCreator
    public StageExecutionStats(
            @JsonProperty("schedulingCompleteInMillis") long schedulingCompleteInMillis,

            @JsonProperty("getSplitDistribution") DistributionSnapshot getSplitDistribution,

            @JsonProperty("totalTasks") int totalTasks,
            @JsonProperty("runningTasks") int runningTasks,
            @JsonProperty("completedTasks") int completedTasks,

            @JsonProperty("totalLifespans") int totalLifespans,
            @JsonProperty("completedLifespans") int completedLifespans,

            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("blockedDrivers") int blockedDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,

            @JsonProperty("totalNewDrivers") int totalNewDrivers,
            @JsonProperty("queuedNewDrivers") int queuedNewDrivers,
            @JsonProperty("runningNewDrivers") int runningNewDrivers,
            @JsonProperty("completedNewDrivers") int completedNewDrivers,

            @JsonProperty("totalSplits") int totalSplits,
            @JsonProperty("queuedSplits") int queuedSplits,
            @JsonProperty("runningSplits") int runningSplits,
            @JsonProperty("completedSplits") int completedSplits,

            @JsonProperty("cumulativeUserMemory") double cumulativeUserMemory,
            @JsonProperty("cumulativeTotalMemory") double cumulativeTotalMemory,
            @JsonProperty("userMemoryReservationInBytes") long userMemoryReservationInBytes,
            @JsonProperty("totalMemoryReservationInBytes") long totalMemoryReservationInBytes,
            @JsonProperty("peakUserMemoryReservationInBytes") long peakUserMemoryReservationInBytes,
            @JsonProperty("peakNodeTotalMemoryReservationInBytes") long peakNodeTotalMemoryReservationInBytes,

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

            @JsonProperty("bufferedDataSizeInBytes") long bufferedDataSizeInBytes,
            @JsonProperty("outputDataSizeInBytes") long outputDataSizeInBytes,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("physicalWrittenDataSizeInBytes") long physicalWrittenDataSizeInBytes,

            @JsonProperty("gcInfo") StageGcStatistics gcInfo,

            @JsonProperty("operatorSummaries") List<OperatorStats> operatorSummaries,
            @JsonProperty("runtimeStats") RuntimeStats runtimeStats)
    {
        this.schedulingCompleteInMillis = schedulingCompleteInMillis;
        this.getSplitDistribution = requireNonNull(getSplitDistribution, "getSplitDistribution is null");

        checkArgument(totalTasks >= 0, "totalTasks is negative");
        this.totalTasks = totalTasks;
        checkArgument(runningTasks >= 0, "runningTasks is negative");
        this.runningTasks = runningTasks;
        checkArgument(completedTasks >= 0, "completedTasks is negative");
        this.completedTasks = completedTasks;

        checkArgument(totalLifespans >= 0, "completedLifespans is negative");
        this.totalLifespans = totalLifespans;
        checkArgument(completedLifespans >= 0, "completedLifespans is negative");
        this.completedLifespans = completedLifespans;

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

        checkArgument(totalNewDrivers >= 0, "totalNewDrivers is negative");
        this.totalNewDrivers = totalNewDrivers;
        checkArgument(queuedNewDrivers >= 0, "queuedNewDrivers is negative");
        this.queuedNewDrivers = queuedNewDrivers;
        checkArgument(runningNewDrivers >= 0, "runningNewDrivers is negative");
        this.runningNewDrivers = runningNewDrivers;
        checkArgument(completedNewDrivers >= 0, "completedNewDrivers is negative");
        this.completedNewDrivers = completedNewDrivers;

        checkArgument(totalSplits >= 0, "totalSplits is negative");
        this.totalSplits = totalSplits;
        checkArgument(queuedSplits >= 0, "queuedSplits is negative");
        this.queuedSplits = queuedSplits;
        checkArgument(runningSplits >= 0, "runningSplits is negative");
        this.runningSplits = runningSplits;
        checkArgument(completedSplits >= 0, "completedSplits is negative");
        this.completedSplits = completedSplits;

        this.cumulativeUserMemory = (cumulativeUserMemory >= 0) ? cumulativeUserMemory : Long.MAX_VALUE;
        this.cumulativeTotalMemory = (cumulativeTotalMemory >= 0) ? cumulativeTotalMemory : Long.MAX_VALUE;
        this.userMemoryReservationInBytes = (userMemoryReservationInBytes >= 0) ? userMemoryReservationInBytes : Long.MAX_VALUE;
        this.totalMemoryReservationInBytes = (totalMemoryReservationInBytes >= 0) ? totalMemoryReservationInBytes : Long.MAX_VALUE;
        this.peakUserMemoryReservationInBytes = (peakUserMemoryReservationInBytes >= 0) ? peakUserMemoryReservationInBytes : Long.MAX_VALUE;
        this.peakNodeTotalMemoryReservationInBytes = (peakNodeTotalMemoryReservationInBytes >= 0) ? peakNodeTotalMemoryReservationInBytes : Long.MAX_VALUE;

        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.retriedCpuTime = requireNonNull(retriedCpuTime, "retriedCpuTime is null");
        this.totalBlockedTime = requireNonNull(totalBlockedTime, "totalBlockedTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.totalAllocationInBytes = (totalAllocationInBytes >= 0) ? totalAllocationInBytes : Long.MAX_VALUE;
        this.rawInputDataSizeInBytes = (rawInputDataSizeInBytes >= 0) ? rawInputDataSizeInBytes : Long.MAX_VALUE;
        this.rawInputPositions = (rawInputPositions >= 0) ? rawInputPositions : Long.MAX_VALUE;

        this.processedInputDataSizeInBytes = (processedInputDataSizeInBytes >= 0) ? processedInputDataSizeInBytes : Long.MAX_VALUE;
        this.processedInputPositions = (processedInputPositions >= 0) ? processedInputPositions : Long.MAX_VALUE;

        this.bufferedDataSizeInBytes = (bufferedDataSizeInBytes >= 0) ? bufferedDataSizeInBytes : Long.MAX_VALUE;

        this.outputDataSizeInBytes = (outputDataSizeInBytes >= 0) ? outputDataSizeInBytes : Long.MAX_VALUE;

        this.outputPositions = (outputPositions >= 0) ? outputPositions : Long.MAX_VALUE;

        this.physicalWrittenDataSizeInBytes = (physicalWrittenDataSizeInBytes >= 0) ? physicalWrittenDataSizeInBytes : Long.MAX_VALUE;

        this.gcInfo = requireNonNull(gcInfo, "gcInfo is null");

        this.operatorSummaries = ImmutableList.copyOf(requireNonNull(operatorSummaries, "operatorSummaries is null"));
        this.runtimeStats = (runtimeStats == null) ? new RuntimeStats() : runtimeStats;
    }

    @JsonProperty
    public long getSchedulingCompleteInMillis()
    {
        return schedulingCompleteInMillis;
    }

    @JsonProperty
    public DistributionSnapshot getGetSplitDistribution()
    {
        return getSplitDistribution;
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
    public int getCompletedTasks()
    {
        return completedTasks;
    }

    @JsonProperty
    public int getTotalLifespans()
    {
        return totalLifespans;
    }

    @JsonProperty
    public int getCompletedLifespans()
    {
        return completedLifespans;
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
    public int getTotalNewDrivers()
    {
        return totalNewDrivers;
    }

    @JsonProperty
    public int getQueuedNewDrivers()
    {
        return queuedNewDrivers;
    }

    @JsonProperty
    public int getRunningNewDrivers()
    {
        return runningNewDrivers;
    }

    @JsonProperty
    public int getCompletedNewDrivers()
    {
        return completedNewDrivers;
    }

    @JsonProperty
    public int getTotalSplits()
    {
        return totalSplits;
    }

    @JsonProperty
    public int getQueuedSplits()
    {
        return queuedSplits;
    }

    @JsonProperty
    public int getRunningSplits()
    {
        return runningSplits;
    }

    @JsonProperty
    public int getCompletedSplits()
    {
        return completedSplits;
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
    public long getPeakNodeTotalMemoryReservationInBytes()
    {
        return peakNodeTotalMemoryReservationInBytes;
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
    public long getBufferedDataSizeInBytes()
    {
        return bufferedDataSizeInBytes;
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
    public long getPhysicalWrittenDataSizeInBytes()
    {
        return physicalWrittenDataSizeInBytes;
    }

    @JsonProperty
    public StageGcStatistics getGcInfo()
    {
        return gcInfo;
    }

    @JsonProperty
    public List<OperatorStats> getOperatorSummaries()
    {
        return operatorSummaries;
    }

    @JsonProperty
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    public BasicStageExecutionStats toBasicStageStats(StageExecutionState stageExecutionState)
    {
        boolean isScheduled = (stageExecutionState == RUNNING) || stageExecutionState.isDone();

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
                rawInputPositions,
                cumulativeUserMemory,
                cumulativeTotalMemory,
                userMemoryReservationInBytes,
                totalMemoryReservationInBytes,
                totalCpuTime,
                totalScheduledTime,
                fullyBlocked,
                blockedReasons,
                totalAllocationInBytes,
                progressPercentage);
    }

    public static StageExecutionStats zero(int stageId)
    {
        return new StageExecutionStats(
                0L,
                new Distribution().snapshot(),
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
                new Duration(0, NANOSECONDS),
                new Duration(0, NANOSECONDS),
                new Duration(0, NANOSECONDS),
                new Duration(0, NANOSECONDS),
                false,
                ImmutableSet.of(),
                0L,
                0L,
                0,
                0L,
                0,
                0L,
                0L,
                0,
                0L,
                new StageGcStatistics(stageId, 0, 0, 0, 0, 0, 0, 0),
                ImmutableList.of(),
                new RuntimeStats());
    }
}
