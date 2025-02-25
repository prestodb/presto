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
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;

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
    private final DateTime schedulingComplete;

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

    private final double cumulativeUserMemory;
    private final double cumulativeTotalMemory;
    private final long userMemoryReservation;
    private final long totalMemoryReservation;
    private final long peakUserMemoryReservation;
    private final long peakNodeTotalMemoryReservation;

    private final Duration totalScheduledTime;
    private final Duration totalCpuTime;
    private final Duration retriedCpuTime;
    private final Duration totalBlockedTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final long totalAllocation;

    private final long rawInputDataSize;
    private final long rawInputPositions;

    private final long processedInputDataSize;
    private final long processedInputPositions;

    private final long bufferedDataSize;
    private final long outputDataSize;
    private final long outputPositions;

    private final long physicalWrittenDataSize;

    private final StageGcStatistics gcInfo;

    private final List<OperatorStats> operatorSummaries;

    // RuntimeStats aggregated at the stage level including the metrics exposed in each task and each operator of this stage.
    private final RuntimeStats runtimeStats;

    @JsonCreator
    public StageExecutionStats(
            @JsonProperty("schedulingComplete") DateTime schedulingComplete,

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

            @JsonProperty("cumulativeUserMemory") double cumulativeUserMemory,
            @JsonProperty("cumulativeTotalMemory") double cumulativeTotalMemory,
            @JsonProperty("userMemoryReservation") long userMemoryReservation,
            @JsonProperty("totalMemoryReservation") long totalMemoryReservation,
            @JsonProperty("peakUserMemoryReservation") long peakUserMemoryReservation,
            @JsonProperty("peakNodeTotalMemoryReservation") long peakNodeTotalMemoryReservation,

            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("retriedCpuTime") Duration retriedCpuTime,
            @JsonProperty("totalBlockedTime") Duration totalBlockedTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

            @JsonProperty("totalAllocation") long totalAllocation,

            @JsonProperty("rawInputDataSize") long rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,

            @JsonProperty("processedInputDataSize") long processedInputDataSize,
            @JsonProperty("processedInputPositions") long processedInputPositions,

            @JsonProperty("bufferedDataSize") long bufferedDataSize,
            @JsonProperty("outputDataSize") long outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("physicalWrittenDataSize") long physicalWrittenDataSize,

            @JsonProperty("gcInfo") StageGcStatistics gcInfo,

            @JsonProperty("operatorSummaries") List<OperatorStats> operatorSummaries,
            @JsonProperty("runtimeStats") RuntimeStats runtimeStats)
    {
        this.schedulingComplete = schedulingComplete;
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
        checkArgument(cumulativeUserMemory >= 0, "cumulativeUserMemory is negative");
        this.cumulativeUserMemory = cumulativeUserMemory;
        checkArgument(cumulativeTotalMemory >= 0, "cumulativeTotalMemory is negative");
        this.cumulativeTotalMemory = cumulativeTotalMemory;
        checkArgument(userMemoryReservation >= 0, "userMemoryReservation is negative");
        this.userMemoryReservation = userMemoryReservation;
        checkArgument(totalMemoryReservation >= 0, "totalMemoryReservation is negative");
        this.totalMemoryReservation = totalMemoryReservation;
        checkArgument(peakUserMemoryReservation >= 0, "peakUserMemoryReservation is negative");
        this.peakUserMemoryReservation = peakUserMemoryReservation;
        checkArgument(peakNodeTotalMemoryReservation >= 0, "peakNodeTotalMemoryReservation is negative");
        this.peakNodeTotalMemoryReservation = peakNodeTotalMemoryReservation;

        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.retriedCpuTime = requireNonNull(retriedCpuTime, "retriedCpuTime is null");
        this.totalBlockedTime = requireNonNull(totalBlockedTime, "totalBlockedTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        checkArgument(totalAllocation >= 0, "totalAllocation is negative");
        this.totalAllocation = totalAllocation;
        checkArgument(rawInputDataSize >= 0, "rawInputDataSize is negative");
        this.rawInputDataSize = rawInputDataSize;
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;

        checkArgument(processedInputDataSize >= 0, "processedInputDataSize is negative");
        this.processedInputDataSize = processedInputDataSize;
        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        checkArgument(bufferedDataSize >= 0, "bufferedDataSize is negative");
        this.bufferedDataSize = bufferedDataSize;
        checkArgument(outputDataSize >= 0, "outputDataSize is negative");
        this.outputDataSize = outputDataSize;
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        checkArgument(physicalWrittenDataSize >= 0, "writtenDataSize is negative");
        this.physicalWrittenDataSize = physicalWrittenDataSize;

        this.gcInfo = requireNonNull(gcInfo, "gcInfo is null");

        this.operatorSummaries = ImmutableList.copyOf(requireNonNull(operatorSummaries, "operatorSummaries is null"));
        this.runtimeStats = (runtimeStats == null) ? new RuntimeStats() : runtimeStats;
    }

    @JsonProperty
    public DateTime getSchedulingComplete()
    {
        return schedulingComplete;
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
    public long getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    @JsonProperty
    public long getTotalMemoryReservation()
    {
        return totalMemoryReservation;
    }

    @JsonProperty
    public long getPeakUserMemoryReservation()
    {
        return peakUserMemoryReservation;
    }

    @JsonProperty
    public long getPeakNodeTotalMemoryReservation()
    {
        return peakNodeTotalMemoryReservation;
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
    public long getTotalAllocation()
    {
        return totalAllocation;
    }

    @JsonProperty
    public long getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    @JsonProperty
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    public long getProcessedInputDataSize()
    {
        return processedInputDataSize;
    }

    @JsonProperty
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    @JsonProperty
    public long getBufferedDataSize()
    {
        return bufferedDataSize;
    }

    @JsonProperty
    public long getOutputDataSize()
    {
        return outputDataSize;
    }

    @JsonProperty
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    public long getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
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
                rawInputDataSize,
                rawInputPositions,
                cumulativeUserMemory,
                cumulativeTotalMemory,
                userMemoryReservation,
                totalMemoryReservation,
                totalCpuTime,
                totalScheduledTime,
                fullyBlocked,
                blockedReasons,
                totalAllocation,
                progressPercentage);
    }

    public static StageExecutionStats zero(int stageId)
    {
        return new StageExecutionStats(
                null,
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
