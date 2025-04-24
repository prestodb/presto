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
package com.facebook.presto.server;

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.operator.BlockedReason;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import org.joda.time.DateTime;

import java.util.OptionalDouble;
import java.util.Set;

import static com.facebook.airlift.units.DataSize.Unit.BYTE;
import static com.facebook.presto.util.DateTimeUtils.toTimeStampInMillis;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Lightweight version of QueryStats. Parts of the web UI depend on the fields
 * being named consistently across these classes.
 */
@ThriftStruct
@Immutable
public class BasicQueryStats
{
    private final long createTimeInMillis;
    private final long endTimeInMillis;

    private final Duration waitingForPrerequisitesTime;
    private final Duration queuedTime;
    private final Duration elapsedTime;
    private final Duration executionTime;
    private final Duration analysisTime;

    private final int runningTasks;
    private final int peakRunningTasks;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int completedDrivers;

    private final int totalNewDrivers;
    private final int queuedNewDrivers;
    private final int runningNewDrivers;
    private final int completedNewDrivers;

    private final int totalSplits;
    private final int queuedSplits;
    private final int runningSplits;
    private final int completedSplits;

    private final DataSize rawInputDataSize;
    private final long rawInputPositions;

    private final double cumulativeUserMemory;
    private final double cumulativeTotalMemory;
    private final DataSize userMemoryReservation;
    private final DataSize totalMemoryReservation;
    private final DataSize peakUserMemoryReservation;
    private final DataSize peakTotalMemoryReservation;
    private final DataSize peakTaskTotalMemoryReservation;
    private final DataSize peakNodeTotalMemoryReservation;
    private final Duration totalCpuTime;
    private final Duration totalScheduledTime;

    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final DataSize totalAllocation;

    private final OptionalDouble progressPercentage;

    public BasicQueryStats(
            long createTimeInMillis,
            long endTimeInMillis,
            Duration waitingForPrerequisitesTime,
            Duration queuedTime,
            Duration elapsedTime,
            Duration executionTime,
            Duration analysisTime,
            int runningTasks,
            int peakRunningTasks,
            int totalDrivers,
            int queuedDrivers,
            int runningDrivers,
            int completedDrivers,
            int totalNewDrivers,
            int queuedNewDrivers,
            int runningNewDrivers,
            int completedNewDrivers,
            int totalSplits,
            int queuedSplits,
            int runningSplits,
            int completedSplits,
            DataSize rawInputDataSize,
            long rawInputPositions,
            double cumulativeUserMemory,
            double cumulativeTotalMemory,
            DataSize userMemoryReservation,
            DataSize totalMemoryReservation,
            DataSize peakUserMemoryReservation,
            DataSize peakTotalMemoryReservation,
            DataSize peakTaskTotalMemoryReservation,
            DataSize peakNodeTotalMemoryReservation,
            Duration totalCpuTime,
            Duration totalScheduledTime,
            boolean fullyBlocked,
            Set<BlockedReason> blockedReasons,
            DataSize totalAllocation,
            OptionalDouble progressPercentage)
    {
        this.createTimeInMillis = createTimeInMillis;
        this.endTimeInMillis = endTimeInMillis;

        this.waitingForPrerequisitesTime = requireNonNull(waitingForPrerequisitesTime, "waitingForPrerequisitesTimex is null");
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.elapsedTime = requireNonNull(elapsedTime, "elapsedTime is null");
        this.executionTime = requireNonNull(executionTime, "executionTime is null");
        this.analysisTime = requireNonNull(analysisTime, "analysisTime is null");
        this.runningTasks = runningTasks;
        this.peakRunningTasks = peakRunningTasks;

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
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

        this.rawInputDataSize = requireNonNull(rawInputDataSize);
        this.rawInputPositions = rawInputPositions;

        this.cumulativeUserMemory = cumulativeUserMemory;
        this.cumulativeTotalMemory = cumulativeTotalMemory;
        this.userMemoryReservation = userMemoryReservation;
        this.totalMemoryReservation = totalMemoryReservation;
        this.peakUserMemoryReservation = peakUserMemoryReservation;
        this.peakTotalMemoryReservation = peakTotalMemoryReservation;
        this.peakTaskTotalMemoryReservation = peakTaskTotalMemoryReservation;
        this.peakNodeTotalMemoryReservation = peakNodeTotalMemoryReservation;
        this.totalCpuTime = totalCpuTime;
        this.totalScheduledTime = totalScheduledTime;

        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.totalAllocation = requireNonNull(totalAllocation, "totalAllocation is null");

        this.progressPercentage = requireNonNull(progressPercentage, "progressPercentage is null");
    }

    @ThriftConstructor
    @JsonCreator
    public BasicQueryStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("waitingForPrerequisitesTime") Duration waitingForPrerequisitesTime,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("elapsedTime") Duration elapsedTime,
            @JsonProperty("executionTime") Duration executionTime,
            @JsonProperty("analysisTime") Duration analysisTime,
            @JsonProperty("runningTasks") int runningTasks,
            @JsonProperty("peakRunningTasks") int peakRunningTasks,
            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,
            @JsonProperty("totalNewDrivers") int totalNewDrivers,
            @JsonProperty("queuedNewDrivers") int queuedNewDrivers,
            @JsonProperty("runningNewDrivers") int runningNewDrivers,
            @JsonProperty("completedNewDrivers") int completedNewDrivers,
            @JsonProperty("totalSplits") int totalSplits,
            @JsonProperty("queuedSplits") int queuedSplits,
            @JsonProperty("runningSplits") int runningSplits,
            @JsonProperty("completedSplits") int completedSplits,
            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,
            @JsonProperty("cumulativeUserMemory") double cumulativeUserMemory,
            @JsonProperty("cumulativeTotalMemory") double cumulativeTotalMemory,
            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("totalMemoryReservation") DataSize totalMemoryReservation,
            @JsonProperty("peakUserMemoryReservation") DataSize peakUserMemoryReservation,
            @JsonProperty("peakTotalMemoryReservation") DataSize peakTotalMemoryReservation,
            @JsonProperty("peakTaskTotalMemoryReservation") DataSize peakTaskTotalMemoryReservation,
            @JsonProperty("peakNodeTotalMemoryReservation") DataSize peakNodeTotalMemoryReservation,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,
            @JsonProperty("totalAllocation") DataSize totalAllocation,
            @JsonProperty("progressPercentage") OptionalDouble progressPercentage)
    {
        this(toTimeStampInMillis(createTime),
                toTimeStampInMillis(endTime),
                waitingForPrerequisitesTime,
                queuedTime,
                elapsedTime,
                executionTime,
                analysisTime,
                runningTasks,
                peakRunningTasks,
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
                rawInputDataSize,
                rawInputPositions,
                cumulativeUserMemory,
                cumulativeTotalMemory,
                userMemoryReservation,
                totalMemoryReservation,
                peakUserMemoryReservation,
                peakTotalMemoryReservation,
                peakTaskTotalMemoryReservation,
                peakNodeTotalMemoryReservation,
                totalCpuTime,
                totalScheduledTime,
                fullyBlocked,
                blockedReasons,
                totalAllocation,
                progressPercentage);
    }

    public BasicQueryStats(QueryStats queryStats)
    {
        this(queryStats.getCreateTimeInMillis(),
                queryStats.getEndTimeInMillis(),
                queryStats.getWaitingForPrerequisitesTime(),
                queryStats.getQueuedTime(),
                queryStats.getElapsedTime(),
                queryStats.getExecutionTime(),
                queryStats.getAnalysisTime(),
                queryStats.getRunningTasks(),
                queryStats.getPeakRunningTasks(),
                queryStats.getTotalDrivers(),
                queryStats.getQueuedDrivers(),
                queryStats.getRunningDrivers(),
                queryStats.getCompletedDrivers(),
                queryStats.getTotalNewDrivers(),
                queryStats.getQueuedNewDrivers(),
                queryStats.getRunningNewDrivers(),
                queryStats.getCompletedNewDrivers(),
                queryStats.getTotalSplits(),
                queryStats.getQueuedSplits(),
                queryStats.getRunningSplits(),
                queryStats.getCompletedSplits(),
                queryStats.getRawInputDataSize(),
                queryStats.getRawInputPositions(),
                queryStats.getCumulativeUserMemory(),
                queryStats.getCumulativeTotalMemory(),
                queryStats.getUserMemoryReservation(),
                queryStats.getTotalMemoryReservation(),
                queryStats.getPeakUserMemoryReservation(),
                queryStats.getPeakTotalMemoryReservation(),
                queryStats.getPeakTaskTotalMemory(),
                queryStats.getPeakNodeTotalMemory(),
                queryStats.getTotalCpuTime(),
                queryStats.getTotalScheduledTime(),
                queryStats.isFullyBlocked(),
                queryStats.getBlockedReasons(),
                queryStats.getTotalAllocation(),
                queryStats.getProgressPercentage());
    }

    public static BasicQueryStats immediateFailureQueryStats()
    {
        long now = currentTimeMillis();
        return new BasicQueryStats(
                now,
                now,
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
                0,
                0,
                0,
                new DataSize(0, BYTE),
                0,
                0,
                0,
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                false,
                ImmutableSet.of(),
                new DataSize(0, BYTE),
                OptionalDouble.empty());
    }

    @ThriftField(1)
    @JsonProperty
    public DateTime getCreateTime()
    {
        return new DateTime(createTimeInMillis);
    }

    public long getCreateTimeInMillis()
    {
        return createTimeInMillis;
    }

    @ThriftField(2)
    @JsonProperty
    public DateTime getEndTime()
    {
        return new DateTime(endTimeInMillis);
    }

    public long getEndTimeInMillis()
    {
        return endTimeInMillis;
    }

    @ThriftField(3)
    @JsonProperty
    public Duration getQueuedTime()
    {
        return queuedTime;
    }

    @ThriftField(4)
    @JsonProperty
    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    @ThriftField(5)
    @JsonProperty
    public Duration getExecutionTime()
    {
        return executionTime;
    }

    @ThriftField(6)
    @JsonProperty
    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    @ThriftField(7)
    @JsonProperty
    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    @ThriftField(8)
    @JsonProperty
    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    @ThriftField(9)
    @JsonProperty
    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    @ThriftField(10)
    @JsonProperty
    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    @ThriftField(11)
    @JsonProperty
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @ThriftField(12)
    @JsonProperty
    public double getCumulativeUserMemory()
    {
        return cumulativeUserMemory;
    }

    @ThriftField(13)
    @JsonProperty
    public DataSize getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    @ThriftField(14)
    @JsonProperty
    public DataSize getTotalMemoryReservation()
    {
        return totalMemoryReservation;
    }

    @ThriftField(15)
    public int getPeakRunningTasks()
    {
        return peakRunningTasks;
    }

    @ThriftField(16)
    @JsonProperty
    public DataSize getPeakUserMemoryReservation()
    {
        return peakUserMemoryReservation;
    }

    @ThriftField(17)
    @JsonProperty
    public DataSize getPeakTotalMemoryReservation()
    {
        return peakTotalMemoryReservation;
    }

    @ThriftField(18)
    @JsonProperty
    public DataSize getPeakTaskTotalMemoryReservation()
    {
        return peakTaskTotalMemoryReservation;
    }

    @ThriftField(value = 19, name = "peakNodeTotalMemoryReservation")
    @JsonProperty
    public DataSize getPeakNodeTotalMemoryReservation()
    {
        return peakNodeTotalMemoryReservation;
    }

    @ThriftField(20)
    @JsonProperty
    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    @ThriftField(21)
    @JsonProperty
    public Duration getTotalScheduledTime()
    {
        return totalScheduledTime;
    }

    @ThriftField(22)
    @JsonProperty
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @ThriftField(23)
    @JsonProperty
    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    @ThriftField(24)
    @JsonProperty
    public DataSize getTotalAllocation()
    {
        return totalAllocation;
    }

    @ThriftField(25)
    @JsonProperty
    public OptionalDouble getProgressPercentage()
    {
        return progressPercentage;
    }

    @ThriftField(26)
    @JsonProperty
    public Duration getWaitingForPrerequisitesTime()
    {
        return waitingForPrerequisitesTime;
    }

    @ThriftField(27)
    @JsonProperty
    public double getCumulativeTotalMemory()
    {
        return cumulativeTotalMemory;
    }

    @ThriftField(28)
    @JsonProperty
    public int getRunningTasks()
    {
        return runningTasks;
    }

    @ThriftField(29)
    @JsonProperty
    public Duration getAnalysisTime()
    {
        return analysisTime;
    }

    @ThriftField(30)
    @JsonProperty
    public int getTotalSplits()
    {
        return totalSplits;
    }

    @ThriftField(31)
    @JsonProperty
    public int getQueuedSplits()
    {
        return queuedSplits;
    }

    @ThriftField(32)
    @JsonProperty
    public int getRunningSplits()
    {
        return runningSplits;
    }

    @ThriftField(33)
    @JsonProperty
    public int getCompletedSplits()
    {
        return completedSplits;
    }

    @ThriftField(34)
    @JsonProperty
    public int getTotalNewDrivers()
    {
        return totalNewDrivers;
    }

    @ThriftField(35)
    @JsonProperty
    public int getQueuedNewDrivers()
    {
        return queuedNewDrivers;
    }

    @ThriftField(36)
    @JsonProperty
    public int getRunningNewDrivers()
    {
        return runningNewDrivers;
    }

    @ThriftField(37)
    @JsonProperty
    public int getCompletedNewDrivers()
    {
        return completedNewDrivers;
    }
}
