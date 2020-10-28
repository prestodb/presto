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

import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.operator.BlockedReason;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;

import java.util.OptionalDouble;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Lightweight version of QueryStats. Parts of the web UI depend on the fields
 * being named consistently across these classes.
 */
@Immutable
public class BasicQueryStats
{
    private final DateTime createTime;
    private final DateTime endTime;

    private final Duration queuedTime;
    private final Duration elapsedTime;
    private final Duration executionTime;

    private final int peakRunningTasks;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int completedDrivers;

    private final DataSize rawInputDataSize;
    private final long rawInputPositions;

    private final double cumulativeUserMemory;
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

    @JsonCreator
    public BasicQueryStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("elapsedTime") Duration elapsedTime,
            @JsonProperty("executionTime") Duration executionTime,
            @JsonProperty("peakRunningTasks") int peakRunningTasks,
            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,
            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,
            @JsonProperty("cumulativeUserMemory") double cumulativeUserMemory,
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
        this.createTime = createTime;
        this.endTime = endTime;

        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.elapsedTime = requireNonNull(elapsedTime, "elapsedTime is null");
        this.executionTime = requireNonNull(executionTime, "executionTime is null");

        this.peakRunningTasks = peakRunningTasks;

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
        checkArgument(completedDrivers >= 0, "completedDrivers is negative");
        this.completedDrivers = completedDrivers;

        this.rawInputDataSize = requireNonNull(rawInputDataSize);
        this.rawInputPositions = rawInputPositions;

        this.cumulativeUserMemory = cumulativeUserMemory;
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

    public BasicQueryStats(QueryStats queryStats)
    {
        this(queryStats.getCreateTime(),
                queryStats.getEndTime(),
                queryStats.getQueuedTime(),
                queryStats.getElapsedTime(),
                queryStats.getExecutionTime(),
                queryStats.getPeakRunningTasks(),
                queryStats.getTotalDrivers(),
                queryStats.getQueuedDrivers(),
                queryStats.getRunningDrivers(),
                queryStats.getCompletedDrivers(),
                queryStats.getRawInputDataSize(),
                queryStats.getRawInputPositions(),
                queryStats.getCumulativeUserMemory(),
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
        DateTime now = DateTime.now();
        return new BasicQueryStats(
                now,
                now,
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                0,
                0,
                0,
                0,
                0,
                new DataSize(0, BYTE),
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

    @JsonProperty
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public DateTime getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    public Duration getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    public Duration getExecutionTime()
    {
        return executionTime;
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
    public int getCompletedDrivers()
    {
        return completedDrivers;
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
    public double getCumulativeUserMemory()
    {
        return cumulativeUserMemory;
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

    public int getPeakRunningTasks()
    {
        return peakRunningTasks;
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
    public DataSize getPeakTaskTotalMemoryReservation()
    {
        return peakTaskTotalMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakNodeTotalMemorReservation()
    {
        return peakNodeTotalMemoryReservation;
    }

    @JsonProperty
    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    @JsonProperty
    public Duration getTotalScheduledTime()
    {
        return totalScheduledTime;
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
    public OptionalDouble getProgressPercentage()
    {
        return progressPercentage;
    }
}
