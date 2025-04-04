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
package com.facebook.presto.operator;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.execution.Lifespan;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Immutable
@ThriftStruct
public class DriverStats
{
    private final Lifespan lifespan;

    private final long createTimeInMillis;
    private final long startTimeInMillis;
    private final long endTimeInMillis;

    private final Duration queuedTime;
    private final Duration elapsedTime;

    private final long userMemoryReservationInBytes;
    private final long revocableMemoryReservationInBytes;
    private final long systemMemoryReservationInBytes;

    private final Duration totalScheduledTime;
    private final Duration totalCpuTime;
    private final Duration totalBlockedTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final long totalAllocationInBytes;

    private final long rawInputDataSizeInBytes;
    private final long rawInputPositions;
    private final Duration rawInputReadTime;

    private final long processedInputDataSizeInBytes;
    private final long processedInputPositions;

    private final long outputDataSizeInBytes;
    private final long outputPositions;

    private final long physicalWrittenDataSizeInBytes;

    private final List<OperatorStats> operatorStats;

    public DriverStats()
    {
        this.lifespan = null;

        this.createTimeInMillis = currentTimeMillis();
        this.startTimeInMillis = 0L;
        this.endTimeInMillis = 0L;
        this.queuedTime = new Duration(0, MILLISECONDS);
        this.elapsedTime = new Duration(0, MILLISECONDS);

        this.userMemoryReservationInBytes = 0L;
        this.revocableMemoryReservationInBytes = 0L;
        this.systemMemoryReservationInBytes = 0L;

        this.totalScheduledTime = new Duration(0, MILLISECONDS);
        this.totalCpuTime = new Duration(0, MILLISECONDS);
        this.totalBlockedTime = new Duration(0, MILLISECONDS);
        this.fullyBlocked = false;
        this.blockedReasons = ImmutableSet.of();

        this.totalAllocationInBytes = 0L;

        this.rawInputDataSizeInBytes = 0L;
        this.rawInputPositions = 0;
        this.rawInputReadTime = new Duration(0, MILLISECONDS);

        this.processedInputDataSizeInBytes = 0L;
        this.processedInputPositions = 0;

        this.outputDataSizeInBytes = 0L;
        this.outputPositions = 0;

        this.physicalWrittenDataSizeInBytes = 0L;

        this.operatorStats = ImmutableList.of();
    }

    @JsonCreator
    @ThriftConstructor
    public DriverStats(
            @JsonProperty("lifespan") Lifespan lifespan,

            @JsonProperty("createTimeInMillis") long createTimeInMillis,
            @JsonProperty("startTimeInMillis") long startTimeInMillis,
            @JsonProperty("endTimeInMillis") long endTimeInMillis,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("elapsedTime") Duration elapsedTime,

            @JsonProperty("userMemoryReservationInBytes") long userMemoryReservationInBytes,
            @JsonProperty("revocableMemoryReservationInBytes") long revocableMemoryReservationInBytes,
            @JsonProperty("systemMemoryReservationInBytes") long systemMemoryReservationInBytes,

            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("totalBlockedTime") Duration totalBlockedTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

            @JsonProperty("totalAllocationInBytes") long totalAllocationInBytes,

            @JsonProperty("rawInputDataSizeInBytes") long rawInputDataSizeInBytes,
            @JsonProperty("rawInputPositions") long rawInputPositions,
            @JsonProperty("rawInputReadTime") Duration rawInputReadTime,

            @JsonProperty("processedInputDataSizeInBytes") long processedInputDataSizeInBytes,
            @JsonProperty("processedInputPositions") long processedInputPositions,

            @JsonProperty("outputDataSizeInBytes") long outputDataSizeInBytes,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("physicalWrittenDataSizeInBytes") long physicalWrittenDataSizeInBytes,

            @JsonProperty("operatorStats") List<OperatorStats> operatorStats)
    {
        this.lifespan = lifespan;

        checkArgument(createTimeInMillis >= 0, "createTimeInMillis is negative");
        this.createTimeInMillis = createTimeInMillis;
        this.startTimeInMillis = startTimeInMillis;
        this.endTimeInMillis = endTimeInMillis;
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.elapsedTime = requireNonNull(elapsedTime, "elapsedTime is null");

        checkArgument(userMemoryReservationInBytes >= 0, "userMemoryReservationInBytes is negative");
        this.userMemoryReservationInBytes = userMemoryReservationInBytes;
        checkArgument(revocableMemoryReservationInBytes >= 0, "revocableMemoryReservationInBytes is negative");
        this.revocableMemoryReservationInBytes = revocableMemoryReservationInBytes;
        checkArgument(systemMemoryReservationInBytes >= 0, "systemMemoryReservationInBytes is negative");
        this.systemMemoryReservationInBytes = systemMemoryReservationInBytes;

        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.totalBlockedTime = requireNonNull(totalBlockedTime, "totalBlockedTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        checkArgument(totalAllocationInBytes >= 0, "totalAllocationInBytes is negative");
        this.totalAllocationInBytes = totalAllocationInBytes;

        checkArgument(rawInputDataSizeInBytes >= 0, "rawInputDataSizeInBytes is negative");
        this.rawInputDataSizeInBytes = rawInputDataSizeInBytes;

        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;
        this.rawInputReadTime = requireNonNull(rawInputReadTime, "rawInputReadTime is null");

        checkArgument(processedInputDataSizeInBytes >= 0, "processedInputDataSizeInBytes is negative");
        this.processedInputDataSizeInBytes = processedInputDataSizeInBytes;

        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        // An overflow could have occurred on this stat - handle this gracefully.
        this.outputDataSizeInBytes = (outputDataSizeInBytes >= 0) ? outputDataSizeInBytes : Long.MAX_VALUE;

        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        checkArgument(physicalWrittenDataSizeInBytes >= 0, "writtenDataSizeInBytes is negative");
        this.physicalWrittenDataSizeInBytes = physicalWrittenDataSizeInBytes;

        this.operatorStats = ImmutableList.copyOf(requireNonNull(operatorStats, "operatorStats is null"));
    }

    @JsonProperty
    @ThriftField(1)
    public Lifespan getLifespan()
    {
        return lifespan;
    }

    @JsonProperty
    @ThriftField(2)
    public long getCreateTimeInMillis()
    {
        return createTimeInMillis;
    }

    @Nullable
    @JsonProperty
    @ThriftField(3)
    public long getStartTimeInMillis()
    {
        return startTimeInMillis;
    }

    @Nullable
    @JsonProperty
    @ThriftField(4)
    public long getEndTimeInMillis()
    {
        return endTimeInMillis;
    }

    @JsonProperty
    @ThriftField(5)
    public Duration getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    @ThriftField(6)
    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    @ThriftField(7)
    public long getUserMemoryReservationInBytes()
    {
        return userMemoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(8)
    public long getRevocableMemoryReservationInBytes()
    {
        return revocableMemoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(9)
    public long getSystemMemoryReservationInBytes()
    {
        return systemMemoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(10)
    public Duration getTotalScheduledTime()
    {
        return totalScheduledTime;
    }

    @JsonProperty
    @ThriftField(11)
    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    @JsonProperty
    @ThriftField(12)
    public Duration getTotalBlockedTime()
    {
        return totalBlockedTime;
    }

    @JsonProperty
    @ThriftField(13)
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @JsonProperty
    @ThriftField(14)
    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    @JsonProperty
    @ThriftField(15)
    public long getTotalAllocationInBytes()
    {
        return totalAllocationInBytes;
    }

    @JsonProperty
    @ThriftField(16)
    public long getRawInputDataSizeInBytes()
    {
        return rawInputDataSizeInBytes;
    }

    @JsonProperty
    @ThriftField(17)
    public Duration getRawInputReadTime()
    {
        return rawInputReadTime;
    }

    @JsonProperty
    @ThriftField(18)
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    @ThriftField(19)
    public long getProcessedInputDataSizeInBytes()
    {
        return processedInputDataSizeInBytes;
    }

    @JsonProperty
    @ThriftField(20)
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    @JsonProperty
    @ThriftField(21)
    public long getOutputDataSizeInBytes()
    {
        return outputDataSizeInBytes;
    }

    @JsonProperty
    @ThriftField(22)
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    @ThriftField(23)
    public long getPhysicalWrittenDataSizeInBytes()
    {
        return physicalWrittenDataSizeInBytes;
    }

    @JsonProperty
    @ThriftField(24)
    public List<OperatorStats> getOperatorStats()
    {
        return operatorStats;
    }
}
