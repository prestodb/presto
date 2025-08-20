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
import com.google.errorprone.annotations.Immutable;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;

@Immutable
@ThriftStruct
public class DriverStats
{
    private final Lifespan lifespan;

    private final long createTimeInMillis;
    private final long startTimeInMillis;
    private final long endTimeInMillis;

    private final long queuedTimeInNanos;
    private final long elapsedTimeInNanos;

    private final long userMemoryReservationInBytes;
    private final long revocableMemoryReservationInBytes;
    private final long systemMemoryReservationInBytes;

    private final long totalScheduledTimeInNanos;
    private final long totalCpuTimeInNanos;
    private final long totalBlockedTimeInNanos;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final long totalAllocationInBytes;

    private final long rawInputDataSizeInBytes;
    private final long rawInputPositions;
    private final long rawInputReadTimeInNanos;

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
        this.queuedTimeInNanos = 0;
        this.elapsedTimeInNanos = 0;

        this.userMemoryReservationInBytes = 0L;
        this.revocableMemoryReservationInBytes = 0L;
        this.systemMemoryReservationInBytes = 0L;

        this.totalScheduledTimeInNanos = 0;
        this.totalCpuTimeInNanos = 0;
        this.totalBlockedTimeInNanos = 0;
        this.fullyBlocked = false;
        this.blockedReasons = ImmutableSet.of();

        this.totalAllocationInBytes = 0L;

        this.rawInputDataSizeInBytes = 0L;
        this.rawInputPositions = 0;
        this.rawInputReadTimeInNanos = 0;

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
            @JsonProperty("queuedTimeInNanos") long queuedTimeInNanos,
            @JsonProperty("elapsedTimeInNanos") long elapsedTimeInNanos,

            @JsonProperty("userMemoryReservationInBytes") long userMemoryReservationInBytes,
            @JsonProperty("revocableMemoryReservationInBytes") long revocableMemoryReservationInBytes,
            @JsonProperty("systemMemoryReservationInBytes") long systemMemoryReservationInBytes,

            @JsonProperty("totalScheduledTimeInNanos") long totalScheduledTimeInNanos,
            @JsonProperty("totalCpuTimeInNanos") long totalCpuTimeInNanos,
            @JsonProperty("totalBlockedTimeInNanos") long totalBlockedTimeInNanos,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

            @JsonProperty("totalAllocationInBytes") long totalAllocationInBytes,

            @JsonProperty("rawInputDataSizeInBytes") long rawInputDataSizeInBytes,
            @JsonProperty("rawInputPositions") long rawInputPositions,
            @JsonProperty("rawInputReadTimeInNanos") long rawInputReadTimeInNanos,

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
        this.queuedTimeInNanos = queuedTimeInNanos;
        this.elapsedTimeInNanos = elapsedTimeInNanos;

        this.userMemoryReservationInBytes = (userMemoryReservationInBytes >= 0) ? userMemoryReservationInBytes : Long.MAX_VALUE;
        this.revocableMemoryReservationInBytes = (revocableMemoryReservationInBytes >= 0) ? revocableMemoryReservationInBytes : Long.MAX_VALUE;
        this.systemMemoryReservationInBytes = (systemMemoryReservationInBytes >= 0) ? systemMemoryReservationInBytes : Long.MAX_VALUE;

        this.totalScheduledTimeInNanos = totalScheduledTimeInNanos;
        this.totalCpuTimeInNanos = totalCpuTimeInNanos;
        this.totalBlockedTimeInNanos = totalBlockedTimeInNanos;
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.totalAllocationInBytes = (totalAllocationInBytes >= 0) ? totalAllocationInBytes : Long.MAX_VALUE;

        this.rawInputDataSizeInBytes = (rawInputDataSizeInBytes >= 0) ? rawInputDataSizeInBytes : Long.MAX_VALUE;

        this.rawInputPositions = (rawInputPositions >= 0) ? rawInputPositions : Long.MAX_VALUE;
        this.rawInputReadTimeInNanos = rawInputReadTimeInNanos;

        this.processedInputDataSizeInBytes = (processedInputDataSizeInBytes >= 0) ? processedInputDataSizeInBytes : Long.MAX_VALUE;

        this.processedInputPositions = (processedInputPositions >= 0) ? processedInputPositions : Long.MAX_VALUE;

        this.outputDataSizeInBytes = (outputDataSizeInBytes >= 0) ? outputDataSizeInBytes : Long.MAX_VALUE;

        this.outputPositions = (outputPositions >= 0) ? outputPositions : Long.MAX_VALUE;

        this.physicalWrittenDataSizeInBytes = (physicalWrittenDataSizeInBytes >= 0) ? physicalWrittenDataSizeInBytes : Long.MAX_VALUE;

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
    public long getQueuedTimeInNanos()
    {
        return queuedTimeInNanos;
    }

    @JsonProperty
    @ThriftField(6)
    public long getElapsedTimeInNanos()
    {
        return elapsedTimeInNanos;
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
    public long getTotalScheduledTimeInNanos()
    {
        return totalScheduledTimeInNanos;
    }

    @JsonProperty
    @ThriftField(11)
    public long getTotalCpuTimeInNanos()
    {
        return totalCpuTimeInNanos;
    }

    @JsonProperty
    @ThriftField(12)
    public long getTotalBlockedTimeInNanos()
    {
        return totalBlockedTimeInNanos;
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
    public long getRawInputReadTimeInNanos()
    {
        return rawInputReadTimeInNanos;
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
