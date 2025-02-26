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
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Immutable
@ThriftStruct
public class DriverStats
{
    private final Lifespan lifespan;

    private final DateTime createTime;
    private final DateTime startTime;
    private final DateTime endTime;

    private final Duration queuedTime;
    private final Duration elapsedTime;

    private final long userMemoryReservation;
    private final long revocableMemoryReservation;
    private final long systemMemoryReservation;

    private final Duration totalScheduledTime;
    private final Duration totalCpuTime;
    private final Duration totalBlockedTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final long totalAllocation;

    private final long rawInputDataSize;
    private final long rawInputPositions;
    private final Duration rawInputReadTime;

    private final long processedInputDataSize;
    private final long processedInputPositions;

    private final long outputDataSize;
    private final long outputPositions;

    private final long physicalWrittenDataSize;

    private final List<OperatorStats> operatorStats;

    public DriverStats()
    {
        this.lifespan = null;

        this.createTime = DateTime.now();
        this.startTime = null;
        this.endTime = null;
        this.queuedTime = new Duration(0, MILLISECONDS);
        this.elapsedTime = new Duration(0, MILLISECONDS);

        this.userMemoryReservation = 0L;
        this.revocableMemoryReservation = 0L;
        this.systemMemoryReservation = 0L;

        this.totalScheduledTime = new Duration(0, MILLISECONDS);
        this.totalCpuTime = new Duration(0, MILLISECONDS);
        this.totalBlockedTime = new Duration(0, MILLISECONDS);
        this.fullyBlocked = false;
        this.blockedReasons = ImmutableSet.of();

        this.totalAllocation = 0L;

        this.rawInputDataSize = 0L;
        this.rawInputPositions = 0;
        this.rawInputReadTime = new Duration(0, MILLISECONDS);

        this.processedInputDataSize = 0L;
        this.processedInputPositions = 0;

        this.outputDataSize = 0L;
        this.outputPositions = 0;

        this.physicalWrittenDataSize = 0L;

        this.operatorStats = ImmutableList.of();
    }

    @JsonCreator
    @ThriftConstructor
    public DriverStats(
            @JsonProperty("lifespan") Lifespan lifespan,

            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("startTime") DateTime startTime,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("elapsedTime") Duration elapsedTime,

            @JsonProperty("userMemoryReservation") long userMemoryReservation,
            @JsonProperty("revocableMemoryReservation") long revocableMemoryReservation,
            @JsonProperty("systemMemoryReservation") long systemMemoryReservation,

            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("totalBlockedTime") Duration totalBlockedTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

            @JsonProperty("totalAllocation") long totalAllocation,

            @JsonProperty("rawInputDataSize") long rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,
            @JsonProperty("rawInputReadTime") Duration rawInputReadTime,

            @JsonProperty("processedInputDataSize") long processedInputDataSize,
            @JsonProperty("processedInputPositions") long processedInputPositions,

            @JsonProperty("outputDataSize") long outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("physicalWrittenDataSize") long physicalWrittenDataSize,

            @JsonProperty("operatorStats") List<OperatorStats> operatorStats)
    {
        this.lifespan = lifespan;

        this.createTime = requireNonNull(createTime, "createTime is null");
        this.startTime = startTime;
        this.endTime = endTime;
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.elapsedTime = requireNonNull(elapsedTime, "elapsedTime is null");

        checkArgument(userMemoryReservation >= 0, "userMemoryReservation is negative");
        this.userMemoryReservation = userMemoryReservation;
        checkArgument(revocableMemoryReservation >= 0, "revocableMemoryReservation is negative");
        this.revocableMemoryReservation = revocableMemoryReservation;
        checkArgument(systemMemoryReservation >= 0, "systemMemoryReservation is negative");
        this.systemMemoryReservation = systemMemoryReservation;

        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.totalBlockedTime = requireNonNull(totalBlockedTime, "totalBlockedTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        checkArgument(totalAllocation >= 0, "totalAllocation is negative");
        this.totalAllocation = totalAllocation;

        checkArgument(rawInputDataSize >= 0, "rawInputDataSize is negative");
        this.rawInputDataSize = rawInputDataSize;

        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;
        this.rawInputReadTime = requireNonNull(rawInputReadTime, "rawInputReadTime is null");

        checkArgument(processedInputDataSize >= 0, "processedInputDataSize is negative");
        this.processedInputDataSize = processedInputDataSize;

        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        checkArgument(outputDataSize >= 0, "outputDataSize is negative");
        this.outputDataSize = outputDataSize;

        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        checkArgument(physicalWrittenDataSize >= 0, "writtenDataSize is negative");
        this.physicalWrittenDataSize = physicalWrittenDataSize;

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
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @Nullable
    @JsonProperty
    @ThriftField(3)
    public DateTime getStartTime()
    {
        return startTime;
    }

    @Nullable
    @JsonProperty
    @ThriftField(4)
    public DateTime getEndTime()
    {
        return endTime;
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
    public long getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    @JsonProperty
    @ThriftField(8)
    public long getRevocableMemoryReservation()
    {
        return revocableMemoryReservation;
    }

    @JsonProperty
    @ThriftField(9)
    public long getSystemMemoryReservation()
    {
        return systemMemoryReservation;
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
    public long getTotalAllocation()
    {
        return totalAllocation;
    }

    @JsonProperty
    @ThriftField(16)
    public long getRawInputDataSize()
    {
        return rawInputDataSize;
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
    public long getProcessedInputDataSize()
    {
        return processedInputDataSize;
    }

    @JsonProperty
    @ThriftField(20)
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    @JsonProperty
    @ThriftField(21)
    public long getOutputDataSize()
    {
        return outputDataSize;
    }

    @JsonProperty
    @ThriftField(22)
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    @ThriftField(23)
    public long getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
    }

    @JsonProperty
    @ThriftField(24)
    public List<OperatorStats> getOperatorStats()
    {
        return operatorStats;
    }
}
