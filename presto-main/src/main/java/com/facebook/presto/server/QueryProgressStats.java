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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.OptionalDouble;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public class QueryProgressStats
{
    private final long elapsedTimeMillis;
    private final long queuedTimeMillis;
    private final long cpuTimeMillis;
    private final long scheduledTimeMillis;
    private final long currentMemoryBytes;
    private final long peakMemoryBytes;
    private final long peakTotalMemoryBytes;
    private final long peakTaskTotalMemoryBytes;
    private final long inputRows;
    private final long inputBytes;
    private final OptionalDouble progressPercentage;
    private final boolean blocked;

    @JsonCreator
    @ThriftConstructor
    public QueryProgressStats(
            @JsonProperty("elapsedTimeMillis") long elapsedTimeMillis,
            @JsonProperty("queuedTimeMillis") long queuedTimeMillis,
            @JsonProperty("cpuTimeMillis") long cpuTimeMillis,
            @JsonProperty("scheduledTimeMillis") long scheduledTimeMillis,
            @JsonProperty("currentMemoryBytes") long currentMemoryBytes,
            @JsonProperty("peakMemoryBytes") long peakMemoryBytes,
            @JsonProperty("peakTotalMemoryBytes") long peakTotalMemoryBytes,
            @JsonProperty("peakTaskTotalMemoryBytes") long peakTaskTotalMemoryBytes,
            @JsonProperty("inputRows") long inputRows,
            @JsonProperty("inputBytes") long inputBytes,
            @JsonProperty("blocked") boolean blocked,
            @JsonProperty("progressPercentage") OptionalDouble progressPercentage)
    {
        this.elapsedTimeMillis = elapsedTimeMillis;
        this.queuedTimeMillis = queuedTimeMillis;
        this.cpuTimeMillis = cpuTimeMillis;
        this.scheduledTimeMillis = scheduledTimeMillis;
        this.currentMemoryBytes = currentMemoryBytes;
        this.peakMemoryBytes = peakMemoryBytes;
        this.peakTotalMemoryBytes = peakTotalMemoryBytes;
        this.peakTaskTotalMemoryBytes = peakTaskTotalMemoryBytes;
        this.inputRows = inputRows;
        this.inputBytes = inputBytes;
        this.blocked = blocked;
        this.progressPercentage = requireNonNull(progressPercentage, "progressPercentage is null");
    }

    public static QueryProgressStats createQueryProgressStats(BasicQueryStats queryStats)
    {
        return new QueryProgressStats(
                queryStats.getElapsedTime().toMillis(),
                queryStats.getQueuedTime().toMillis(),
                queryStats.getTotalCpuTime().toMillis(),
                queryStats.getTotalScheduledTime().toMillis(),
                queryStats.getUserMemoryReservation().toBytes(),
                queryStats.getPeakUserMemoryReservation().toBytes(),
                queryStats.getPeakTotalMemoryReservation().toBytes(),
                queryStats.getPeakTaskTotalMemoryReservation().toBytes(),
                queryStats.getRawInputPositions(),
                queryStats.getRawInputDataSize().toBytes(),
                queryStats.isFullyBlocked(),
                queryStats.getProgressPercentage());
    }

    @ThriftField(1)
    @JsonProperty
    public long getElapsedTimeMillis()
    {
        return elapsedTimeMillis;
    }

    @ThriftField(2)
    @JsonProperty
    public long getQueuedTimeMillis()
    {
        return queuedTimeMillis;
    }

    @ThriftField(3)
    @JsonProperty
    public long getCpuTimeMillis()
    {
        return cpuTimeMillis;
    }

    @ThriftField(4)
    @JsonProperty
    public long getScheduledTimeMillis()
    {
        return scheduledTimeMillis;
    }

    @ThriftField(5)
    @JsonProperty
    public long getCurrentMemoryBytes()
    {
        return currentMemoryBytes;
    }

    @ThriftField(6)
    @JsonProperty
    public long getPeakMemoryBytes()
    {
        return peakMemoryBytes;
    }

    @ThriftField(7)
    @JsonProperty
    public long getPeakTotalMemoryBytes()
    {
        return peakTotalMemoryBytes;
    }

    @ThriftField(8)
    @JsonProperty
    public long getPeakTaskTotalMemoryBytes()
    {
        return peakTaskTotalMemoryBytes;
    }

    @ThriftField(9)
    @JsonProperty
    public long getInputRows()
    {
        return inputRows;
    }

    @ThriftField(10)
    @JsonProperty
    public long getInputBytes()
    {
        return inputBytes;
    }

    @ThriftField(11)
    @JsonProperty
    public boolean isBlocked()
    {
        return blocked;
    }

    @ThriftField(12)
    @JsonProperty
    public OptionalDouble getProgressPercentage()
    {
        return progressPercentage;
    }
}
