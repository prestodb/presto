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

package io.prestosql.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.OptionalDouble;

import static java.util.Objects.requireNonNull;

public class QueryProgressStats
{
    private final long elapsedTimeMillis;
    private final long queuedTimeMillis;
    private final long cpuTimeMillis;
    private final long scheduledTimeMillis;
    private final long currentMemoryBytes;
    private final long peakMemoryBytes;
    private final long inputRows;
    private final long inputBytes;
    private final OptionalDouble progressPercentage;
    private final boolean blocked;

    @JsonCreator
    public QueryProgressStats(
            @JsonProperty("elapsedTimeMillis") long elapsedTimeMillis,
            @JsonProperty("queuedTimeMillis") long queuedTimeMillis,
            @JsonProperty("cpuTimeMillis") long cpuTimeMillis,
            @JsonProperty("scheduledTimeMillis") long scheduledTimeMillis,
            @JsonProperty("currentMemoryBytes") long currentMemoryBytes,
            @JsonProperty("peakMemoryBytes") long peakMemoryBytes,
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
                queryStats.getRawInputPositions(),
                queryStats.getRawInputDataSize().toBytes(),
                queryStats.isFullyBlocked(),
                queryStats.getProgressPercentage());
    }

    @JsonProperty
    public long getElapsedTimeMillis()
    {
        return elapsedTimeMillis;
    }

    @JsonProperty
    public long getQueuedTimeMillis()
    {
        return queuedTimeMillis;
    }

    @JsonProperty
    public long getCpuTimeMillis()
    {
        return cpuTimeMillis;
    }

    @JsonProperty
    public long getScheduledTimeMillis()
    {
        return scheduledTimeMillis;
    }

    @JsonProperty
    public long getCurrentMemoryBytes()
    {
        return currentMemoryBytes;
    }

    @JsonProperty
    public long getPeakMemoryBytes()
    {
        return peakMemoryBytes;
    }

    @JsonProperty
    public long getInputRows()
    {
        return inputRows;
    }

    @JsonProperty
    public long getInputBytes()
    {
        return inputBytes;
    }

    @JsonProperty
    public boolean isBlocked()
    {
        return blocked;
    }

    @JsonProperty
    public OptionalDouble getProgressPercentage()
    {
        return progressPercentage;
    }
}
