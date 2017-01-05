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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Lightweight version of QueryStats. Parts of the web UI depend on the fields
 * being named consistently across these classes.
 */
@Immutable
public class BasicQueryStats
{
    private final DateTime createTime;
    private final DateTime endTime;

    private final Duration elapsedTime;
    private final Duration executionTime;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int completedDrivers;

    private final double cumulativeMemory;
    private final DataSize totalMemoryReservation;
    private final DataSize peakMemoryReservation;
    private final Duration totalCpuTime;

    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    public BasicQueryStats(
            DateTime createTime,
            DateTime endTime,
            Duration elapsedTime,
            Duration executionTime,
            int totalDrivers,
            int queuedDrivers,
            int runningDrivers,
            int completedDrivers,
            double cumulativeMemory,
            DataSize totalMemoryReservation,
            DataSize peakMemoryReservation,
            Duration totalCpuTime,
            boolean fullyBlocked,
            Set<BlockedReason> blockedReasons)
    {
        this.createTime = createTime;
        this.endTime = endTime;

        this.elapsedTime = elapsedTime;
        this.executionTime = executionTime;

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
        checkArgument(completedDrivers >= 0, "completedDrivers is negative");
        this.completedDrivers = completedDrivers;

        this.cumulativeMemory = cumulativeMemory;
        this.totalMemoryReservation = totalMemoryReservation;
        this.peakMemoryReservation = peakMemoryReservation;
        this.totalCpuTime = totalCpuTime;

        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));
    }

    public BasicQueryStats(QueryStats queryStats)
    {
        this(queryStats.getCreateTime(),
                queryStats.getEndTime(),
                queryStats.getElapsedTime(),
                queryStats.getExecutionTime(),
                queryStats.getTotalDrivers(),
                queryStats.getQueuedDrivers(),
                queryStats.getRunningDrivers(),
                queryStats.getCompletedDrivers(),
                queryStats.getCumulativeMemory(),
                queryStats.getTotalMemoryReservation(),
                queryStats.getPeakMemoryReservation(),
                queryStats.getTotalCpuTime(),
                queryStats.isFullyBlocked(),
                queryStats.getBlockedReasons());
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
    public double getCumulativeMemory()
    {
        return cumulativeMemory;
    }

    @JsonProperty
    public DataSize getTotalMemoryReservation()
    {
        return totalMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakMemoryReservation()
    {
        return peakMemoryReservation;
    }

    @JsonProperty
    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
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
}
