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

import com.facebook.airlift.stats.Distribution.DistributionSnapshot;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class PipelineStats
{
    private final int pipelineId;

    private final DateTime firstStartTime;
    private final DateTime lastStartTime;
    private final DateTime lastEndTime;

    private final boolean inputPipeline;
    private final boolean outputPipeline;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int queuedPartitionedDrivers;
    private final int runningDrivers;
    private final int runningPartitionedDrivers;
    private final int blockedDrivers;
    private final int completedDrivers;

    private final long userMemoryReservationInBytes;
    private final long revocableMemoryReservationInBytes;
    private final long systemMemoryReservationInBytes;

    private final DistributionSnapshot queuedTime;
    private final DistributionSnapshot elapsedTime;

    private final long totalScheduledTimeInNanos;
    private final long totalCpuTimeInNanos;
    private final long totalBlockedTimeInNanos;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final long totalAllocationInBytes;

    private final long rawInputDataSizeInBytes;
    private final long rawInputPositions;

    private final long processedInputDataSizeInBytes;
    private final long processedInputPositions;

    private final long outputDataSizeInBytes;
    private final long outputPositions;

    private final long physicalWrittenDataSizeInBytes;

    private final List<OperatorStats> operatorSummaries;
    private final List<DriverStats> drivers;

    @JsonCreator
    public PipelineStats(
            @JsonProperty("pipelineId") int pipelineId,

            @JsonProperty("firstStartTime") DateTime firstStartTime,
            @JsonProperty("lastStartTime") DateTime lastStartTime,
            @JsonProperty("lastEndTime") DateTime lastEndTime,

            @JsonProperty("inputPipeline") boolean inputPipeline,
            @JsonProperty("outputPipeline") boolean outputPipeline,

            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("queuedPartitionedDrivers") int queuedPartitionedDrivers,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("runningPartitionedDrivers") int runningPartitionedDrivers,
            @JsonProperty("blockedDrivers") int blockedDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,

            @JsonProperty("userMemoryReservationInBytes") long userMemoryReservationInBytes,
            @JsonProperty("revocableMemoryReservationInBytes") long revocableMemoryReservationInBytes,
            @JsonProperty("systemMemoryReservationInBytes") long systemMemoryReservationInBytes,

            @JsonProperty("queuedTime") DistributionSnapshot queuedTime,
            @JsonProperty("elapsedTime") DistributionSnapshot elapsedTime,

            @JsonProperty("totalScheduledTimeInNanos") long totalScheduledTimeInNanos,
            @JsonProperty("totalCpuTimeInNanos") long totalCpuTimeInNanos,
            @JsonProperty("totalBlockedTimeInNanos") long totalBlockedTimeInNanos,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

            @JsonProperty("totalAllocationInBytes") long totalAllocationInBytes,

            @JsonProperty("rawInputDataSizeInBytes") long rawInputDataSizeInBytes,
            @JsonProperty("rawInputPositions") long rawInputPositions,

            @JsonProperty("processedInputDataSizeInBytes") long processedInputDataSizeInBytes,
            @JsonProperty("processedInputPositions") long processedInputPositions,

            @JsonProperty("outputDataSizeInBytes") long outputDataSizeInBytes,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("physicalWrittenDataSizeInBytes") long physicalWrittenDataSizeInBytes,

            @JsonProperty("operatorSummaries") List<OperatorStats> operatorSummaries,
            @JsonProperty("drivers") List<DriverStats> drivers)
    {
        this.pipelineId = pipelineId;

        this.firstStartTime = firstStartTime;
        this.lastStartTime = lastStartTime;
        this.lastEndTime = lastEndTime;

        this.inputPipeline = inputPipeline;
        this.outputPipeline = outputPipeline;

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(queuedPartitionedDrivers >= 0, "queuedPartitionedDrivers is negative");
        this.queuedPartitionedDrivers = queuedPartitionedDrivers;
        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
        checkArgument(runningPartitionedDrivers >= 0, "runningPartitionedDrivers is negative");
        this.runningPartitionedDrivers = runningPartitionedDrivers;
        checkArgument(blockedDrivers >= 0, "blockedDrivers is negative");
        this.blockedDrivers = blockedDrivers;
        checkArgument(completedDrivers >= 0, "completedDrivers is negative");
        this.completedDrivers = completedDrivers;

        this.userMemoryReservationInBytes = userMemoryReservationInBytes;
        this.revocableMemoryReservationInBytes = revocableMemoryReservationInBytes;
        this.systemMemoryReservationInBytes = systemMemoryReservationInBytes;

        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.elapsedTime = requireNonNull(elapsedTime, "elapsedTime is null");
        this.totalScheduledTimeInNanos = totalScheduledTimeInNanos;

        this.totalCpuTimeInNanos = totalCpuTimeInNanos;
        this.totalBlockedTimeInNanos = totalBlockedTimeInNanos;
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.totalAllocationInBytes = totalAllocationInBytes;

        this.rawInputDataSizeInBytes = rawInputDataSizeInBytes;
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;

        this.processedInputDataSizeInBytes = processedInputDataSizeInBytes;
        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        this.outputDataSizeInBytes = outputDataSizeInBytes;
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        this.physicalWrittenDataSizeInBytes = physicalWrittenDataSizeInBytes;

        this.operatorSummaries = ImmutableList.copyOf(requireNonNull(operatorSummaries, "operatorSummaries is null"));
        this.drivers = ImmutableList.copyOf(requireNonNull(drivers, "drivers is null"));
    }

    @JsonProperty
    public int getPipelineId()
    {
        return pipelineId;
    }

    @Nullable
    @JsonProperty
    public DateTime getFirstStartTime()
    {
        return firstStartTime;
    }

    @Nullable
    @JsonProperty
    public DateTime getLastStartTime()
    {
        return lastStartTime;
    }

    @Nullable
    @JsonProperty
    public DateTime getLastEndTime()
    {
        return lastEndTime;
    }

    @JsonProperty
    public boolean isInputPipeline()
    {
        return inputPipeline;
    }

    @JsonProperty
    public boolean isOutputPipeline()
    {
        return outputPipeline;
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
    public int getQueuedPartitionedDrivers()
    {
        return queuedPartitionedDrivers;
    }

    @JsonProperty
    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    @JsonProperty
    public int getRunningPartitionedDrivers()
    {
        return runningPartitionedDrivers;
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
    public long getUserMemoryReservationInBytes()
    {
        return userMemoryReservationInBytes;
    }

    @JsonProperty
    public long getRevocableMemoryReservationInBytes()
    {
        return revocableMemoryReservationInBytes;
    }

    @JsonProperty
    public long getSystemMemoryReservationInBytes()
    {
        return systemMemoryReservationInBytes;
    }

    @JsonProperty
    public DistributionSnapshot getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    public DistributionSnapshot getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    public long getTotalScheduledTimeInNanos()
    {
        return totalScheduledTimeInNanos;
    }

    @JsonProperty
    public long getTotalCpuTimeInNanos()
    {
        return totalCpuTimeInNanos;
    }

    @JsonProperty
    public long getTotalBlockedTimeInNanos()
    {
        return totalBlockedTimeInNanos;
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
    public List<OperatorStats> getOperatorSummaries()
    {
        return operatorSummaries;
    }

    @JsonProperty
    public List<DriverStats> getDrivers()
    {
        return drivers;
    }

    public PipelineStats summarize()
    {
        return new PipelineStats(
                pipelineId,
                firstStartTime,
                lastStartTime,
                lastEndTime,
                inputPipeline,
                outputPipeline,
                totalDrivers,
                queuedDrivers,
                queuedPartitionedDrivers,
                runningDrivers,
                runningPartitionedDrivers,
                blockedDrivers,
                completedDrivers,
                userMemoryReservationInBytes,
                revocableMemoryReservationInBytes,
                systemMemoryReservationInBytes,
                queuedTime,
                elapsedTime,
                totalScheduledTimeInNanos,
                totalCpuTimeInNanos,
                totalBlockedTimeInNanos,
                fullyBlocked,
                blockedReasons,
                totalAllocationInBytes,
                rawInputDataSizeInBytes,
                rawInputPositions,
                processedInputDataSizeInBytes,
                processedInputPositions,
                outputDataSizeInBytes,
                outputPositions,
                physicalWrittenDataSizeInBytes,
                summarizeOperatorStats(operatorSummaries),
                ImmutableList.of());
    }

    private static List<OperatorStats> summarizeOperatorStats(List<OperatorStats> operatorSummaries)
    {
        // Use an exact size ImmutableList builder to avoid a redundant copy in the PipelineStats constructor
        ImmutableList.Builder<OperatorStats> results = ImmutableList.builderWithExpectedSize(operatorSummaries.size());
        for (OperatorStats operatorStats : operatorSummaries) {
            results.add(operatorStats.summarize());
        }
        return results.build();
    }
}
