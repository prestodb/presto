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

import com.facebook.presto.common.RuntimeStats;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TaskStats
{
    private final DateTime createTime;
    private final DateTime firstStartTime;
    private final DateTime lastStartTime;
    private final DateTime lastEndTime;
    private final DateTime endTime;

    private final long elapsedTimeInNanos;
    private final long queuedTimeInNanos;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int queuedPartitionedDrivers;
    private final int runningDrivers;
    private final int runningPartitionedDrivers;
    private final int blockedDrivers;
    private final int completedDrivers;

    private final double cumulativeUserMemory;
    private final double cumulativeTotalMemory;
    private final long userMemoryReservationInBytes;
    private final long revocableMemoryReservationInBytes;
    private final long systemMemoryReservationInBytes;

    private final long peakUserMemoryInBytes;
    private final long peakTotalMemoryInBytes;
    private final long peakNodeTotalMemoryInBytes;

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

    private final int fullGcCount;
    private final long fullGcTimeInMillis;

    private final List<PipelineStats> pipelines;

    // RuntimeStats aggregated at the task level including the metrics exposed in this task and each operator of this task.
    private final RuntimeStats runtimeStats;

    public TaskStats(DateTime createTime, DateTime endTime)
    {
        this(
                createTime,
                null,
                null,
                null,
                endTime,
                0L,
                0L,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0.0,
                0.0,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                false,
                ImmutableSet.of(),
                0L,
                0L,
                0,
                0L,
                0,
                0L,
                0,
                0L,
                0,
                0L,
                ImmutableList.of(),
                new RuntimeStats());
    }

    @JsonCreator
    public TaskStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("firstStartTime") DateTime firstStartTime,
            @JsonProperty("lastStartTime") DateTime lastStartTime,
            @JsonProperty("lastEndTime") DateTime lastEndTime,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("elapsedTimeInNanos") long elapsedTimeInNanos,
            @JsonProperty("queuedTimeInNanos") long queuedTimeInNanos,

            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("queuedPartitionedDrivers") int queuedPartitionedDrivers,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("runningPartitionedDrivers") int runningPartitionedDrivers,
            @JsonProperty("blockedDrivers") int blockedDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,

            @JsonProperty("cumulativeUserMemory") double cumulativeUserMemory,
            @JsonProperty("cumulativeTotalMemory") double cumulativeTotalMemory,
            @JsonProperty("userMemoryReservation") long userMemoryReservationInBytes,
            @JsonProperty("revocableMemoryReservationInBytes") long revocableMemoryReservationInBytes,
            @JsonProperty("systemMemoryReservationInBytes") long systemMemoryReservationInBytes,

            @JsonProperty("peakTotalMemoryInBytes") long peakTotalMemoryInBytes,
            @JsonProperty("peakUserMemoryInBytes") long peakUserMemoryInBytes,
            @JsonProperty("peakNodeTotalMemoryInbytes") long peakNodeTotalMemoryInBytes,

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

            @JsonProperty("fullGcCount") int fullGcCount,
            @JsonProperty("fullGcTimeInMillis") long fullGcTimeInMillis,

            @JsonProperty("pipelines") List<PipelineStats> pipelines,
            @JsonProperty("runtimeStats") RuntimeStats runtimeStats)
    {
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.firstStartTime = firstStartTime;
        this.lastStartTime = lastStartTime;
        this.lastEndTime = lastEndTime;
        this.endTime = endTime;
        this.elapsedTimeInNanos = elapsedTimeInNanos;
        this.queuedTimeInNanos = queuedTimeInNanos;

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

        this.cumulativeUserMemory = cumulativeUserMemory;
        this.cumulativeTotalMemory = cumulativeTotalMemory;
        this.userMemoryReservationInBytes = userMemoryReservationInBytes;
        this.revocableMemoryReservationInBytes = revocableMemoryReservationInBytes;
        this.systemMemoryReservationInBytes = systemMemoryReservationInBytes;

        this.peakTotalMemoryInBytes = peakTotalMemoryInBytes;
        this.peakUserMemoryInBytes = peakUserMemoryInBytes;
        this.peakNodeTotalMemoryInBytes = peakNodeTotalMemoryInBytes;

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

        checkArgument(fullGcCount >= 0, "fullGcCount is negative");
        this.fullGcCount = fullGcCount;
        this.fullGcTimeInMillis = fullGcTimeInMillis;

        this.pipelines = ImmutableList.copyOf(requireNonNull(pipelines, "pipelines is null"));
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
    }

    @JsonProperty
    public DateTime getCreateTime()
    {
        return createTime;
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

    @Nullable
    @JsonProperty
    public DateTime getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    public long getElapsedTimeInNanos()
    {
        return elapsedTimeInNanos;
    }

    @JsonProperty
    public long getQueuedTimeInNanos()
    {
        return queuedTimeInNanos;
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
    public long getPeakUserMemoryInBytes()
    {
        return peakUserMemoryInBytes;
    }

    @JsonProperty
    public long getPeakTotalMemoryInBytes()
    {
        return peakTotalMemoryInBytes;
    }

    @JsonProperty
    public long getPeakNodeTotalMemoryInBytes()
    {
        return peakNodeTotalMemoryInBytes;
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
    public List<PipelineStats> getPipelines()
    {
        return pipelines;
    }

    @JsonProperty
    public int getQueuedPartitionedDrivers()
    {
        return queuedPartitionedDrivers;
    }

    @JsonProperty
    public int getRunningPartitionedDrivers()
    {
        return runningPartitionedDrivers;
    }

    @JsonProperty
    public int getFullGcCount()
    {
        return fullGcCount;
    }

    @JsonProperty
    public long getFullGcTimeInMillis()
    {
        return fullGcTimeInMillis;
    }

    @JsonProperty
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    public TaskStats summarize()
    {
        return new TaskStats(
                createTime,
                firstStartTime,
                lastStartTime,
                lastEndTime,
                endTime,
                elapsedTimeInNanos,
                queuedTimeInNanos,
                totalDrivers,
                queuedDrivers,
                queuedPartitionedDrivers,
                runningDrivers,
                runningPartitionedDrivers,
                blockedDrivers,
                completedDrivers,
                cumulativeUserMemory,
                cumulativeTotalMemory,
                userMemoryReservationInBytes,
                revocableMemoryReservationInBytes,
                systemMemoryReservationInBytes,
                peakTotalMemoryInBytes,
                peakUserMemoryInBytes,
                peakNodeTotalMemoryInBytes,
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
                fullGcCount,
                fullGcTimeInMillis,
                ImmutableList.of(),
                runtimeStats);
    }

    public TaskStats summarizeFinal()
    {
        return new TaskStats(
                createTime,
                firstStartTime,
                lastStartTime,
                lastEndTime,
                endTime,
                elapsedTimeInNanos,
                queuedTimeInNanos,
                totalDrivers,
                queuedDrivers,
                queuedPartitionedDrivers,
                runningDrivers,
                runningPartitionedDrivers,
                blockedDrivers,
                completedDrivers,
                cumulativeUserMemory,
                cumulativeTotalMemory,
                userMemoryReservationInBytes,
                revocableMemoryReservationInBytes,
                systemMemoryReservationInBytes,
                peakTotalMemoryInBytes,
                peakUserMemoryInBytes,
                peakNodeTotalMemoryInBytes,
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
                fullGcCount,
                fullGcTimeInMillis,
                summarizePipelineStats(pipelines),
                runtimeStats);
    }

    private static List<PipelineStats> summarizePipelineStats(List<PipelineStats> pipelines)
    {
        // Use an exact size ImmutableList builder to avoid a redundant copy in the TaskStats constructor
        ImmutableList.Builder<PipelineStats> results = ImmutableList.builderWithExpectedSize(pipelines.size());
        for (PipelineStats pipeline : pipelines) {
            results.add(pipeline.summarize());
        }
        return results.build();
    }
}
