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
import com.facebook.presto.common.RuntimeStats;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.util.DateTimeUtils.toTimeStampInMillis;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class TaskStats
{
    private final long createTimeInMillis;
    private final long firstStartTimeInMillis;
    private final long lastStartTimeInMillis;
    private final long lastEndTimeInMillis;
    private final long endTimeInMillis;

    private final long elapsedTimeInNanos;
    private final long queuedTimeInNanos;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int queuedPartitionedDrivers;
    private final long queuedPartitionedSplitsWeight;
    private final int runningDrivers;
    private final int runningPartitionedDrivers;
    private final long runningPartitionedSplitsWeight;
    private final int blockedDrivers;
    private final int completedDrivers;

    private final int totalNewDrivers;
    private final int queuedNewDrivers;
    private final int runningNewDrivers;
    private final int completedNewDrivers;

    private final int totalSplits;
    private final int queuedSplits;
    private final int runningSplits;
    private final int completedSplits;

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

    public TaskStats(long createTimeInMillis, long endTimeInMillis)
    {
        this(createTimeInMillis,
                0L,
                0L,
                0L,
                endTimeInMillis,
                0L,
                0L,
                0,
                0,
                0,
                0L,
                0,
                0,
                0L,
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

    public TaskStats(DateTime createTime, DateTime endTime)
    {
        this(toTimeStampInMillis(createTime),
                0L,
                0L,
                0L,
                toTimeStampInMillis(endTime),
                0L,
                0L,
                0,
                0,
                0,
                0L,
                0,
                0,
                0L,
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
    @ThriftConstructor
    public TaskStats(
            @JsonProperty("createTimeInMillis") long createTimeInMillis,
            @JsonProperty("firstStartTimeInMillis") long firstStartTimeInMillis,
            @JsonProperty("lastStartTimeInMillis") long lastStartTimeInMillis,
            @JsonProperty("lastEndTimeInMillis") long lastEndTimeInMillis,
            @JsonProperty("endTimeInMillis") long endTimeInMillis,
            @JsonProperty("elapsedTimeInNanos") long elapsedTimeInNanos,
            @JsonProperty("queuedTimeInNanos") long queuedTimeInNanos,

            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("queuedPartitionedDrivers") int queuedPartitionedDrivers,
            @JsonProperty("queuedPartitionedSplitsWeight") long queuedPartitionedSplitsWeight,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("runningPartitionedDrivers") int runningPartitionedDrivers,
            @JsonProperty("runningPartitionedSplitsWeight") long runningPartitionedSplitsWeight,
            @JsonProperty("blockedDrivers") int blockedDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,

            @JsonProperty("totalNewDrivers") int totalNewDrivers,
            @JsonProperty("queuedNewDrivers") int queuedNewDrivers,
            @JsonProperty("runningNewDrivers") int runningNewDrivers,
            @JsonProperty("completedNewDrivers") int completedNewDrivers,

            @JsonProperty("totalSplits") int totalSplits,
            @JsonProperty("queuedSplits") int queuedSplits,
            @JsonProperty("runningSplits") int runningSplits,
            @JsonProperty("completedSplits") int completedSplits,

            @JsonProperty("cumulativeUserMemory") double cumulativeUserMemory,
            @JsonProperty("cumulativeTotalMemory") double cumulativeTotalMemory,
            @JsonProperty("userMemoryReservationInBytes") long userMemoryReservationInBytes,
            @JsonProperty("revocableMemoryReservationInBytes") long revocableMemoryReservationInBytes,
            @JsonProperty("systemMemoryReservationInBytes") long systemMemoryReservationInBytes,

            @JsonProperty("peakTotalMemoryInBytes") long peakTotalMemoryInBytes,
            @JsonProperty("peakUserMemoryInBytes") long peakUserMemoryInBytes,
            @JsonProperty("peakNodeTotalMemoryInBytes") long peakNodeTotalMemoryInBytes,

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
        checkArgument(createTimeInMillis >= 0, "createTimeInMillis is negative");
        this.createTimeInMillis = createTimeInMillis;
        this.firstStartTimeInMillis = firstStartTimeInMillis;
        this.lastStartTimeInMillis = lastStartTimeInMillis;
        this.lastEndTimeInMillis = lastEndTimeInMillis;
        this.endTimeInMillis = endTimeInMillis;

        this.elapsedTimeInNanos = elapsedTimeInNanos;
        this.queuedTimeInNanos = queuedTimeInNanos;

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(queuedPartitionedDrivers >= 0, "queuedPartitionedDrivers is negative");
        this.queuedPartitionedDrivers = queuedPartitionedDrivers;
        checkArgument(queuedPartitionedSplitsWeight >= 0, "queuedPartitionedSplitsWeight must be non-negative");
        this.queuedPartitionedSplitsWeight = queuedPartitionedSplitsWeight;

        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
        checkArgument(runningPartitionedDrivers >= 0, "runningPartitionedDrivers is negative");
        this.runningPartitionedDrivers = runningPartitionedDrivers;
        checkArgument(runningPartitionedSplitsWeight >= 0, "runningPartitionedSplitsWeight must be non-negative");
        this.runningPartitionedSplitsWeight = runningPartitionedSplitsWeight;

        checkArgument(blockedDrivers >= 0, "blockedDrivers is negative");
        this.blockedDrivers = blockedDrivers;

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
        this.rawInputPositions = (rawInputPositions >= 0) ? rawInputPositions : Long.MAX_VALUE;

        this.processedInputDataSizeInBytes = processedInputDataSizeInBytes;
        this.processedInputPositions = (processedInputPositions >= 0) ? processedInputPositions : Long.MAX_VALUE;

        this.outputDataSizeInBytes = outputDataSizeInBytes;
        this.outputPositions = (outputPositions >= 0) ? outputPositions : Long.MAX_VALUE;

        this.physicalWrittenDataSizeInBytes = physicalWrittenDataSizeInBytes;

        checkArgument(fullGcCount >= 0, "fullGcCount is negative");
        this.fullGcCount = fullGcCount;
        this.fullGcTimeInMillis = fullGcTimeInMillis;

        this.pipelines = ImmutableList.copyOf(requireNonNull(pipelines, "pipelines is null"));
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
    }

    public DateTime getCreateTime()
    {
        return new DateTime(createTimeInMillis);
    }

    @JsonProperty
    @ThriftField(1)
    public long getCreateTimeInMillis()
    {
        return createTimeInMillis;
    }

    public DateTime getFirstStartTime()
    {
        return new DateTime(firstStartTimeInMillis);
    }

    @JsonProperty
    @ThriftField(2)
    public long getFirstStartTimeInMillis()
    {
        return firstStartTimeInMillis;
    }

    public DateTime getLastStartTime()
    {
        return new DateTime(lastStartTimeInMillis);
    }

    @JsonProperty
    @ThriftField(3)
    public long getLastStartTimeInMillis()
    {
        return lastStartTimeInMillis;
    }

    public DateTime getLastEndTime()
    {
        return new DateTime(lastEndTimeInMillis);
    }

    @JsonProperty
    @ThriftField(4)
    public long getLastEndTimeInMillis()
    {
        return lastEndTimeInMillis;
    }

    public DateTime getEndTime()
    {
        return new DateTime(endTimeInMillis);
    }

    @JsonProperty
    @ThriftField(5)
    public long getEndTimeInMillis()
    {
        return endTimeInMillis;
    }

    @JsonProperty
    @ThriftField(6)
    public long getElapsedTimeInNanos()
    {
        return elapsedTimeInNanos;
    }

    @JsonProperty
    @ThriftField(7)
    public long getQueuedTimeInNanos()
    {
        return queuedTimeInNanos;
    }

    @JsonProperty
    @ThriftField(8)
    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    @ThriftField(9)
    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    @JsonProperty
    @ThriftField(10)
    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    @JsonProperty
    @ThriftField(11)
    public int getBlockedDrivers()
    {
        return blockedDrivers;
    }

    @JsonProperty
    @ThriftField(12)
    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    @JsonProperty
    @ThriftField(13)
    public double getCumulativeUserMemory()
    {
        return cumulativeUserMemory;
    }

    @JsonProperty
    @ThriftField(14)
    public double getCumulativeTotalMemory()
    {
        return cumulativeTotalMemory;
    }

    @JsonProperty
    @ThriftField(15)
    public long getUserMemoryReservationInBytes()
    {
        return userMemoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(16)
    public long getRevocableMemoryReservationInBytes()
    {
        return revocableMemoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(17)
    public long getSystemMemoryReservationInBytes()
    {
        return systemMemoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(18)
    public long getPeakUserMemoryInBytes()
    {
        return peakUserMemoryInBytes;
    }

    @JsonProperty
    @ThriftField(19)
    public long getPeakTotalMemoryInBytes()
    {
        return peakTotalMemoryInBytes;
    }

    @JsonProperty
    @ThriftField(20)
    public long getPeakNodeTotalMemoryInBytes()
    {
        return peakNodeTotalMemoryInBytes;
    }

    @JsonProperty
    @ThriftField(21)
    public long getTotalScheduledTimeInNanos()
    {
        return totalScheduledTimeInNanos;
    }

    @JsonProperty
    @ThriftField(22)
    public long getTotalCpuTimeInNanos()
    {
        return totalCpuTimeInNanos;
    }

    @JsonProperty
    @ThriftField(23)
    public long getTotalBlockedTimeInNanos()
    {
        return totalBlockedTimeInNanos;
    }

    @JsonProperty
    @ThriftField(24)
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @JsonProperty
    @ThriftField(25)
    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    @JsonProperty
    @ThriftField(26)
    public long getTotalAllocationInBytes()
    {
        return totalAllocationInBytes;
    }

    @JsonProperty
    @ThriftField(27)
    public long getRawInputDataSizeInBytes()
    {
        return rawInputDataSizeInBytes;
    }

    @JsonProperty
    @ThriftField(28)
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    @ThriftField(29)
    public long getProcessedInputDataSizeInBytes()
    {
        return processedInputDataSizeInBytes;
    }

    @JsonProperty
    @ThriftField(30)
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    @JsonProperty
    @ThriftField(31)
    public long getOutputDataSizeInBytes()
    {
        return outputDataSizeInBytes;
    }

    @JsonProperty
    @ThriftField(32)
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    @ThriftField(33)
    public long getPhysicalWrittenDataSizeInBytes()
    {
        return physicalWrittenDataSizeInBytes;
    }

    @JsonProperty
    @ThriftField(34)
    public List<PipelineStats> getPipelines()
    {
        return pipelines;
    }

    @JsonProperty
    @ThriftField(35)
    public int getQueuedPartitionedDrivers()
    {
        return queuedPartitionedDrivers;
    }

    @JsonProperty
    @ThriftField(36)
    public long getQueuedPartitionedSplitsWeight()
    {
        return queuedPartitionedSplitsWeight;
    }

    @JsonProperty
    @ThriftField(37)
    public int getRunningPartitionedDrivers()
    {
        return runningPartitionedDrivers;
    }

    @JsonProperty
    @ThriftField(38)
    public long getRunningPartitionedSplitsWeight()
    {
        return runningPartitionedSplitsWeight;
    }

    @JsonProperty
    @ThriftField(39)
    public int getFullGcCount()
    {
        return fullGcCount;
    }

    @JsonProperty
    @ThriftField(40)
    public long getFullGcTimeInMillis()
    {
        return fullGcTimeInMillis;
    }

    @JsonProperty
    @ThriftField(41)
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    @JsonProperty
    @ThriftField(42)
    public int getTotalSplits()
    {
        return totalSplits;
    }

    @JsonProperty
    @ThriftField(43)
    public int getQueuedSplits()
    {
        return queuedSplits;
    }

    @JsonProperty
    @ThriftField(44)
    public int getRunningSplits()
    {
        return runningSplits;
    }

    @JsonProperty
    @ThriftField(45)
    public int getCompletedSplits()
    {
        return completedSplits;
    }

    @JsonProperty
    @ThriftField(46)
    public int getTotalNewDrivers()
    {
        return totalNewDrivers;
    }

    @JsonProperty
    @ThriftField(47)
    public int getQueuedNewDrivers()
    {
        return queuedNewDrivers;
    }

    @JsonProperty
    @ThriftField(48)
    public int getRunningNewDrivers()
    {
        return runningNewDrivers;
    }

    @JsonProperty
    @ThriftField(49)
    public int getCompletedNewDrivers()
    {
        return completedNewDrivers;
    }

    public TaskStats summarize()
    {
        return new TaskStats(
                createTimeInMillis,
                firstStartTimeInMillis,
                lastStartTimeInMillis,
                lastEndTimeInMillis,
                endTimeInMillis,
                elapsedTimeInNanos,
                queuedTimeInNanos,
                totalDrivers,
                queuedDrivers,
                queuedPartitionedDrivers,
                queuedPartitionedSplitsWeight,
                runningDrivers,
                runningPartitionedDrivers,
                runningPartitionedSplitsWeight,
                blockedDrivers,
                completedDrivers,
                totalNewDrivers,
                queuedNewDrivers,
                runningNewDrivers,
                completedNewDrivers,
                totalSplits,
                queuedSplits,
                runningSplits,
                completedSplits,
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
                createTimeInMillis,
                firstStartTimeInMillis,
                lastStartTimeInMillis,
                lastEndTimeInMillis,
                endTimeInMillis,
                elapsedTimeInNanos,
                queuedTimeInNanos,
                totalDrivers,
                queuedDrivers,
                queuedPartitionedDrivers,
                queuedPartitionedSplitsWeight,
                runningDrivers,
                runningPartitionedDrivers,
                runningPartitionedSplitsWeight,
                blockedDrivers,
                completedDrivers,
                totalNewDrivers,
                queuedNewDrivers,
                runningNewDrivers,
                completedNewDrivers,
                totalSplits,
                queuedSplits,
                runningSplits,
                completedSplits,
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
