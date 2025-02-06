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
package com.facebook.presto.execution;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.operator.BlockedReason;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.OptionalDouble;
import java.util.Set;

import static com.facebook.airlift.units.Duration.succinctDuration;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BasicStageExecutionStats
{
    public static final BasicStageExecutionStats EMPTY_STAGE_STATS = new BasicStageExecutionStats(
            false,

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
            0,
            0,

            0L,
            0,

            0.0,
            0.0,
            0L,
            0L,

            new Duration(0, MILLISECONDS),
            new Duration(0, MILLISECONDS),

            false,
            ImmutableSet.of(),

            0L,

            OptionalDouble.empty());

    private final boolean isScheduled;
    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int completedDrivers;
    private final int totalNewDrivers;
    private final int queuedNewDrivers;
    private final int runningNewDrivers;
    private final int completedNewDrivers;
    private final int totalSplits;
    private final int queuedSplits;
    private final int runningSplits;
    private final int completedSplits;
    private final long rawInputDataSizeInBytes;
    private final long rawInputPositions;
    private final double cumulativeUserMemory;
    private final double cumulativeTotalMemory;
    private final long userMemoryReservationInBytes;
    private final long totalMemoryReservationInBytes;
    private final Duration totalCpuTime;
    private final Duration totalScheduledTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;
    private final long totalAllocationInBytes;
    private final OptionalDouble progressPercentage;

    public BasicStageExecutionStats(
            boolean isScheduled,

            int totalDrivers,
            int queuedDrivers,
            int runningDrivers,
            int completedDrivers,

            int totalNewDrivers,
            int queuedNewDrivers,
            int runningNewDrivers,
            int completedNewDrivers,

            int totalSplits,
            int queuedSplits,
            int runningSplits,
            int completedSplits,

            long rawInputDataSizeInBytes,
            long rawInputPositions,

            double cumulativeUserMemory,
            double cumulativeTotalMemory,
            long userMemoryReservationInBytes,
            long totalMemoryReservationInBytes,

            Duration totalCpuTime,
            Duration totalScheduledTime,

            boolean fullyBlocked,
            Set<BlockedReason> blockedReasons,

            long totalAllocationInBytes,

            OptionalDouble progressPercentage)
    {
        this.isScheduled = isScheduled;
        this.totalDrivers = totalDrivers;
        this.queuedDrivers = queuedDrivers;
        this.runningDrivers = runningDrivers;
        this.completedDrivers = completedDrivers;
        this.totalNewDrivers = totalNewDrivers;
        this.queuedNewDrivers = queuedNewDrivers;
        this.runningNewDrivers = runningNewDrivers;
        this.completedNewDrivers = completedNewDrivers;
        this.totalSplits = totalSplits;
        this.queuedSplits = queuedSplits;
        this.runningSplits = runningSplits;
        this.completedSplits = completedSplits;
        checkArgument(rawInputDataSizeInBytes >= 0, "rawInputDataSizeInBytes is negative");
        this.rawInputDataSizeInBytes = rawInputDataSizeInBytes;
        this.rawInputPositions = rawInputPositions;
        this.cumulativeUserMemory = cumulativeUserMemory;
        this.cumulativeTotalMemory = cumulativeTotalMemory;
        checkArgument(userMemoryReservationInBytes >= 0, "userMemoryReservationInBytes is negative");
        this.userMemoryReservationInBytes = userMemoryReservationInBytes;
        checkArgument(totalMemoryReservationInBytes >= 0, "totalMemoryReservationInBytes is negative");
        this.totalMemoryReservationInBytes = totalMemoryReservationInBytes;
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));
        checkArgument(totalAllocationInBytes >= 0, "totalAllocationInBytes is negative");
        this.totalAllocationInBytes = totalAllocationInBytes;
        this.progressPercentage = requireNonNull(progressPercentage, "progressPercentage is null");
    }

    public boolean isScheduled()
    {
        return isScheduled;
    }

    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    public int getTotalNewDrivers()
    {
        return totalNewDrivers;
    }

    public int getQueuedNewDrivers()
    {
        return queuedNewDrivers;
    }

    public int getRunningNewDrivers()
    {
        return runningNewDrivers;
    }

    public int getCompletedNewDrivers()
    {
        return completedNewDrivers;
    }

    public int getTotalSplits()
    {
        return totalSplits;
    }

    public int getQueuedSplits()
    {
        return queuedSplits;
    }

    public int getRunningSplits()
    {
        return runningSplits;
    }

    public int getCompletedSplits()
    {
        return completedSplits;
    }

    public long getRawInputDataSizeInBytes()
    {
        return rawInputDataSizeInBytes;
    }

    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    public double getCumulativeUserMemory()
    {
        return cumulativeUserMemory;
    }

    public double getCumulativeTotalMemory()
    {
        return cumulativeTotalMemory;
    }

    public long getUserMemoryReservationInBytes()
    {
        return userMemoryReservationInBytes;
    }

    public long getTotalMemoryReservationInBytes()
    {
        return totalMemoryReservationInBytes;
    }

    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    public Duration getTotalScheduledTime()
    {
        return totalScheduledTime;
    }

    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    public long getTotalAllocationInBytes()
    {
        return totalAllocationInBytes;
    }

    public OptionalDouble getProgressPercentage()
    {
        return progressPercentage;
    }

    public static BasicStageExecutionStats aggregateBasicStageStats(Iterable<BasicStageExecutionStats> stages)
    {
        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int completedDrivers = 0;

        int totalNewDrivers = 0;
        int queuedNewDrivers = 0;
        int runningNewDrivers = 0;
        int completedNewDrivers = 0;

        int totalSplits = 0;
        int queuedSplits = 0;
        int runningSplits = 0;
        int completedSplits = 0;

        double cumulativeUserMemory = 0;
        double cumulativeTotalMemory = 0;
        long userMemoryReservation = 0;
        long totalMemoryReservation = 0;

        long totalScheduledTimeMillis = 0;
        long totalCpuTime = 0;

        long rawInputDataSize = 0;
        long rawInputPositions = 0;

        boolean isScheduled = true;

        boolean fullyBlocked = true;
        Set<BlockedReason> blockedReasons = new HashSet<>();

        long totalAllocation = 0;

        for (BasicStageExecutionStats stageStats : stages) {
            totalDrivers += stageStats.getTotalDrivers();
            queuedDrivers += stageStats.getQueuedDrivers();
            runningDrivers += stageStats.getRunningDrivers();
            completedDrivers += stageStats.getCompletedDrivers();

            totalNewDrivers += stageStats.getTotalNewDrivers();
            queuedNewDrivers += stageStats.getQueuedNewDrivers();
            runningNewDrivers += stageStats.getRunningNewDrivers();
            completedNewDrivers += stageStats.getCompletedNewDrivers();

            totalSplits += stageStats.getTotalSplits();
            queuedSplits += stageStats.getQueuedSplits();
            runningSplits += stageStats.getRunningSplits();
            completedSplits += stageStats.getCompletedSplits();

            cumulativeUserMemory += stageStats.getCumulativeUserMemory();
            cumulativeTotalMemory += stageStats.getCumulativeTotalMemory();
            userMemoryReservation += stageStats.getUserMemoryReservationInBytes();
            totalMemoryReservation += stageStats.getTotalMemoryReservationInBytes();

            totalScheduledTimeMillis += stageStats.getTotalScheduledTime().roundTo(MILLISECONDS);
            totalCpuTime += stageStats.getTotalCpuTime().roundTo(MILLISECONDS);

            isScheduled &= stageStats.isScheduled();

            fullyBlocked &= stageStats.isFullyBlocked();
            blockedReasons.addAll(stageStats.getBlockedReasons());

            totalAllocation += stageStats.getTotalAllocationInBytes();

            rawInputDataSize += stageStats.getRawInputDataSizeInBytes();
            rawInputPositions += stageStats.getRawInputPositions();
        }

        OptionalDouble progressPercentage = OptionalDouble.empty();
        if (isScheduled && totalDrivers != 0) {
            progressPercentage = OptionalDouble.of(min(100, (completedDrivers * 100.0) / totalDrivers));
        }

        return new BasicStageExecutionStats(
                isScheduled,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                completedDrivers,

                totalNewDrivers,
                queuedNewDrivers,
                runningNewDrivers,
                completedNewDrivers,

                totalSplits,
                queuedSplits,
                runningSplits,
                completedSplits,

                rawInputDataSize,
                rawInputPositions,

                cumulativeUserMemory,
                cumulativeTotalMemory,
                userMemoryReservation,
                totalMemoryReservation,

                succinctDuration(totalCpuTime, MILLISECONDS),
                succinctDuration(totalScheduledTimeMillis, MILLISECONDS),

                fullyBlocked,
                blockedReasons,

                totalAllocation,

                progressPercentage);
    }
}
