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

import com.facebook.presto.operator.BlockedReason;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.HashSet;
import java.util.OptionalDouble;
import java.util.Set;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
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

            new DataSize(0, BYTE),
            0,

            0.0,
            0.0,
            new DataSize(0, BYTE),
            new DataSize(0, BYTE),

            new Duration(0, MILLISECONDS),
            new Duration(0, MILLISECONDS),

            false,
            ImmutableSet.of(),

            new DataSize(0, BYTE),

            OptionalDouble.empty());

    private final boolean isScheduled;
    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int completedDrivers;
    private final DataSize rawInputDataSize;
    private final long rawInputPositions;
    private final double cumulativeUserMemory;
    private final double cumulativeTotalMemory;
    private final DataSize userMemoryReservation;
    private final DataSize totalMemoryReservation;
    private final Duration totalCpuTime;
    private final Duration totalScheduledTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;
    private final DataSize totalAllocation;
    private final OptionalDouble progressPercentage;

    public BasicStageExecutionStats(
            boolean isScheduled,

            int totalDrivers,
            int queuedDrivers,
            int runningDrivers,
            int completedDrivers,

            DataSize rawInputDataSize,
            long rawInputPositions,

            double cumulativeUserMemory,
            double cumulativeTotalMemory,
            DataSize userMemoryReservation,
            DataSize totalMemoryReservation,

            Duration totalCpuTime,
            Duration totalScheduledTime,

            boolean fullyBlocked,
            Set<BlockedReason> blockedReasons,

            DataSize totalAllocation,

            OptionalDouble progressPercentage)
    {
        this.isScheduled = isScheduled;
        this.totalDrivers = totalDrivers;
        this.queuedDrivers = queuedDrivers;
        this.runningDrivers = runningDrivers;
        this.completedDrivers = completedDrivers;
        this.rawInputDataSize = requireNonNull(rawInputDataSize, "rawInputDataSize is null");
        this.rawInputPositions = rawInputPositions;
        this.cumulativeUserMemory = cumulativeUserMemory;
        this.cumulativeTotalMemory = cumulativeTotalMemory;
        this.userMemoryReservation = requireNonNull(userMemoryReservation, "userMemoryReservation is null");
        this.totalMemoryReservation = requireNonNull(totalMemoryReservation, "totalMemoryReservation is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));
        this.totalAllocation = requireNonNull(totalAllocation, "totalAllocation is null");
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

    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
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

    public DataSize getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    public DataSize getTotalMemoryReservation()
    {
        return totalMemoryReservation;
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

    public DataSize getTotalAllocation()
    {
        return totalAllocation;
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

            cumulativeUserMemory += stageStats.getCumulativeUserMemory();
            cumulativeTotalMemory += stageStats.getCumulativeTotalMemory();
            userMemoryReservation += stageStats.getUserMemoryReservation().toBytes();
            totalMemoryReservation += stageStats.getTotalMemoryReservation().toBytes();

            totalScheduledTimeMillis += stageStats.getTotalScheduledTime().roundTo(MILLISECONDS);
            totalCpuTime += stageStats.getTotalCpuTime().roundTo(MILLISECONDS);

            isScheduled &= stageStats.isScheduled();

            fullyBlocked &= stageStats.isFullyBlocked();
            blockedReasons.addAll(stageStats.getBlockedReasons());

            totalAllocation += stageStats.getTotalAllocation().toBytes();

            rawInputDataSize += stageStats.getRawInputDataSize().toBytes();
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

                succinctBytes(rawInputDataSize),
                rawInputPositions,

                cumulativeUserMemory,
                cumulativeTotalMemory,
                succinctBytes(userMemoryReservation),
                succinctBytes(totalMemoryReservation),

                succinctDuration(totalCpuTime, MILLISECONDS),
                succinctDuration(totalScheduledTimeMillis, MILLISECONDS),

                fullyBlocked,
                blockedReasons,

                succinctBytes(totalAllocation),

                progressPercentage);
    }
}
