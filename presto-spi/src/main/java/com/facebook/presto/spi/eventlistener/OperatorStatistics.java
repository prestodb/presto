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
package com.facebook.presto.spi.eventlistener;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.spi.plan.PlanNodeId;
import io.airlift.units.Duration;

import javax.annotation.concurrent.Immutable;

import java.util.Optional;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class OperatorStatistics
{
    private final int stageId;
    private final int stageExecutionId;
    private final int pipelineId;
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final String operatorType;

    private final long totalDrivers;

    private final long addInputCalls;
    private final Duration addInputWall;
    private final Duration addInputCpu;
    private final long addInputAllocationInBytes;
    private final long rawInputDataSizeInBytes;
    private final long rawInputPositions;
    private final long inputDataSizeInBytes;
    private final long inputPositions;
    private final double sumSquaredInputPositions;

    private final long getOutputCalls;
    private final Duration getOutputWall;
    private final Duration getOutputCpu;
    private final long getOutputAllocationInBytes;
    private final long outputDataSizeInBytes;
    private final long outputPositions;

    private final long physicalWrittenDataSizeInBytes;

    private final Duration blockedWall;

    private final long finishCalls;
    private final Duration finishWall;
    private final Duration finishCpu;
    private final long finishAllocationInBytes;

    private final long userMemoryReservationInBytes;
    private final long revocableMemoryReservationInBytes;
    private final long systemMemoryReservationInBytes;
    private final long peakUserMemoryReservationInBytes;
    private final long peakSystemMemoryReservationInBytes;
    private final long peakTotalMemoryReservationInBytes;

    private final long spilledDataSizeInBytes;

    private final Optional<String> info;

    private final RuntimeStats runtimeStats;

    private final double estimateOutputDataSize;
    private final double estimateOutputPositions;

    public OperatorStatistics(
            int stageId,
            int stageExecutionId,
            int pipelineId,
            int operatorId,
            PlanNodeId planNodeId,
            String operatorType,

            long totalDrivers,

            long addInputCalls,
            Duration addInputWall,
            Duration addInputCpu,
            long addInputAllocation,
            long rawInputDataSize,
            long rawInputPositions,
            long inputDataSize,
            long inputPositions,
            double sumSquaredInputPositions,

            long getOutputCalls,
            Duration getOutputWall,
            Duration getOutputCpu,
            long getOutputAllocation,
            long outputDataSize,
            long outputPositions,

            long physicalWrittenDataSize,

            Duration blockedWall,

            long finishCalls,
            Duration finishWall,
            Duration finishCpu,
            long finishAllocation,

            long userMemoryReservation,
            long revocableMemoryReservation,
            long systemMemoryReservation,
            long peakUserMemoryReservation,
            long peakSystemMemoryReservation,
            long peakTotalMemoryReservation,

            long spilledDataSize,

            Optional<String> info,
            RuntimeStats runtimeStats,
            double estimateOutputDataSize,
            double estimateOutputPositions)
    {
        this.stageId = stageId;
        this.stageExecutionId = stageExecutionId;
        this.pipelineId = pipelineId;

        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.operatorType = requireNonNull(operatorType, "operatorType is null");

        this.totalDrivers = totalDrivers;

        this.addInputCalls = addInputCalls;
        this.addInputWall = requireNonNull(addInputWall, "addInputWall is null");
        this.addInputCpu = requireNonNull(addInputCpu, "addInputCpu is null");
        checkArgument(addInputAllocation >= 0, "addInputAllocation is negative");
        this.addInputAllocationInBytes = addInputAllocation;
        checkArgument(rawInputDataSize >= 0, "rawInputDataSize is negative");
        this.rawInputDataSizeInBytes = rawInputDataSize;
        this.rawInputPositions = rawInputPositions;
        checkArgument(inputDataSize >= 0, "inputDataSize is negative");
        this.inputDataSizeInBytes = inputDataSize;
        this.inputPositions = inputPositions;
        this.sumSquaredInputPositions = sumSquaredInputPositions;

        this.getOutputCalls = getOutputCalls;
        this.getOutputWall = requireNonNull(getOutputWall, "getOutputWall is null");
        this.getOutputCpu = requireNonNull(getOutputCpu, "getOutputCpu is null");
        checkArgument(getOutputAllocation >= 0, "getOutputAllocation is negative");
        this.getOutputAllocationInBytes = getOutputAllocation;
        checkArgument(outputDataSize >= 0, "outputDataSize is negative");
        this.outputDataSizeInBytes = outputDataSize;
        this.outputPositions = outputPositions;

        checkArgument(physicalWrittenDataSize >= 0, "writtenDataSize is negative");
        this.physicalWrittenDataSizeInBytes = physicalWrittenDataSize;
        this.blockedWall = requireNonNull(blockedWall, "blockedWall is null");

        this.finishCalls = finishCalls;
        this.finishWall = requireNonNull(finishWall, "finishWall is null");
        this.finishCpu = requireNonNull(finishCpu, "finishCpu is null");
        checkArgument(finishAllocation >= 0, "finishAllocation is negative");
        this.finishAllocationInBytes = finishAllocation;
        checkArgument(userMemoryReservation >= 0, "userMemoryReservation is negative");
        this.userMemoryReservationInBytes = userMemoryReservation;
        checkArgument(revocableMemoryReservation >= 0, "revocableMemoryReservation is negative");
        this.revocableMemoryReservationInBytes = revocableMemoryReservation;
        checkArgument(systemMemoryReservation >= 0, "systemMemoryReservation is negative");
        this.systemMemoryReservationInBytes = systemMemoryReservation;
        checkArgument(peakUserMemoryReservation >= 0, "peakUserMemoryReservation is negative");
        this.peakUserMemoryReservationInBytes = peakUserMemoryReservation;
        checkArgument(peakSystemMemoryReservation >= 0, "peakSystemMemoryReservation is negative");
        this.peakSystemMemoryReservationInBytes = peakSystemMemoryReservation;
        checkArgument(peakTotalMemoryReservation >= 0, "peakTotalMemoryReservation is negative");
        this.peakTotalMemoryReservationInBytes = peakTotalMemoryReservation;
        checkArgument(spilledDataSize >= 0, "spilledDataSize is negative");
        this.spilledDataSizeInBytes = spilledDataSize;
        this.info = requireNonNull(info, "info is null");
        this.runtimeStats = runtimeStats;

        this.estimateOutputDataSize = estimateOutputDataSize;
        this.estimateOutputPositions = estimateOutputPositions;
    }

    public int getStageId()
    {
        return stageId;
    }

    public int getStageExecutionId()
    {
        return stageExecutionId;
    }

    public int getPipelineId()
    {
        return pipelineId;
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    public String getOperatorType()
    {
        return operatorType;
    }

    public long getTotalDrivers()
    {
        return totalDrivers;
    }

    public long getAddInputCalls()
    {
        return addInputCalls;
    }

    public Duration getAddInputWall()
    {
        return addInputWall;
    }

    public Duration getAddInputCpu()
    {
        return addInputCpu;
    }

    public long getAddInputAllocationInBytes()
    {
        return addInputAllocationInBytes;
    }

    public long getRawInputDataSizeInBytes()
    {
        return rawInputDataSizeInBytes;
    }

    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    public long getInputDataSizeInBytes()
    {
        return inputDataSizeInBytes;
    }

    public long getInputPositions()
    {
        return inputPositions;
    }

    public double getSumSquaredInputPositions()
    {
        return sumSquaredInputPositions;
    }

    public long getGetOutputCalls()
    {
        return getOutputCalls;
    }

    public Duration getGetOutputWall()
    {
        return getOutputWall;
    }

    public Duration getGetOutputCpu()
    {
        return getOutputCpu;
    }

    public long getGetOutputAllocationInBytes()
    {
        return getOutputAllocationInBytes;
    }

    public long getOutputDataSizeInBytes()
    {
        return outputDataSizeInBytes;
    }

    public long getOutputPositions()
    {
        return outputPositions;
    }

    public long getPhysicalWrittenDataSizeInBytes()
    {
        return physicalWrittenDataSizeInBytes;
    }

    public Duration getBlockedWall()
    {
        return blockedWall;
    }

    public long getFinishCalls()
    {
        return finishCalls;
    }

    public Duration getFinishWall()
    {
        return finishWall;
    }

    public Duration getFinishCpu()
    {
        return finishCpu;
    }

    public long getFinishAllocationInBytes()
    {
        return finishAllocationInBytes;
    }

    public long getUserMemoryReservationInBytes()
    {
        return userMemoryReservationInBytes;
    }

    public long getRevocableMemoryReservationInBytes()
    {
        return revocableMemoryReservationInBytes;
    }

    public long getSystemMemoryReservationInBytes()
    {
        return systemMemoryReservationInBytes;
    }

    public long getPeakUserMemoryReservationInBytes()
    {
        return peakUserMemoryReservationInBytes;
    }

    public long getPeakSystemMemoryReservationInBytes()
    {
        return peakSystemMemoryReservationInBytes;
    }

    public long getPeakTotalMemoryReservationInBytes()
    {
        return peakTotalMemoryReservationInBytes;
    }

    public long getSpilledDataSizeInBytes()
    {
        return spilledDataSizeInBytes;
    }

    public Optional<String> getInfo()
    {
        return info;
    }

    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    public double getEstimateOutputDataSize()
    {
        return estimateOutputDataSize;
    }

    public double getEstimateOutputPositions()
    {
        return estimateOutputPositions;
    }
}
