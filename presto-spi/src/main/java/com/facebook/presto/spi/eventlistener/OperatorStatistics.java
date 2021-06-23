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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.Immutable;

import java.util.Optional;

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
    private final DataSize addInputAllocation;
    private final DataSize rawInputDataSize;
    private final long rawInputPositions;
    private final DataSize inputDataSize;
    private final long inputPositions;
    private final double sumSquaredInputPositions;

    private final long getOutputCalls;
    private final Duration getOutputWall;
    private final Duration getOutputCpu;
    private final DataSize getOutputAllocation;
    private final DataSize outputDataSize;
    private final long outputPositions;

    private final DataSize physicalWrittenDataSize;

    private final Duration blockedWall;

    private final long finishCalls;
    private final Duration finishWall;
    private final Duration finishCpu;
    private final DataSize finishAllocation;

    private final DataSize userMemoryReservation;
    private final DataSize revocableMemoryReservation;
    private final DataSize systemMemoryReservation;
    private final DataSize peakUserMemoryReservation;
    private final DataSize peakSystemMemoryReservation;
    private final DataSize peakTotalMemoryReservation;

    private final DataSize spilledDataSize;

    private final Optional<String> info;

    private final RuntimeStats runtimeStats;

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
            DataSize addInputAllocation,
            DataSize rawInputDataSize,
            long rawInputPositions,
            DataSize inputDataSize,
            long inputPositions,
            double sumSquaredInputPositions,

            long getOutputCalls,
            Duration getOutputWall,
            Duration getOutputCpu,
            DataSize getOutputAllocation,
            DataSize outputDataSize,
            long outputPositions,

            DataSize physicalWrittenDataSize,

            Duration blockedWall,

            long finishCalls,
            Duration finishWall,
            Duration finishCpu,
            DataSize finishAllocation,

            DataSize userMemoryReservation,
            DataSize revocableMemoryReservation,
            DataSize systemMemoryReservation,
            DataSize peakUserMemoryReservation,
            DataSize peakSystemMemoryReservation,
            DataSize peakTotalMemoryReservation,

            DataSize spilledDataSize,

            Optional<String> info,
            RuntimeStats runtimeStats)
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
        this.addInputAllocation = requireNonNull(addInputAllocation, "addInputAllocation is null");
        this.rawInputDataSize = requireNonNull(rawInputDataSize, "rawInputDataSize is null");
        this.rawInputPositions = requireNonNull(rawInputPositions, "rawInputPositions is null");
        this.inputDataSize = requireNonNull(inputDataSize, "inputDataSize is null");
        this.inputPositions = inputPositions;
        this.sumSquaredInputPositions = sumSquaredInputPositions;

        this.getOutputCalls = getOutputCalls;
        this.getOutputWall = requireNonNull(getOutputWall, "getOutputWall is null");
        this.getOutputCpu = requireNonNull(getOutputCpu, "getOutputCpu is null");
        this.getOutputAllocation = requireNonNull(getOutputAllocation, "getOutputAllocation is null");
        this.outputDataSize = requireNonNull(outputDataSize, "outputDataSize is null");
        this.outputPositions = outputPositions;

        this.physicalWrittenDataSize = requireNonNull(physicalWrittenDataSize, "writtenDataSize is null");

        this.blockedWall = requireNonNull(blockedWall, "blockedWall is null");

        this.finishCalls = finishCalls;
        this.finishWall = requireNonNull(finishWall, "finishWall is null");
        this.finishCpu = requireNonNull(finishCpu, "finishCpu is null");
        this.finishAllocation = requireNonNull(finishAllocation, "finishAllocation is null");

        this.userMemoryReservation = requireNonNull(userMemoryReservation, "userMemoryReservation is null");
        this.revocableMemoryReservation = requireNonNull(revocableMemoryReservation, "revocableMemoryReservation is null");
        this.systemMemoryReservation = requireNonNull(systemMemoryReservation, "systemMemoryReservation is null");

        this.peakUserMemoryReservation = requireNonNull(peakUserMemoryReservation, "peakUserMemoryReservation is null");
        this.peakSystemMemoryReservation = requireNonNull(peakSystemMemoryReservation, "peakSystemMemoryReservation is null");
        this.peakTotalMemoryReservation = requireNonNull(peakTotalMemoryReservation, "peakTotalMemoryReservation is null");

        this.spilledDataSize = requireNonNull(spilledDataSize, "spilledDataSize is null");

        this.info = requireNonNull(info, "info is null");
        this.runtimeStats = runtimeStats;
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

    public DataSize getAddInputAllocation()
    {
        return addInputAllocation;
    }

    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    public DataSize getInputDataSize()
    {
        return inputDataSize;
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

    public DataSize getGetOutputAllocation()
    {
        return getOutputAllocation;
    }

    public DataSize getOutputDataSize()
    {
        return outputDataSize;
    }

    public long getOutputPositions()
    {
        return outputPositions;
    }

    public DataSize getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
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

    public DataSize getFinishAllocation()
    {
        return finishAllocation;
    }

    public DataSize getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    public DataSize getRevocableMemoryReservation()
    {
        return revocableMemoryReservation;
    }

    public DataSize getSystemMemoryReservation()
    {
        return systemMemoryReservation;
    }

    public DataSize getPeakUserMemoryReservation()
    {
        return peakUserMemoryReservation;
    }

    public DataSize getPeakSystemMemoryReservation()
    {
        return peakSystemMemoryReservation;
    }

    public DataSize getPeakTotalMemoryReservation()
    {
        return peakTotalMemoryReservation;
    }

    public DataSize getSpilledDataSize()
    {
        return spilledDataSize;
    }

    public Optional<String> getInfo()
    {
        return info;
    }

    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }
}
