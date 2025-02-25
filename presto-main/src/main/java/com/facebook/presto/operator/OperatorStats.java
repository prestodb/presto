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
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Immutable
@ThriftStruct
public class OperatorStats
{
    private final int stageId;
    private final int stageExecutionId;
    private final int pipelineId;
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final String operatorType;

    private final long totalDrivers;

    private final long isBlockedCalls;
    private final Duration isBlockedWall;
    private final Duration isBlockedCpu;
    private final long isBlockedAllocation;

    private final long addInputCalls;
    private final Duration addInputWall;
    private final Duration addInputCpu;
    private final long addInputAllocation;
    private final long rawInputDataSize;
    private final long rawInputPositions;
    private final long inputDataSize;
    private final long inputPositions;
    private final double sumSquaredInputPositions;

    private final long getOutputCalls;
    private final Duration getOutputWall;
    private final Duration getOutputCpu;
    private final long getOutputAllocation;
    private final long outputDataSize;
    private final long outputPositions;

    private final long physicalWrittenDataSize;

    private final Duration additionalCpu;
    private final Duration blockedWall;

    private final long finishCalls;
    private final Duration finishWall;
    private final Duration finishCpu;
    private final long finishAllocation;

    private final long userMemoryReservation;
    private final long revocableMemoryReservation;
    private final long systemMemoryReservation;
    private final long peakUserMemoryReservation;
    private final long peakSystemMemoryReservation;
    private final long peakTotalMemoryReservation;

    private final long spilledDataSize;

    private final Optional<BlockedReason> blockedReason;

    @Nullable
    private final OperatorInfo info;
    @Nullable
    private final OperatorInfoUnion infoUnion;

    private final RuntimeStats runtimeStats;

    private final DynamicFilterStats dynamicFilterStats;

    private final long nullJoinBuildKeyCount;
    private final long joinBuildKeyCount;
    private final long nullJoinProbeKeyCount;
    private final long joinProbeKeyCount;

    @JsonCreator
    public OperatorStats(
            @JsonProperty("stageId") int stageId,
            @JsonProperty("stageExecutionId") int stageExecutionId,
            @JsonProperty("pipelineId") int pipelineId,
            @JsonProperty("operatorId") int operatorId,
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("operatorType") String operatorType,

            @JsonProperty("totalDrivers") long totalDrivers,

            @JsonProperty("isBlockedCalls") long isBlockedCalls,
            @JsonProperty("isBlockedWall") Duration isBlockedWall,
            @JsonProperty("isBlockedCpu") Duration isBlockedCpu,
            @JsonProperty("isBlockedAllocation") long isBlockedAllocation,

            @JsonProperty("addInputCalls") long addInputCalls,
            @JsonProperty("addInputWall") Duration addInputWall,
            @JsonProperty("addInputCpu") Duration addInputCpu,
            @JsonProperty("addInputAllocation") long addInputAllocation,
            @JsonProperty("rawInputDataSize") long rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,
            @JsonProperty("inputDataSize") long inputDataSize,
            @JsonProperty("inputPositions") long inputPositions,
            @JsonProperty("sumSquaredInputPositions") double sumSquaredInputPositions,

            @JsonProperty("getOutputCalls") long getOutputCalls,
            @JsonProperty("getOutputWall") Duration getOutputWall,
            @JsonProperty("getOutputCpu") Duration getOutputCpu,
            @JsonProperty("getOutputAllocation") long getOutputAllocation,
            @JsonProperty("outputDataSize") long outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("physicalWrittenDataSize") long physicalWrittenDataSize,

            @JsonProperty("additionalCpu") Duration additionalCpu,
            @JsonProperty("blockedWall") Duration blockedWall,

            @JsonProperty("finishCalls") long finishCalls,
            @JsonProperty("finishWall") Duration finishWall,
            @JsonProperty("finishCpu") Duration finishCpu,
            @JsonProperty("finishAllocation") long finishAllocation,

            @JsonProperty("userMemoryReservation") long userMemoryReservation,
            @JsonProperty("revocableMemoryReservation") long revocableMemoryReservation,
            @JsonProperty("systemMemoryReservation") long systemMemoryReservation,
            @JsonProperty("peakUserMemoryReservation") long peakUserMemoryReservation,
            @JsonProperty("peakSystemMemoryReservation") long peakSystemMemoryReservation,
            @JsonProperty("peakTotalMemoryReservation") long peakTotalMemoryReservation,

            @JsonProperty("spilledDataSize") long spilledDataSize,

            @JsonProperty("blockedReason") Optional<BlockedReason> blockedReason,

            @Nullable
            @JsonProperty("info") OperatorInfo info,
            @JsonProperty("runtimeStats") RuntimeStats runtimeStats,
            @JsonProperty("dynamicFilterStats") DynamicFilterStats dynamicFilterStats,
            @JsonProperty("nullJoinBuildKeyCount") long nullJoinBuildKeyCount,
            @JsonProperty("joinBuildKeyCount") long joinBuildKeyCount,
            @JsonProperty("nullJoinProbeKeyCount") long nullJoinProbeKeyCount,
            @JsonProperty("joinProbeKeyCount") long joinProbeKeyCount)
    {
        this.stageId = stageId;
        this.stageExecutionId = stageExecutionId;
        this.pipelineId = pipelineId;

        checkArgument(operatorId >= 0, "operatorId is negative");
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.operatorType = requireNonNull(operatorType, "operatorType is null");

        this.totalDrivers = totalDrivers;

        this.isBlockedCalls = isBlockedCalls;
        this.isBlockedWall = requireNonNull(isBlockedWall, "isBlockedWall is null");
        this.isBlockedCpu = requireNonNull(isBlockedCpu, "isBlockedCpu is null");
        checkArgument(isBlockedAllocation >= 0, "isBlockedAllocation is negative");
        this.isBlockedAllocation = isBlockedAllocation;

        this.addInputCalls = addInputCalls;
        this.addInputWall = requireNonNull(addInputWall, "addInputWall is null");
        this.addInputCpu = requireNonNull(addInputCpu, "addInputCpu is null");
        checkArgument(addInputAllocation >= 0, "addInputAllocation is negative");
        this.addInputAllocation = addInputAllocation;
        checkArgument(rawInputDataSize >= 0, "rawInputDataSize is negative");
        this.rawInputDataSize = rawInputDataSize;
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;
        checkArgument(inputDataSize >= 0, "inputDataSize is negative");
        this.inputDataSize = inputDataSize;
        checkArgument(inputPositions >= 0, "inputPositions is negative");
        this.inputPositions = inputPositions;
        this.sumSquaredInputPositions = sumSquaredInputPositions;

        this.getOutputCalls = getOutputCalls;
        this.getOutputWall = requireNonNull(getOutputWall, "getOutputWall is null");
        this.getOutputCpu = requireNonNull(getOutputCpu, "getOutputCpu is null");
        checkArgument(getOutputAllocation >= 0, "getOutputAllocation is negative");
        this.getOutputAllocation = getOutputAllocation;
        checkArgument(outputDataSize >= 0, "outputDataSize is negative");
        this.outputDataSize = outputDataSize;
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        checkArgument(physicalWrittenDataSize >= 0, "writtenDataSize is negative");
        this.physicalWrittenDataSize = physicalWrittenDataSize;
        this.additionalCpu = requireNonNull(additionalCpu, "additionalCpu is negative");
        this.blockedWall = requireNonNull(blockedWall, "blockedWall is null");

        this.finishCalls = finishCalls;
        this.finishWall = requireNonNull(finishWall, "finishWall is null");
        this.finishCpu = requireNonNull(finishCpu, "finishCpu is null");
        checkArgument(finishAllocation >= 0, "finishAllocation is negative");
        this.finishAllocation = finishAllocation;
        checkArgument(userMemoryReservation >= 0, "userMemoryReservation is negative");
        this.userMemoryReservation = userMemoryReservation;
        checkArgument(revocableMemoryReservation >= 0, "revocableMemoryReservation is negative");
        this.revocableMemoryReservation = revocableMemoryReservation;
        checkArgument(systemMemoryReservation >= 0, "systemMemoryReservation is negative");
        this.systemMemoryReservation = systemMemoryReservation;
        checkArgument(peakUserMemoryReservation >= 0, "peakUserMemoryReservation is negative");
        this.peakUserMemoryReservation = peakUserMemoryReservation;
        checkArgument(peakSystemMemoryReservation >= 0, "peakSystemMemoryReservation is negative");
        this.peakSystemMemoryReservation = peakSystemMemoryReservation;
        checkArgument(peakTotalMemoryReservation >= 0, "peakTotalMemoryReservation is negative");
        this.peakTotalMemoryReservation = peakTotalMemoryReservation;
        checkArgument(spilledDataSize >= 0, "spilledDataSize is negative");
        this.spilledDataSize = spilledDataSize;
        this.runtimeStats = runtimeStats;

        this.dynamicFilterStats = dynamicFilterStats;

        this.blockedReason = blockedReason;

        this.info = info;
        this.infoUnion = null;
        this.nullJoinBuildKeyCount = nullJoinBuildKeyCount;
        this.joinBuildKeyCount = joinBuildKeyCount;
        this.nullJoinProbeKeyCount = nullJoinProbeKeyCount;
        this.joinProbeKeyCount = joinProbeKeyCount;
    }

    @ThriftConstructor
    public OperatorStats(
            int stageId,
            int stageExecutionId,
            int pipelineId,
            int operatorId,
            PlanNodeId planNodeId,
            String operatorType,

            long totalDrivers,

            long isBlockedCalls,
            Duration isBlockedWall,
            Duration isBlockedCpu,
            long isBlockedAllocation,

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

            Duration additionalCpu,
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

            Optional<BlockedReason> blockedReason,

            RuntimeStats runtimeStats,
            DynamicFilterStats dynamicFilterStats,
            @Nullable
            OperatorInfoUnion infoUnion,
            long nullJoinBuildKeyCount,
            long joinBuildKeyCount,
            long nullJoinProbeKeyCount,
            long joinProbeKeyCount)
    {
        this.stageId = stageId;
        this.stageExecutionId = stageExecutionId;
        this.pipelineId = pipelineId;

        checkArgument(operatorId >= 0, "operatorId is negative");
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.operatorType = requireNonNull(operatorType, "operatorType is null");

        this.totalDrivers = totalDrivers;

        this.isBlockedCalls = isBlockedCalls;
        this.isBlockedWall = requireNonNull(isBlockedWall, "isBlockedWall is null");
        this.isBlockedCpu = requireNonNull(isBlockedCpu, "isBlockedCpu is null");
        checkArgument(isBlockedAllocation >= 0, "isBlockedAllocation is negative");
        this.isBlockedAllocation = isBlockedAllocation;

        this.addInputCalls = addInputCalls;
        this.addInputWall = requireNonNull(addInputWall, "addInputWall is null");
        this.addInputCpu = requireNonNull(addInputCpu, "addInputCpu is null");
        checkArgument(addInputAllocation >= 0, "addInputAllocation is negative");
        this.addInputAllocation = addInputAllocation;
        checkArgument(rawInputDataSize >= 0, "rawInputDataSize is negative");
        this.rawInputDataSize = rawInputDataSize;
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;

        checkArgument(inputDataSize >= 0, "inputDataSize is negative");
        this.inputDataSize = inputDataSize;
        checkArgument(inputPositions >= 0, "inputPositions is negative");
        this.inputPositions = inputPositions;
        this.sumSquaredInputPositions = sumSquaredInputPositions;

        this.getOutputCalls = getOutputCalls;
        this.getOutputWall = requireNonNull(getOutputWall, "getOutputWall is null");
        this.getOutputCpu = requireNonNull(getOutputCpu, "getOutputCpu is null");
        checkArgument(getOutputAllocation >= 0, "getOutputAllocation is negative");
        this.getOutputAllocation = getOutputAllocation;
        checkArgument(outputDataSize >= 0, "outputDataSize is negative");
        this.outputDataSize = outputDataSize;
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        checkArgument(physicalWrittenDataSize >= 0, "writtenDataSize is negative");
        this.physicalWrittenDataSize = physicalWrittenDataSize;
        this.additionalCpu = requireNonNull(additionalCpu, "additionalCpu is null");
        this.blockedWall = requireNonNull(blockedWall, "blockedWall is null");

        this.finishCalls = finishCalls;
        this.finishWall = requireNonNull(finishWall, "finishWall is null");
        this.finishCpu = requireNonNull(finishCpu, "finishCpu is null");
        checkArgument(finishAllocation >= 0, "finishAllocation is negative");
        this.finishAllocation = finishAllocation;
        checkArgument(userMemoryReservation >= 0, "userMemoryReservation is negative");
        this.userMemoryReservation = userMemoryReservation;
        checkArgument(revocableMemoryReservation >= 0, "revocableMemoryReservation is negative");
        this.revocableMemoryReservation = revocableMemoryReservation;
        checkArgument(systemMemoryReservation >= 0, "systemMemoryReservation is negative");
        this.systemMemoryReservation = systemMemoryReservation;

        checkArgument(peakUserMemoryReservation >= 0, "peakUserMemoryReservation is negative");
        this.peakUserMemoryReservation = peakUserMemoryReservation;
        checkArgument(peakSystemMemoryReservation >= 0, "peakSystemMemoryReservation is negative");
        this.peakSystemMemoryReservation = peakSystemMemoryReservation;
        checkArgument(peakTotalMemoryReservation >= 0, "peakTotalMemoryReservation is negative");
        this.peakTotalMemoryReservation = peakTotalMemoryReservation;

        checkArgument(spilledDataSize >= 0, "spilledDataSize is negative");
        this.spilledDataSize = spilledDataSize;

        this.runtimeStats = runtimeStats;

        this.dynamicFilterStats = dynamicFilterStats;

        this.blockedReason = blockedReason;

        this.infoUnion = infoUnion;
        this.info = null;
        this.nullJoinBuildKeyCount = nullJoinBuildKeyCount;
        this.joinBuildKeyCount = joinBuildKeyCount;
        this.nullJoinProbeKeyCount = nullJoinProbeKeyCount;
        this.joinProbeKeyCount = joinProbeKeyCount;
    }

    @JsonProperty
    @ThriftField(1)
    public int getStageId()
    {
        return stageId;
    }

    @JsonProperty
    @ThriftField(2)
    public int getStageExecutionId()
    {
        return stageExecutionId;
    }

    @JsonProperty
    @ThriftField(3)
    public int getPipelineId()
    {
        return pipelineId;
    }

    @JsonProperty
    @ThriftField(4)
    public int getOperatorId()
    {
        return operatorId;
    }

    @JsonProperty
    @ThriftField(5)
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    @ThriftField(6)
    public String getOperatorType()
    {
        return operatorType;
    }

    @JsonProperty
    @ThriftField(7)
    public long getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    @ThriftField(8)
    public long getAddInputCalls()
    {
        return addInputCalls;
    }

    @JsonProperty
    @ThriftField(9)
    public Duration getAddInputWall()
    {
        return addInputWall;
    }

    @JsonProperty
    @ThriftField(10)
    public Duration getAddInputCpu()
    {
        return addInputCpu;
    }

    @JsonProperty
    @ThriftField(11)
    public long getAddInputAllocation()
    {
        return addInputAllocation;
    }

    @JsonProperty
    @ThriftField(12)
    public long getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    @JsonProperty
    @ThriftField(13)
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    @ThriftField(14)
    public long getInputDataSize()
    {
        return inputDataSize;
    }

    @JsonProperty
    @ThriftField(15)
    public long getInputPositions()
    {
        return inputPositions;
    }

    @JsonProperty
    @ThriftField(16)
    public double getSumSquaredInputPositions()
    {
        return sumSquaredInputPositions;
    }

    @JsonProperty
    @ThriftField(17)
    public long getGetOutputCalls()
    {
        return getOutputCalls;
    }

    @JsonProperty
    @ThriftField(18)
    public Duration getGetOutputWall()
    {
        return getOutputWall;
    }

    @JsonProperty
    @ThriftField(19)
    public Duration getGetOutputCpu()
    {
        return getOutputCpu;
    }

    @JsonProperty
    @ThriftField(20)
    public long getGetOutputAllocation()
    {
        return getOutputAllocation;
    }

    @JsonProperty
    @ThriftField(21)
    public long getOutputDataSize()
    {
        return outputDataSize;
    }

    @JsonProperty
    @ThriftField(22)
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    @ThriftField(23)
    public long getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
    }

    @JsonProperty
    @ThriftField(24)
    public Duration getAdditionalCpu()
    {
        return additionalCpu;
    }

    @JsonProperty
    @ThriftField(25)
    public Duration getBlockedWall()
    {
        return blockedWall;
    }

    @JsonProperty
    @ThriftField(26)
    public long getFinishCalls()
    {
        return finishCalls;
    }

    @JsonProperty
    @ThriftField(27)
    public Duration getFinishWall()
    {
        return finishWall;
    }

    @JsonProperty
    @ThriftField(28)
    public Duration getFinishCpu()
    {
        return finishCpu;
    }

    @JsonProperty
    @ThriftField(29)
    public long getFinishAllocation()
    {
        return finishAllocation;
    }

    @JsonProperty
    @ThriftField(30)
    public long getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    @JsonProperty
    @ThriftField(31)
    public long getRevocableMemoryReservation()
    {
        return revocableMemoryReservation;
    }

    @JsonProperty
    @ThriftField(32)
    public long getSystemMemoryReservation()
    {
        return systemMemoryReservation;
    }

    @JsonProperty
    @ThriftField(33)
    public long getPeakUserMemoryReservation()
    {
        return peakUserMemoryReservation;
    }

    @JsonProperty
    @ThriftField(34)
    public long getPeakSystemMemoryReservation()
    {
        return peakSystemMemoryReservation;
    }

    @JsonProperty
    @ThriftField(35)
    public long getPeakTotalMemoryReservation()
    {
        return peakTotalMemoryReservation;
    }

    @JsonProperty
    @ThriftField(36)
    public long getSpilledDataSize()
    {
        return spilledDataSize;
    }

    @Nullable
    @JsonProperty
    @ThriftField(37)
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    @JsonProperty
    @ThriftField(38)
    public Optional<BlockedReason> getBlockedReason()
    {
        return blockedReason;
    }

    @Nullable
    @JsonProperty
    public OperatorInfo getInfo()
    {
        return info;
    }

    @Nullable
    @ThriftField(39)
    public OperatorInfoUnion getInfoUnion()
    {
        return infoUnion;
    }

    @JsonProperty
    @ThriftField(40)
    public long getNullJoinBuildKeyCount()
    {
        return nullJoinBuildKeyCount;
    }

    @JsonProperty
    @ThriftField(41)
    public long getJoinBuildKeyCount()
    {
        return joinBuildKeyCount;
    }

    @JsonProperty
    @ThriftField(42)
    public long getNullJoinProbeKeyCount()
    {
        return nullJoinProbeKeyCount;
    }

    @JsonProperty
    @ThriftField(43)
    public long getJoinProbeKeyCount()
    {
        return joinProbeKeyCount;
    }

    @Nullable
    @JsonProperty
    @ThriftField(44)
    public DynamicFilterStats getDynamicFilterStats()
    {
        return dynamicFilterStats;
    }

    @JsonProperty
    @ThriftField(45)
    public long getIsBlockedCalls()
    {
        return isBlockedCalls;
    }

    @JsonProperty
    @ThriftField(46)
    public Duration getIsBlockedWall()
    {
        return isBlockedWall;
    }

    @JsonProperty
    @ThriftField(47)
    public Duration getIsBlockedCpu()
    {
        return isBlockedCpu;
    }

    @JsonProperty
    @ThriftField(48)
    public long getIsBlockedAllocation()
    {
        return isBlockedAllocation;
    }

    public static Optional<OperatorStats> merge(List<OperatorStats> operators)
    {
        if (operators.isEmpty()) {
            return Optional.empty();
        }

        OperatorStats first = operators.stream().findFirst().get();
        int stageId = first.getStageId();
        int operatorId = first.getOperatorId();
        int stageExecutionId = first.getStageExecutionId();
        int pipelineId = first.getPipelineId();
        PlanNodeId planNodeId = first.getPlanNodeId();
        String operatorType = first.getOperatorType();

        long totalDrivers = 0;

        long isBlockedCalls = 0;
        long isBlockedWall = 0;
        long isBlockedCpu = 0;
        long isBlockedAllocation = 0;

        long addInputCalls = 0;
        long addInputWall = 0;
        long addInputCpu = 0;
        long addInputAllocation = 0;
        long rawInputDataSize = 0;
        long rawInputPositions = 0;
        long inputDataSize = 0;
        long inputPositions = 0;

        double sumSquaredInputPositions = 0.0;

        long getOutputCalls = 0;
        long getOutputWall = 0;
        long getOutputCpu = 0;
        long getOutputAllocation = 0;
        long outputDataSize = 0;
        long outputPositions = 0;

        long physicalWrittenDataSize = 0;

        long finishCalls = 0;
        long finishWall = 0;
        long finishCpu = 0;
        long finishAllocation = 0;

        long additionalCpu = 0;
        long blockedWall = 0;

        long memoryReservation = 0;
        long revocableMemoryReservation = 0;
        long systemMemoryReservation = 0;

        long peakUserMemory = 0;
        long peakSystemMemory = 0;
        long peakTotalMemory = 0;

        long spilledDataSize = 0;

        long nullJoinBuildKeyCount = 0;
        long joinBuildKeyCount = 0;
        long nullJoinProbeKeyCount = 0;
        long joinProbeKeyCount = 0;

        RuntimeStats runtimeStats = new RuntimeStats();
        DynamicFilterStats dynamicFilterStats = new DynamicFilterStats(new HashSet<>());

        Optional<BlockedReason> blockedReason = Optional.empty();

        Mergeable<OperatorInfo> base = null;

        for (OperatorStats operator : operators) {
            checkArgument(operator.getOperatorId() == operatorId, "Expected operatorId to be %s but was %s", operatorId, operator.getOperatorId());

            totalDrivers += operator.totalDrivers;

            isBlockedCalls += operator.getGetOutputCalls();
            isBlockedWall += operator.getGetOutputWall().roundTo(NANOSECONDS);
            isBlockedCpu += operator.getGetOutputCpu().roundTo(NANOSECONDS);
            isBlockedAllocation += operator.getIsBlockedAllocation();

            addInputCalls += operator.getAddInputCalls();
            addInputWall += operator.getAddInputWall().roundTo(NANOSECONDS);
            addInputCpu += operator.getAddInputCpu().roundTo(NANOSECONDS);
            addInputAllocation += operator.getAddInputAllocation();
            rawInputDataSize += operator.getRawInputDataSize();
            rawInputPositions += operator.getRawInputPositions();
            inputDataSize += operator.getInputDataSize();
            inputPositions += operator.getInputPositions();
            sumSquaredInputPositions += operator.getSumSquaredInputPositions();

            getOutputCalls += operator.getGetOutputCalls();
            getOutputWall += operator.getGetOutputWall().roundTo(NANOSECONDS);
            getOutputCpu += operator.getGetOutputCpu().roundTo(NANOSECONDS);
            getOutputAllocation += operator.getGetOutputAllocation();
            outputDataSize += operator.getOutputDataSize();
            outputPositions += operator.getOutputPositions();

            physicalWrittenDataSize += operator.getPhysicalWrittenDataSize();

            finishCalls += operator.getFinishCalls();
            finishWall += operator.getFinishWall().roundTo(NANOSECONDS);
            finishCpu += operator.getFinishCpu().roundTo(NANOSECONDS);
            finishAllocation += operator.getFinishAllocation();

            additionalCpu += operator.getAdditionalCpu().roundTo(NANOSECONDS);
            blockedWall += operator.getBlockedWall().roundTo(NANOSECONDS);

            memoryReservation += operator.getUserMemoryReservation();
            revocableMemoryReservation += operator.getRevocableMemoryReservation();
            systemMemoryReservation += operator.getSystemMemoryReservation();

            peakUserMemory = max(peakUserMemory, operator.getPeakUserMemoryReservation());
            peakSystemMemory = max(peakSystemMemory, operator.getPeakSystemMemoryReservation());
            peakTotalMemory = max(peakTotalMemory, operator.getPeakTotalMemoryReservation());

            spilledDataSize += operator.getSpilledDataSize();

            if (operator.getBlockedReason().isPresent()) {
                blockedReason = operator.getBlockedReason();
            }

            OperatorInfo info = operator.getInfo();
            if (base == null) {
                base = getMergeableInfoOrNull(info);
            }
            else if (info != null && base.getClass() == info.getClass()) {
                base = mergeInfo(base, info);
            }

            runtimeStats.mergeWith(operator.getRuntimeStats());
            dynamicFilterStats.mergeWith(operator.getDynamicFilterStats());

            nullJoinBuildKeyCount += operator.getNullJoinBuildKeyCount();
            joinBuildKeyCount += operator.getJoinBuildKeyCount();
            nullJoinProbeKeyCount += operator.getNullJoinProbeKeyCount();
            joinProbeKeyCount += operator.getJoinProbeKeyCount();
        }

        return Optional.of(new OperatorStats(
                stageId,
                stageExecutionId,
                pipelineId,
                operatorId,
                planNodeId,
                operatorType,

                totalDrivers,

                isBlockedCalls,
                succinctNanos(isBlockedWall),
                succinctNanos(isBlockedCpu),
                isBlockedAllocation,

                addInputCalls,
                succinctNanos(addInputWall),
                succinctNanos(addInputCpu),
                addInputAllocation,
                rawInputDataSize,
                rawInputPositions,
                inputDataSize,
                inputPositions,
                sumSquaredInputPositions,

                getOutputCalls,
                succinctNanos(getOutputWall),
                succinctNanos(getOutputCpu),
                getOutputAllocation,
                outputDataSize,
                outputPositions,

                physicalWrittenDataSize,

                succinctNanos(additionalCpu),
                succinctNanos(blockedWall),

                finishCalls,
                succinctNanos(finishWall),
                succinctNanos(finishCpu),
                finishAllocation,

                memoryReservation,
                revocableMemoryReservation,
                systemMemoryReservation,
                peakUserMemory,
                peakSystemMemory,
                peakTotalMemory,

                spilledDataSize,

                blockedReason,

                (OperatorInfo) base,
                runtimeStats,
                dynamicFilterStats,
                nullJoinBuildKeyCount,
                joinBuildKeyCount,
                nullJoinProbeKeyCount,
                joinProbeKeyCount));
    }

    @SuppressWarnings("unchecked")
    private static Mergeable<OperatorInfo> getMergeableInfoOrNull(OperatorInfo info)
    {
        Mergeable<OperatorInfo> base = null;
        if (info instanceof Mergeable) {
            base = (Mergeable<OperatorInfo>) info;
        }
        return base;
    }

    @SuppressWarnings("unchecked")
    private static <T> Mergeable<T> mergeInfo(Mergeable<T> base, T other)
    {
        return (Mergeable<T>) base.mergeWith(other);
    }

    public OperatorStats summarize()
    {
        if (info == null || info.isFinal()) {
            return this;
        }
        OperatorInfo info = null;
        return new OperatorStats(
                stageId,
                stageExecutionId,
                pipelineId,
                operatorId,
                planNodeId,
                operatorType,
                totalDrivers,
                isBlockedCalls,
                isBlockedWall,
                isBlockedCpu,
                isBlockedAllocation,
                addInputCalls,
                addInputWall,
                addInputCpu,
                addInputAllocation,
                rawInputDataSize,
                rawInputPositions,
                inputDataSize,
                inputPositions,
                sumSquaredInputPositions,
                getOutputCalls,
                getOutputWall,
                getOutputCpu,
                getOutputAllocation,
                outputDataSize,
                outputPositions,
                physicalWrittenDataSize,
                additionalCpu,
                blockedWall,
                finishCalls,
                finishWall,
                finishCpu,
                finishAllocation,
                userMemoryReservation,
                revocableMemoryReservation,
                systemMemoryReservation,
                peakUserMemoryReservation,
                peakSystemMemoryReservation,
                peakTotalMemoryReservation,
                spilledDataSize,
                blockedReason,
                info,
                runtimeStats,
                dynamicFilterStats,
                nullJoinBuildKeyCount,
                joinBuildKeyCount,
                nullJoinProbeKeyCount,
                joinProbeKeyCount);
    }
}
