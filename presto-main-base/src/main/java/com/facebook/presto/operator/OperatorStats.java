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

import com.facebook.airlift.units.Duration;
import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import jakarta.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.units.Duration.succinctNanos;
import static com.google.common.base.Preconditions.checkArgument;
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
    private final long isBlockedAllocationInBytes;

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

    private final Duration additionalCpu;
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
            @JsonProperty("isBlockedAllocationInBytes") long isBlockedAllocationInBytes,

            @JsonProperty("addInputCalls") long addInputCalls,
            @JsonProperty("addInputWall") Duration addInputWall,
            @JsonProperty("addInputCpu") Duration addInputCpu,
            @JsonProperty("addInputAllocationInBytes") long addInputAllocationInBytes,
            @JsonProperty("rawInputDataSizeInBytes") long rawInputDataSizeInBytes,
            @JsonProperty("rawInputPositions") long rawInputPositions,
            @JsonProperty("inputDataSizeInBytes") long inputDataSizeInBytes,
            @JsonProperty("inputPositions") long inputPositions,
            @JsonProperty("sumSquaredInputPositions") double sumSquaredInputPositions,

            @JsonProperty("getOutputCalls") long getOutputCalls,
            @JsonProperty("getOutputWall") Duration getOutputWall,
            @JsonProperty("getOutputCpu") Duration getOutputCpu,
            @JsonProperty("getOutputAllocationInBytes") long getOutputAllocationInBytes,
            @JsonProperty("outputDataSizeInBytes") long outputDataSizeInBytes,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("physicalWrittenDataSizeInBytes") long physicalWrittenDataSizeInBytes,

            @JsonProperty("additionalCpu") Duration additionalCpu,
            @JsonProperty("blockedWall") Duration blockedWall,

            @JsonProperty("finishCalls") long finishCalls,
            @JsonProperty("finishWall") Duration finishWall,
            @JsonProperty("finishCpu") Duration finishCpu,
            @JsonProperty("finishAllocationInBytes") long finishAllocationInBytes,

            @JsonProperty("userMemoryReservationInBytes") long userMemoryReservationInBytes,
            @JsonProperty("revocableMemoryReservationInBytes") long revocableMemoryReservationInBytes,
            @JsonProperty("systemMemoryReservationInBytes") long systemMemoryReservationInBytes,
            @JsonProperty("peakUserMemoryReservationInBytes") long peakUserMemoryReservationInBytes,
            @JsonProperty("peakSystemMemoryReservationInBytes") long peakSystemMemoryReservationInBytes,
            @JsonProperty("peakTotalMemoryReservationInBytes") long peakTotalMemoryReservationInBytes,

            @JsonProperty("spilledDataSizeInBytes") long spilledDataSizeInBytes,

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
        checkArgument(isBlockedAllocationInBytes >= 0, "isBlockedAllocationInBytes is negative");
        this.isBlockedAllocationInBytes = isBlockedAllocationInBytes;

        this.addInputCalls = addInputCalls;
        this.addInputWall = requireNonNull(addInputWall, "addInputWall is null");
        this.addInputCpu = requireNonNull(addInputCpu, "addInputCpu is null");
        checkArgument(addInputAllocationInBytes >= 0, "addInputAllocationInBytes is negative");
        this.addInputAllocationInBytes = addInputAllocationInBytes;
        checkArgument(rawInputDataSizeInBytes >= 0, "rawInputDataSizeInBytes is negative");
        this.rawInputDataSizeInBytes = rawInputDataSizeInBytes;
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;
        checkArgument(inputDataSizeInBytes >= 0, "inputDataSizeInBytes is negative");
        this.inputDataSizeInBytes = inputDataSizeInBytes;
        checkArgument(inputPositions >= 0, "inputPositions is negative");
        this.inputPositions = inputPositions;
        this.sumSquaredInputPositions = sumSquaredInputPositions;

        this.getOutputCalls = getOutputCalls;
        this.getOutputWall = requireNonNull(getOutputWall, "getOutputWall is null");
        this.getOutputCpu = requireNonNull(getOutputCpu, "getOutputCpu is null");
        checkArgument(getOutputAllocationInBytes >= 0, "getOutputAllocationInBytes is negative");
        this.getOutputAllocationInBytes = getOutputAllocationInBytes;

        // An overflow could have occurred on this stat - handle this gracefully.
        this.outputDataSizeInBytes = (outputDataSizeInBytes >= 0) ? outputDataSizeInBytes : Long.MAX_VALUE;

        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        checkArgument(physicalWrittenDataSizeInBytes >= 0, "writtenDataSizeInBytes is negative");
        this.physicalWrittenDataSizeInBytes = physicalWrittenDataSizeInBytes;
        this.additionalCpu = requireNonNull(additionalCpu, "additionalCpu is negative");
        this.blockedWall = requireNonNull(blockedWall, "blockedWall is null");

        this.finishCalls = finishCalls;
        this.finishWall = requireNonNull(finishWall, "finishWall is null");
        this.finishCpu = requireNonNull(finishCpu, "finishCpu is null");
        checkArgument(finishAllocationInBytes >= 0, "finishAllocationInBytes is negative");
        this.finishAllocationInBytes = finishAllocationInBytes;
        checkArgument(userMemoryReservationInBytes >= 0, "userMemoryReservationInBytes is negative");
        this.userMemoryReservationInBytes = userMemoryReservationInBytes;
        checkArgument(revocableMemoryReservationInBytes >= 0, "revocableMemoryReservationInBytes is negative");
        this.revocableMemoryReservationInBytes = revocableMemoryReservationInBytes;
        checkArgument(systemMemoryReservationInBytes >= 0, "systemMemoryReservationInBytes is negative");
        this.systemMemoryReservationInBytes = systemMemoryReservationInBytes;
        checkArgument(peakUserMemoryReservationInBytes >= 0, "peakUserMemoryReservationInBytes is negative");
        this.peakUserMemoryReservationInBytes = peakUserMemoryReservationInBytes;
        checkArgument(peakSystemMemoryReservationInBytes >= 0, "peakSystemMemoryReservationInBytes is negative");
        this.peakSystemMemoryReservationInBytes = peakSystemMemoryReservationInBytes;
        checkArgument(peakTotalMemoryReservationInBytes >= 0, "peakTotalMemoryReservationInBytes is negative");
        this.peakTotalMemoryReservationInBytes = peakTotalMemoryReservationInBytes;
        checkArgument(spilledDataSizeInBytes >= 0, "spilledDataSizeInBytes is negative");
        this.spilledDataSizeInBytes = spilledDataSizeInBytes;
        this.runtimeStats = runtimeStats;

        this.dynamicFilterStats = dynamicFilterStats;

        this.blockedReason = blockedReason;

        this.info = info;
        this.infoUnion = (info != null) ? OperatorInfoUnion.convertToOperatorInfoUnion(info) : null;
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
            long isBlockedAllocationInBytes,

            long addInputCalls,
            Duration addInputWall,
            Duration addInputCpu,
            long addInputAllocationInBytes,
            long rawInputDataSizeInBytes,
            long rawInputPositions,
            long inputDataSizeInBytes,
            long inputPositions,
            double sumSquaredInputPositions,

            long getOutputCalls,
            Duration getOutputWall,
            Duration getOutputCpu,
            long getOutputAllocationInBytes,
            long outputDataSizeInBytes,
            long outputPositions,

            long physicalWrittenDataSizeInBytes,

            Duration additionalCpu,
            Duration blockedWall,

            long finishCalls,
            Duration finishWall,
            Duration finishCpu,
            long finishAllocationInBytes,

            long userMemoryReservationInBytes,
            long revocableMemoryReservationInBytes,
            long systemMemoryReservationInBytes,
            long peakUserMemoryReservationInBytes,
            long peakSystemMemoryReservationInBytes,
            long peakTotalMemoryReservationInBytes,

            long spilledDataSizeInBytes,

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
        checkArgument(isBlockedAllocationInBytes >= 0, "isBlockedAllocation is negative");
        this.isBlockedAllocationInBytes = isBlockedAllocationInBytes;

        this.addInputCalls = addInputCalls;
        this.addInputWall = requireNonNull(addInputWall, "addInputWall is null");
        this.addInputCpu = requireNonNull(addInputCpu, "addInputCpu is null");
        checkArgument(addInputAllocationInBytes >= 0, "addInputAllocation is negative");
        this.addInputAllocationInBytes = addInputAllocationInBytes;
        checkArgument(rawInputDataSizeInBytes >= 0, "rawInputDataSize is negative");
        this.rawInputDataSizeInBytes = rawInputDataSizeInBytes;
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;

        checkArgument(inputDataSizeInBytes >= 0, "inputDataSize is negative");
        this.inputDataSizeInBytes = inputDataSizeInBytes;
        checkArgument(inputPositions >= 0, "inputPositions is negative");
        this.inputPositions = inputPositions;
        this.sumSquaredInputPositions = sumSquaredInputPositions;

        this.getOutputCalls = getOutputCalls;
        this.getOutputWall = requireNonNull(getOutputWall, "getOutputWall is null");
        this.getOutputCpu = requireNonNull(getOutputCpu, "getOutputCpu is null");
        checkArgument(getOutputAllocationInBytes >= 0, "getOutputAllocation is negative");
        this.getOutputAllocationInBytes = getOutputAllocationInBytes;
        checkArgument(outputDataSizeInBytes >= 0, "outputDataSize is negative");
        this.outputDataSizeInBytes = outputDataSizeInBytes;
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        checkArgument(physicalWrittenDataSizeInBytes >= 0, "writtenDataSize is negative");
        this.physicalWrittenDataSizeInBytes = physicalWrittenDataSizeInBytes;
        this.additionalCpu = requireNonNull(additionalCpu, "additionalCpu is null");
        this.blockedWall = requireNonNull(blockedWall, "blockedWall is null");

        this.finishCalls = finishCalls;
        this.finishWall = requireNonNull(finishWall, "finishWall is null");
        this.finishCpu = requireNonNull(finishCpu, "finishCpu is null");
        checkArgument(finishAllocationInBytes >= 0, "finishAllocation is negative");
        this.finishAllocationInBytes = finishAllocationInBytes;
        checkArgument(userMemoryReservationInBytes >= 0, "userMemoryReservation is negative");
        this.userMemoryReservationInBytes = userMemoryReservationInBytes;
        checkArgument(revocableMemoryReservationInBytes >= 0, "revocableMemoryReservation is negative");
        this.revocableMemoryReservationInBytes = revocableMemoryReservationInBytes;
        checkArgument(systemMemoryReservationInBytes >= 0, "systemMemoryReservation is negative");
        this.systemMemoryReservationInBytes = systemMemoryReservationInBytes;

        checkArgument(peakUserMemoryReservationInBytes >= 0, "peakUserMemoryReservation is negative");
        this.peakUserMemoryReservationInBytes = peakUserMemoryReservationInBytes;
        checkArgument(peakSystemMemoryReservationInBytes >= 0, "peakSystemMemoryReservation is negative");
        this.peakSystemMemoryReservationInBytes = peakSystemMemoryReservationInBytes;
        checkArgument(peakTotalMemoryReservationInBytes >= 0, "peakTotalMemoryReservation is negative");
        this.peakTotalMemoryReservationInBytes = peakTotalMemoryReservationInBytes;

        checkArgument(spilledDataSizeInBytes >= 0, "spilledDataSize is negative");
        this.spilledDataSizeInBytes = spilledDataSizeInBytes;

        this.runtimeStats = runtimeStats;

        this.dynamicFilterStats = dynamicFilterStats;

        this.blockedReason = blockedReason;

        this.infoUnion = infoUnion;
        this.info = (infoUnion != null) ? OperatorInfoUnion.convertToOperatorInfo(infoUnion) : null;
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
    public long getAddInputAllocationInBytes()
    {
        return addInputAllocationInBytes;
    }

    @JsonProperty
    @ThriftField(12)
    public long getRawInputDataSizeInBytes()
    {
        return rawInputDataSizeInBytes;
    }

    @JsonProperty
    @ThriftField(13)
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    @ThriftField(14)
    public long getInputDataSizeInBytes()
    {
        return inputDataSizeInBytes;
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
    public long getGetOutputAllocationInBytes()
    {
        return getOutputAllocationInBytes;
    }

    @JsonProperty
    @ThriftField(21)
    public long getOutputDataSizeInBytes()
    {
        return outputDataSizeInBytes;
    }

    @JsonProperty
    @ThriftField(22)
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    @ThriftField(23)
    public long getPhysicalWrittenDataSizeInBytes()
    {
        return physicalWrittenDataSizeInBytes;
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
    public long getFinishAllocationInBytes()
    {
        return finishAllocationInBytes;
    }

    @JsonProperty
    @ThriftField(30)
    public long getUserMemoryReservationInBytes()
    {
        return userMemoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(31)
    public long getRevocableMemoryReservationInBytes()
    {
        return revocableMemoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(32)
    public long getSystemMemoryReservationInBytes()
    {
        return systemMemoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(33)
    public long getPeakUserMemoryReservationInBytes()
    {
        return peakUserMemoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(34)
    public long getPeakSystemMemoryReservationInBytes()
    {
        return peakSystemMemoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(35)
    public long getPeakTotalMemoryReservationInBytes()
    {
        return peakTotalMemoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(36)
    public long getSpilledDataSizeInBytes()
    {
        return spilledDataSizeInBytes;
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
        if (info == null && infoUnion != null) {
            return OperatorInfoUnion.convertToOperatorInfo(infoUnion);
        }
        return info;
    }

    @Nullable
    @ThriftField(39)
    public OperatorInfoUnion getInfoUnion()
    {
        if (infoUnion == null && info != null) {
            return OperatorInfoUnion.convertToOperatorInfoUnion(info);
        }
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
    public long getIsBlockedAllocationInBytes()
    {
        return isBlockedAllocationInBytes;
    }

    public static Optional<OperatorStats> merge(List<OperatorStats> operators)
    {
        if (operators.isEmpty()) {
            return Optional.empty();
        }

        if (operators.size() == 1) {
            return Optional.of(operators.get(0));
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
        double addInputAllocation = 0;
        double rawInputDataSize = 0;
        long rawInputPositions = 0;
        double inputDataSize = 0;
        long inputPositions = 0;
        double sumSquaredInputPositions = 0.0;

        long getOutputCalls = 0;
        long getOutputWall = 0;
        long getOutputCpu = 0;
        double getOutputAllocation = 0;
        double outputDataSize = 0;
        long outputPositions = 0;

        double physicalWrittenDataSize = 0;

        long additionalCpu = 0;
        long blockedWall = 0;

        long finishCalls = 0;
        long finishWall = 0;
        long finishCpu = 0;
        long finishAllocation = 0;

        double memoryReservation = 0;
        double revocableMemoryReservation = 0;
        double systemMemoryReservation = 0;
        double peakUserMemory = 0;
        double peakSystemMemory = 0;
        double peakTotalMemory = 0;

        double spilledDataSize = 0;

        long nullJoinBuildKeyCount = 0;
        long joinBuildKeyCount = 0;
        long nullJoinProbeKeyCount = 0;
        long joinProbeKeyCount = 0;

        RuntimeStats runtimeStats = new RuntimeStats();
        DynamicFilterStats dynamicFilterStats = new DynamicFilterStats(new HashSet<>());

        Optional<BlockedReason> blockedReason = Optional.empty();

        boolean mergeInfo = first.getInfo() instanceof Mergeable;
        Mergeable<OperatorInfo> base = null;

        for (OperatorStats operator : operators) {
            checkArgument(operator.getOperatorId() == operatorId, "Expected operatorId to be %s but was %s", operatorId, operator.getOperatorId());

            totalDrivers += operator.totalDrivers;

            isBlockedCalls += operator.getGetOutputCalls();
            isBlockedWall += operator.getGetOutputWall().roundTo(NANOSECONDS);
            isBlockedCpu += operator.getGetOutputCpu().roundTo(NANOSECONDS);
            isBlockedAllocation += operator.getIsBlockedAllocationInBytes();

            addInputCalls += operator.getAddInputCalls();
            addInputWall += operator.getAddInputWall().roundTo(NANOSECONDS);
            addInputCpu += operator.getAddInputCpu().roundTo(NANOSECONDS);
            addInputAllocation += operator.getAddInputAllocationInBytes();
            rawInputDataSize += operator.getRawInputDataSizeInBytes();
            rawInputPositions += operator.getRawInputPositions();
            inputDataSize += operator.getInputDataSizeInBytes();
            inputPositions += operator.getInputPositions();
            sumSquaredInputPositions += operator.getSumSquaredInputPositions();

            getOutputCalls += operator.getGetOutputCalls();
            getOutputWall += operator.getGetOutputWall().roundTo(NANOSECONDS);
            getOutputCpu += operator.getGetOutputCpu().roundTo(NANOSECONDS);
            getOutputAllocation += operator.getGetOutputAllocationInBytes();
            outputDataSize += operator.getOutputDataSizeInBytes();
            outputPositions += operator.getOutputPositions();

            physicalWrittenDataSize += operator.getPhysicalWrittenDataSizeInBytes();

            finishCalls += operator.getFinishCalls();
            finishWall += operator.getFinishWall().roundTo(NANOSECONDS);
            finishCpu += operator.getFinishCpu().roundTo(NANOSECONDS);
            finishAllocation += operator.getFinishAllocationInBytes();

            additionalCpu += operator.getAdditionalCpu().roundTo(NANOSECONDS);
            blockedWall += operator.getBlockedWall().roundTo(NANOSECONDS);

            memoryReservation += operator.getUserMemoryReservationInBytes();
            revocableMemoryReservation += operator.getRevocableMemoryReservationInBytes();
            systemMemoryReservation += operator.getSystemMemoryReservationInBytes();

            peakUserMemory = max(peakUserMemory, operator.getPeakUserMemoryReservationInBytes());
            peakSystemMemory = max(peakSystemMemory, operator.getPeakSystemMemoryReservationInBytes());
            peakTotalMemory = max(peakTotalMemory, operator.getPeakTotalMemoryReservationInBytes());

            spilledDataSize += operator.getSpilledDataSizeInBytes();

            if (operator.getBlockedReason().isPresent()) {
                blockedReason = operator.getBlockedReason();
            }

            OperatorInfo info = operator.getInfo();
            if (mergeInfo) {
                if (base == null) {
                    base = (Mergeable<OperatorInfo>) info;
                }
                else if (info != null && info.getClass() == base.getClass()) {
                    base = mergeInfo(base, info);
                }
            }

            runtimeStats.mergeWith(operator.getRuntimeStats());
            dynamicFilterStats.mergeWith(operator.getDynamicFilterStats());

            nullJoinBuildKeyCount += operator.getNullJoinBuildKeyCount();
            joinBuildKeyCount += operator.getJoinBuildKeyCount();
            nullJoinProbeKeyCount += operator.getNullJoinProbeKeyCount();
            joinProbeKeyCount += operator.getJoinProbeKeyCount();
        }
        if (finishCpu < 0) {
            finishCpu = Long.MAX_VALUE;
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
                (long) addInputAllocation,
                (long) rawInputDataSize,
                rawInputPositions,
                (long) inputDataSize,
                inputPositions,
                sumSquaredInputPositions,

                getOutputCalls,
                succinctNanos(getOutputWall),
                succinctNanos(getOutputCpu),
                (long) getOutputAllocation,
                (long) outputDataSize,
                outputPositions,

                (long) physicalWrittenDataSize,

                succinctNanos(additionalCpu),
                succinctNanos(blockedWall),

                finishCalls,
                succinctNanos(finishWall),
                succinctNanos(finishCpu),
                finishAllocation,

                (long) memoryReservation,
                (long) revocableMemoryReservation,
                (long) systemMemoryReservation,
                (long) peakUserMemory,
                (long) peakSystemMemory,
                (long) peakTotalMemory,

                (long) spilledDataSize,

                blockedReason,

                mergeInfo ? (OperatorInfo) base : null,
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
                isBlockedAllocationInBytes,
                addInputCalls,
                addInputWall,
                addInputCpu,
                addInputAllocationInBytes,
                rawInputDataSizeInBytes,
                rawInputPositions,
                inputDataSizeInBytes,
                inputPositions,
                sumSquaredInputPositions,
                getOutputCalls,
                getOutputWall,
                getOutputCpu,
                getOutputAllocationInBytes,
                outputDataSizeInBytes,
                outputPositions,
                physicalWrittenDataSizeInBytes,
                additionalCpu,
                blockedWall,
                finishCalls,
                finishWall,
                finishCpu,
                finishAllocationInBytes,
                userMemoryReservationInBytes,
                revocableMemoryReservationInBytes,
                systemMemoryReservationInBytes,
                peakUserMemoryReservationInBytes,
                peakSystemMemoryReservationInBytes,
                peakTotalMemoryReservationInBytes,
                spilledDataSizeInBytes,
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
