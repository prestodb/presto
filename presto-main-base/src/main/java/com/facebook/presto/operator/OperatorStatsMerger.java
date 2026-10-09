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
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.util.Mergeable;

import java.util.HashSet;
import java.util.Optional;

import static com.facebook.airlift.units.Duration.succinctNanos;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Merges the {@link OperatorStats} of one operator one at a time. Adding operators in a given order
 * and calling {@link #build()} gives the same result as {@link OperatorStats#merge} on a list in
 * that order. The first operator is only read once a second one is added, so added operators must
 * not be modified. The merger must not be used after {@link #build()}.
 */
public class OperatorStatsMerger
{
    private OperatorStats first;
    private int count;
    private boolean built;

    private int stageId;
    private int stageExecutionId;
    private int pipelineId;
    private int operatorId;
    private PlanNodeId planNodeId;
    private String operatorType;

    private long totalDrivers;

    private long isBlockedCalls;
    private long isBlockedWall;
    private long isBlockedCpu;
    private long isBlockedAllocation;

    private long addInputCalls;
    private long addInputWall;
    private long addInputCpu;
    private double addInputAllocation;
    private double rawInputDataSize;
    private long rawInputPositions;
    private double inputDataSize;
    private long inputPositions;
    private double sumSquaredInputPositions;

    private long getOutputCalls;
    private long getOutputWall;
    private long getOutputCpu;
    private double getOutputAllocation;
    private double outputDataSize;
    private long outputPositions;

    private double physicalWrittenDataSize;

    private long additionalCpu;
    private long blockedWall;

    private long finishCalls;
    private long finishWall;
    private long finishCpu;
    private long finishAllocation;

    private double memoryReservation;
    private double revocableMemoryReservation;
    private double systemMemoryReservation;
    private double peakUserMemory;
    private double peakSystemMemory;
    private double peakTotalMemory;

    private double spilledDataSize;

    private long nullJoinBuildKeyCount;
    private long joinBuildKeyCount;
    private long nullJoinProbeKeyCount;
    private long joinProbeKeyCount;

    private RuntimeStats runtimeStats;
    private DynamicFilterStats dynamicFilterStats;

    private Optional<BlockedReason> blockedReason = Optional.empty();

    private boolean mergeInfo;
    private Mergeable<OperatorInfo> base;

    public void add(OperatorStats operator)
    {
        requireNonNull(operator, "operator is null");
        checkState(!built, "build() has already been called");
        if (count == 0) {
            first = operator;
        }
        else {
            if (count == 1) {
                start(first);
                first = null;
            }
            accumulate(operator);
        }
        count++;
    }

    /**
     * Returns the merged stats, empty if no operator was added, or the added instance itself if
     * exactly one operator was added.
     */
    public Optional<OperatorStats> build()
    {
        built = true;
        if (count == 0) {
            return Optional.empty();
        }
        if (count == 1) {
            return Optional.of(first);
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
                succinctNanos(finishCpu < 0 ? Long.MAX_VALUE : finishCpu),
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

    private void start(OperatorStats first)
    {
        stageId = first.getStageId();
        operatorId = first.getOperatorId();
        stageExecutionId = first.getStageExecutionId();
        pipelineId = first.getPipelineId();
        planNodeId = first.getPlanNodeId();
        operatorType = first.getOperatorType();

        runtimeStats = new RuntimeStats();
        dynamicFilterStats = new DynamicFilterStats(new HashSet<>());

        mergeInfo = first.getInfo() instanceof Mergeable;

        accumulate(first);
    }

    @SuppressWarnings("unchecked")
    private void accumulate(OperatorStats operator)
    {
        checkArgument(operator.getOperatorId() == operatorId, "Expected operatorId to be %s but was %s", operatorId, operator.getOperatorId());

        totalDrivers += operator.getTotalDrivers();

        // isBlockedCalls, isBlockedWall and isBlockedCpu accumulate the getOutput* values; kept for compatibility with existing merged stats
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
                base = (Mergeable<OperatorInfo>) base.mergeWith(info);
            }
        }

        runtimeStats.mergeWith(operator.getRuntimeStats());
        dynamicFilterStats.mergeWith(operator.getDynamicFilterStats());

        nullJoinBuildKeyCount += operator.getNullJoinBuildKeyCount();
        joinBuildKeyCount += operator.getJoinBuildKeyCount();
        nullJoinProbeKeyCount += operator.getNullJoinProbeKeyCount();
        joinProbeKeyCount += operator.getJoinProbeKeyCount();
    }
}
