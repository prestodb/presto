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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.operator.repartition.PartitionedOutputInfo;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.util.Mergeable;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static com.facebook.airlift.units.Duration.succinctNanos;
import static com.facebook.presto.execution.TestingTaskInfos.createOperatorStats;
import static com.facebook.presto.execution.TestingTaskInfos.exactJson;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

public class TestOperatorStatsMerger
{
    private static final JsonCodec<OperatorStats> CODEC = JsonCodec.jsonCodec(OperatorStats.class);
    private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("7");

    @Test
    public void testMatchesLegacyMerge()
    {
        for (int seed = 0; seed < 50; seed++) {
            Optional<OperatorStats> expected = legacyMerge(randomOperators(seed));
            Optional<OperatorStats> merged = OperatorStats.merge(randomOperators(seed));

            OperatorStatsMerger merger = new OperatorStatsMerger();
            randomOperators(seed).forEach(merger::add);
            Optional<OperatorStats> incremental = merger.build();

            assertEquals(merged.isPresent(), expected.isPresent());
            assertEquals(incremental.isPresent(), expected.isPresent());
            if (expected.isPresent()) {
                assertSameStats(merged.get(), expected.get());
                assertSameStats(incremental.get(), expected.get());
            }
        }
    }

    @Test
    public void testSingleOperatorIsReturnedAsIs()
    {
        OperatorStats operator = createOperatorStats(new Random(1), 1, 0, 0, 3, PLAN_NODE_ID, "TableFinishOperator", tableFinishInfo());

        OperatorStatsMerger merger = new OperatorStatsMerger();
        merger.add(operator);
        assertSame(merger.build().get(), operator);
        assertSame(OperatorStats.merge(ImmutableList.of(operator)).get(), operator);
    }

    @Test
    public void testEmpty()
    {
        assertFalse(new OperatorStatsMerger().build().isPresent());
        assertFalse(OperatorStats.merge(ImmutableList.of()).isPresent());
    }

    @Test
    public void testInfoMergedOnlyWhenFirstInfoIsMergeable()
    {
        Random random = new Random(2);
        PartitionedOutputInfo first = new PartitionedOutputInfo(1, 2, 3);
        PartitionedOutputInfo second = new PartitionedOutputInfo(10, 20, 30);
        OperatorStats merged = OperatorStats.merge(ImmutableList.of(
                operator(random, first),
                operator(random, new SplitOperatorInfo("split")),
                operator(random, null),
                operator(random, second))).get();
        PartitionedOutputInfo info = (PartitionedOutputInfo) merged.getInfo();
        assertEquals(info.getRowsAdded(), 11);
        assertEquals(info.getPagesAdded(), 22);

        assertNull(OperatorStats.merge(ImmutableList.of(operator(random, null), operator(random, first))).get().getInfo());
        assertNull(OperatorStats.merge(ImmutableList.of(operator(random, tableFinishInfo()), operator(random, tableFinishInfo()))).get().getInfo());
    }

    @Test
    public void testFinishCpuOverflow()
    {
        Random random = new Random(3);
        List<OperatorStats> operators = ImmutableList.of(
                withFinishCpu(operator(random, null), new Duration(60_000, DAYS)),
                withFinishCpu(operator(random, null), new Duration(60_000, DAYS)));
        OperatorStats merged = OperatorStats.merge(operators).get();
        assertEquals(merged.getFinishCpu(), succinctNanos(Long.MAX_VALUE));
        assertSameStats(merged, legacyMerge(operators).get());
    }

    @Test
    public void testAddAfterBuild()
    {
        OperatorStatsMerger merger = new OperatorStatsMerger();
        merger.build();
        expectThrows(IllegalStateException.class, () -> merger.add(operator(new Random(4), null)));
    }

    private static void assertSameStats(OperatorStats actual, OperatorStats expected)
    {
        assertEquals(exactJson(actual), exactJson(expected));
        assertEquals(CODEC.toJson(actual), CODEC.toJson(expected));
    }

    private static List<OperatorStats> randomOperators(int seed)
    {
        Random random = new Random(seed);
        int count = random.nextInt(6);
        ImmutableList.Builder<OperatorStats> operators = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            operators.add(operator(random, randomInfo(random)));
        }
        return operators.build();
    }

    private static OperatorStats operator(Random random, OperatorInfo info)
    {
        return createOperatorStats(random, 1, 0, 2, 5, PLAN_NODE_ID, "HashAggregationOperator", info);
    }

    private static OperatorInfo randomInfo(Random random)
    {
        switch (random.nextInt(5)) {
            case 0:
                return new PartitionedOutputInfo(random.nextInt(1_000), random.nextInt(1_000), random.nextInt(1_000_000));
            case 1:
                return new HashCollisionsInfo(random.nextDouble() * 1e6, random.nextDouble() * 1e9, random.nextDouble() * 1e6);
            case 2:
                return new SplitOperatorInfo("split-" + random.nextInt(10));
            case 3:
                return tableFinishInfo();
            default:
                return null;
        }
    }

    private static TableFinishInfo tableFinishInfo()
    {
        return new TableFinishInfo("{\"partitions\":[\"ds=2026-10-06\"]}", false, new Duration(28.74, SECONDS), new Duration(1.5, SECONDS));
    }

    private static OperatorStats withFinishCpu(OperatorStats operator, Duration finishCpu)
    {
        return new OperatorStats(
                operator.getStageId(),
                operator.getStageExecutionId(),
                operator.getPipelineId(),
                operator.getOperatorId(),
                operator.getPlanNodeId(),
                operator.getOperatorType(),
                operator.getTotalDrivers(),
                operator.getIsBlockedCalls(),
                operator.getIsBlockedWall(),
                operator.getIsBlockedCpu(),
                operator.getIsBlockedAllocationInBytes(),
                operator.getAddInputCalls(),
                operator.getAddInputWall(),
                operator.getAddInputCpu(),
                operator.getAddInputAllocationInBytes(),
                operator.getRawInputDataSizeInBytes(),
                operator.getRawInputPositions(),
                operator.getInputDataSizeInBytes(),
                operator.getInputPositions(),
                operator.getSumSquaredInputPositions(),
                operator.getGetOutputCalls(),
                operator.getGetOutputWall(),
                operator.getGetOutputCpu(),
                operator.getGetOutputAllocationInBytes(),
                operator.getOutputDataSizeInBytes(),
                operator.getOutputPositions(),
                operator.getPhysicalWrittenDataSizeInBytes(),
                operator.getAdditionalCpu(),
                operator.getBlockedWall(),
                operator.getFinishCalls(),
                operator.getFinishWall(),
                finishCpu,
                operator.getFinishAllocationInBytes(),
                operator.getUserMemoryReservationInBytes(),
                operator.getRevocableMemoryReservationInBytes(),
                operator.getSystemMemoryReservationInBytes(),
                operator.getPeakUserMemoryReservationInBytes(),
                operator.getPeakSystemMemoryReservationInBytes(),
                operator.getPeakTotalMemoryReservationInBytes(),
                operator.getSpilledDataSizeInBytes(),
                operator.getBlockedReason(),
                operator.getInfo(),
                operator.getRuntimeStats(),
                operator.getDynamicFilterStats(),
                operator.getNullJoinBuildKeyCount(),
                operator.getJoinBuildKeyCount(),
                operator.getNullJoinProbeKeyCount(),
                operator.getJoinProbeKeyCount());
    }

    // Reference implementation of the list based merge, which the merger must reproduce exactly
    @SuppressWarnings("unchecked")
    private static Optional<OperatorStats> legacyMerge(List<OperatorStats> operators)
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

            totalDrivers += operator.getTotalDrivers();

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
}
