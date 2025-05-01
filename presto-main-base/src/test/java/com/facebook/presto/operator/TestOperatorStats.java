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
import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.operator.repartition.PartitionedOutputInfo;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;

import static com.facebook.presto.common.RuntimeUnit.NONE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestOperatorStats
{
    private static final SplitOperatorInfo NON_MERGEABLE_INFO = new SplitOperatorInfo("some_info");
    private static final PartitionedOutputInfo MERGEABLE_INFO = new PartitionedOutputInfo(1, 2, 1024);
    private static final String TEST_METRIC_NAME = "test_metric";
    private static final RuntimeMetric TEST_RUNTIME_METRIC_1 = new RuntimeMetric(TEST_METRIC_NAME, NONE, 10, 2, 9, 1);
    private static final RuntimeMetric TEST_RUNTIME_METRIC_2 = new RuntimeMetric(TEST_METRIC_NAME, NONE, 5, 2, 3, 2);
    private static final DynamicFilterStats TEST_DYNAMIC_FILTER_STATS_1 = new DynamicFilterStats(new HashSet<>(Arrays.asList(new PlanNodeId("1"),
            new PlanNodeId("2"))));
    private static final DynamicFilterStats TEST_DYNAMIC_FILTER_STATS_2 = new DynamicFilterStats(new HashSet<>(Arrays.asList(new PlanNodeId("2"),
            new PlanNodeId("3"))));

    public static final OperatorStats NON_MERGEABLE = new OperatorStats(
            0,
            10,
            1,
            41,
            new PlanNodeId("test"),
            "test",

            1,

            1,
            new Duration(2, NANOSECONDS),
            new Duration(3, NANOSECONDS),
            234L,
            2,
            new Duration(3, NANOSECONDS),
            new Duration(4, NANOSECONDS),
            123L,
            5L,
            10,
            6L,
            7,
            8d,

            9,
            new Duration(10, NANOSECONDS),
            new Duration(11, NANOSECONDS),
            234L,
            12L,
            13,

            14L,

            new Duration(100, NANOSECONDS),
            new Duration(15, NANOSECONDS),

            16,
            new Duration(17, NANOSECONDS),
            new Duration(18, NANOSECONDS),
            345L,

            Long.MAX_VALUE,
            20L,
            21L,
            22L,
            23L,
            24L,
            25L,

            Optional.empty(),

            NON_MERGEABLE_INFO,
            new RuntimeStats(ImmutableMap.of(TEST_METRIC_NAME, RuntimeMetric.copyOf(TEST_RUNTIME_METRIC_1))),
            TEST_DYNAMIC_FILTER_STATS_1,
            0,
            0,
            0,
            0);

    public static final OperatorStats MERGEABLE = new OperatorStats(
            0,
            10,
            1,
            41,
            new PlanNodeId("test"),
            "test",

            1,

            1,
            new Duration(2, NANOSECONDS),
            new Duration(3, NANOSECONDS),
            234L,
            2,
            new Duration(3, NANOSECONDS),
            new Duration(4, NANOSECONDS),
            123L,
            5L,
            10,
            6L,
            7,
            8d,

            9,
            new Duration(10, NANOSECONDS),
            new Duration(11, NANOSECONDS),
            234L,
            12L,
            13,

            14L,

            new Duration(100, NANOSECONDS),
            new Duration(15, NANOSECONDS),

            16,
            new Duration(17, NANOSECONDS),
            new Duration(18, NANOSECONDS),
            345L,

            19,
            20,
            21,
            22,
            23,
            24,
            25,
            Optional.empty(),
            MERGEABLE_INFO,
            new RuntimeStats(ImmutableMap.of(TEST_METRIC_NAME, RuntimeMetric.copyOf(TEST_RUNTIME_METRIC_2))),
            TEST_DYNAMIC_FILTER_STATS_2,
            0,
            0,
            0,
            0);

    @Test
    public void testJson()
    {
        JsonCodec<OperatorStats> codec = JsonCodec.jsonCodec(OperatorStats.class);

        String json = codec.toJson(NON_MERGEABLE);
        OperatorStats actual = codec.fromJson(json);

        assertExpectedOperatorStats(actual);
    }

    private static void assertRuntimeMetricEquals(RuntimeMetric m1, RuntimeMetric m2)
    {
        assertEquals(m1.getName(), m2.getName());
        assertEquals(m1.getSum(), m2.getSum());
        assertEquals(m1.getCount(), m2.getCount());
        assertEquals(m1.getMax(), m2.getMax());
        assertEquals(m1.getMin(), m2.getMin());
    }

    public static void assertExpectedOperatorStats(OperatorStats actual)
    {
        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getStageExecutionId(), 10);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 1);
        assertEquals(actual.getAddInputCalls(), 2);
        assertEquals(actual.getAddInputWall(), new Duration(3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(4, NANOSECONDS));
        assertEquals(actual.getAddInputAllocationInBytes(), 123);
        assertEquals(actual.getRawInputDataSizeInBytes(), 5);
        assertEquals(actual.getInputDataSizeInBytes(), 6);
        assertEquals(actual.getInputPositions(), 7);
        assertEquals(actual.getSumSquaredInputPositions(), 8.0);

        assertEquals(actual.getGetOutputCalls(), 9);
        assertEquals(actual.getGetOutputWall(), new Duration(10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(11, NANOSECONDS));
        assertEquals(actual.getGetOutputAllocationInBytes(), 234);
        assertEquals(actual.getOutputDataSizeInBytes(), 12);
        assertEquals(actual.getOutputPositions(), 13);

        assertEquals(actual.getPhysicalWrittenDataSizeInBytes(), 14);

        assertEquals(actual.getBlockedWall(), new Duration(15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 16);
        assertEquals(actual.getFinishWall(), new Duration(17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(18, NANOSECONDS));
        assertEquals(actual.getFinishAllocationInBytes(), 345);

        assertEquals(actual.getUserMemoryReservationInBytes(), Long.MAX_VALUE);
        assertEquals(actual.getRevocableMemoryReservationInBytes(), 20);
        assertEquals(actual.getSystemMemoryReservationInBytes(), 21);
        assertEquals(actual.getPeakUserMemoryReservationInBytes(), 22);
        assertEquals(actual.getPeakSystemMemoryReservationInBytes(), 23);
        assertEquals(actual.getPeakTotalMemoryReservationInBytes(), 24);
        assertEquals(actual.getSpilledDataSizeInBytes(), 25);
        assertEquals(actual.getInfo().getClass(), SplitOperatorInfo.class);
        assertEquals(((SplitOperatorInfo) actual.getInfo()).getSplitInfo(), NON_MERGEABLE_INFO.getSplitInfo());
        assertRuntimeMetricEquals(actual.getRuntimeStats().getMetric(TEST_METRIC_NAME), TEST_RUNTIME_METRIC_1);
        assertEquals(actual.getDynamicFilterStats().getProducerNodeIds(), TEST_DYNAMIC_FILTER_STATS_1.getProducerNodeIds());
    }

    @Test
    public void testAddMixedStartingWithMergeable()
    {
        OperatorStats actual = OperatorStats.merge(ImmutableList.of(MERGEABLE, NON_MERGEABLE, NON_MERGEABLE)).get();

        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getStageExecutionId(), 10);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 3 * 1);
        assertEquals(actual.getAddInputCalls(), 3 * 2);
        assertEquals(actual.getAddInputWall(), new Duration(3 * 3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(3 * 4, NANOSECONDS));
        assertEquals(actual.getAddInputAllocationInBytes(), 3 * 123);
        assertEquals(actual.getRawInputDataSizeInBytes(), 3 * 5);
        assertEquals(actual.getInputDataSizeInBytes(), 3 * 6);
        assertEquals(actual.getInputPositions(), 3 * 7);
        assertEquals(actual.getSumSquaredInputPositions(), 3 * 8.0);

        assertEquals(actual.getGetOutputCalls(), 3 * 9);
        assertEquals(actual.getGetOutputWall(), new Duration(3 * 10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(3 * 11, NANOSECONDS));
        assertEquals(actual.getGetOutputAllocationInBytes(), 3 * 234);
        assertEquals(actual.getOutputDataSizeInBytes(), 3 * 12);
        assertEquals(actual.getOutputPositions(), 3 * 13);

        assertEquals(actual.getPhysicalWrittenDataSizeInBytes(), 3 * 14);
        assertEquals(actual.getAdditionalCpu(), new Duration(3 * 100, NANOSECONDS));
        assertEquals(actual.getBlockedWall(), new Duration(3 * 15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 3 * 16);
        assertEquals(actual.getFinishWall(), new Duration(3 * 17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(3 * 18, NANOSECONDS));
        assertEquals(actual.getFinishAllocationInBytes(), 3 * 345);

        assertEquals(actual.getUserMemoryReservationInBytes(), Long.MAX_VALUE);
        assertEquals(actual.getRevocableMemoryReservationInBytes(), 3 * 20);
        assertEquals(actual.getSystemMemoryReservationInBytes(), 3 * 21);
        assertEquals(actual.getPeakUserMemoryReservationInBytes(), 22);
        assertEquals(actual.getPeakSystemMemoryReservationInBytes(), 23);
        assertEquals(actual.getPeakTotalMemoryReservationInBytes(), 24);
        assertEquals(actual.getSpilledDataSizeInBytes(), 3 * 25);
        assertNotNull(actual.getInfo());

        RuntimeMetric expectedMetric = RuntimeMetric.merge(TEST_RUNTIME_METRIC_2, TEST_RUNTIME_METRIC_1);
        expectedMetric.mergeWith(TEST_RUNTIME_METRIC_1);
        assertRuntimeMetricEquals(actual.getRuntimeStats().getMetric(TEST_METRIC_NAME), expectedMetric);

        DynamicFilterStats expectedDynamicFilterStats = DynamicFilterStats.copyOf(TEST_DYNAMIC_FILTER_STATS_1);
        expectedDynamicFilterStats.mergeWith(TEST_DYNAMIC_FILTER_STATS_2);
        assertEquals(actual.getDynamicFilterStats().getProducerNodeIds(), expectedDynamicFilterStats.getProducerNodeIds());
    }

    @Test
    public void testSingleNonMergeable()
    {
        OperatorStats actual = OperatorStats.merge(ImmutableList.of(NON_MERGEABLE)).get();

        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getStageExecutionId(), 10);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 1 * 1);
        assertEquals(actual.getAddInputCalls(), 1 * 2);
        assertEquals(actual.getAddInputWall(), new Duration(1 * 3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(1 * 4, NANOSECONDS));
        assertEquals(actual.getAddInputAllocationInBytes(), 1 * 123);
        assertEquals(actual.getRawInputDataSizeInBytes(), 1 * 5);
        assertEquals(actual.getInputDataSizeInBytes(), 1 * 6);
        assertEquals(actual.getInputPositions(), 1 * 7);
        assertEquals(actual.getSumSquaredInputPositions(), 1 * 8.0);

        assertEquals(actual.getGetOutputCalls(), 1 * 9);
        assertEquals(actual.getGetOutputWall(), new Duration(1 * 10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(1 * 11, NANOSECONDS));
        assertEquals(actual.getGetOutputAllocationInBytes(), 1 * 234);
        assertEquals(actual.getOutputDataSizeInBytes(), 1 * 12);
        assertEquals(actual.getOutputPositions(), 1 * 13);

        assertEquals(actual.getPhysicalWrittenDataSizeInBytes(), 1 * 14);
        assertEquals(actual.getAdditionalCpu(), new Duration(1 * 100, NANOSECONDS));
        assertEquals(actual.getBlockedWall(), new Duration(1 * 15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 1 * 16);
        assertEquals(actual.getFinishWall(), new Duration(1 * 17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(1 * 18, NANOSECONDS));
        assertEquals(actual.getFinishAllocationInBytes(), 1 * 345);

        assertEquals(actual.getUserMemoryReservationInBytes(), Long.MAX_VALUE);
        assertEquals(actual.getRevocableMemoryReservationInBytes(), 1 * 20);
        assertEquals(actual.getSystemMemoryReservationInBytes(), 1 * 21);
        assertEquals(actual.getPeakUserMemoryReservationInBytes(), 22);
        assertEquals(actual.getPeakSystemMemoryReservationInBytes(), 23);
        assertEquals(actual.getPeakTotalMemoryReservationInBytes(), 24);
        assertEquals(actual.getSpilledDataSizeInBytes(), 1 * 25);
        assertNotNull(actual.getInfo());

        RuntimeMetric expectedMetric = TEST_RUNTIME_METRIC_1;
        assertRuntimeMetricEquals(actual.getRuntimeStats().getMetric(TEST_METRIC_NAME), expectedMetric);

        DynamicFilterStats expectedDynamicFilterStats = TEST_DYNAMIC_FILTER_STATS_1;
        assertEquals(actual.getDynamicFilterStats().getProducerNodeIds(), TEST_DYNAMIC_FILTER_STATS_1.getProducerNodeIds());
    }

    @Test
    public void testAddMixedStartingWithNonMergeable()
    {
        OperatorStats actual = OperatorStats.merge(ImmutableList.of(NON_MERGEABLE, MERGEABLE, MERGEABLE)).get();

        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getStageExecutionId(), 10);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 3 * 1);
        assertEquals(actual.getAddInputCalls(), 3 * 2);
        assertEquals(actual.getAddInputWall(), new Duration(3 * 3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(3 * 4, NANOSECONDS));
        assertEquals(actual.getAddInputAllocationInBytes(), 3 * 123);
        assertEquals(actual.getRawInputDataSizeInBytes(), 3 * 5);
        assertEquals(actual.getInputDataSizeInBytes(), 3 * 6);
        assertEquals(actual.getInputPositions(), 3 * 7);
        assertEquals(actual.getSumSquaredInputPositions(), 3 * 8.0);

        assertEquals(actual.getGetOutputCalls(), 3 * 9);
        assertEquals(actual.getGetOutputWall(), new Duration(3 * 10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(3 * 11, NANOSECONDS));
        assertEquals(actual.getGetOutputAllocationInBytes(), 3 * 234);
        assertEquals(actual.getOutputDataSizeInBytes(), 3 * 12);
        assertEquals(actual.getOutputPositions(), 3 * 13);

        assertEquals(actual.getPhysicalWrittenDataSizeInBytes(), 3 * 14);
        assertEquals(actual.getAdditionalCpu(), new Duration(3 * 100, NANOSECONDS));
        assertEquals(actual.getBlockedWall(), new Duration(3 * 15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 3 * 16);
        assertEquals(actual.getFinishWall(), new Duration(3 * 17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(3 * 18, NANOSECONDS));
        assertEquals(actual.getFinishAllocationInBytes(), 3 * 345);

        assertEquals(actual.getUserMemoryReservationInBytes(), Long.MAX_VALUE);
        assertEquals(actual.getRevocableMemoryReservationInBytes(), 3 * 20);
        assertEquals(actual.getSystemMemoryReservationInBytes(), 3 * 21);
        assertEquals(actual.getPeakUserMemoryReservationInBytes(), 22);
        assertEquals(actual.getPeakSystemMemoryReservationInBytes(), 23);
        assertEquals(actual.getPeakTotalMemoryReservationInBytes(), 24);
        assertEquals(actual.getSpilledDataSizeInBytes(), 3 * 25);
        assertNull(actual.getInfo());

        RuntimeMetric expectedMetric = RuntimeMetric.merge(TEST_RUNTIME_METRIC_1, TEST_RUNTIME_METRIC_2);
        expectedMetric.mergeWith(TEST_RUNTIME_METRIC_2);
        assertRuntimeMetricEquals(actual.getRuntimeStats().getMetric(TEST_METRIC_NAME), expectedMetric);

        DynamicFilterStats expectedDynamicFilterStats = DynamicFilterStats.copyOf(TEST_DYNAMIC_FILTER_STATS_1);
        expectedDynamicFilterStats.mergeWith(TEST_DYNAMIC_FILTER_STATS_2);
        assertEquals(actual.getDynamicFilterStats().getProducerNodeIds(), TEST_DYNAMIC_FILTER_STATS_1.getProducerNodeIds());
    }

    @Test
    public void testAddNonMergeable()
    {
        OperatorStats actual = OperatorStats.merge(ImmutableList.of(NON_MERGEABLE, NON_MERGEABLE, NON_MERGEABLE)).get();

        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getStageExecutionId(), 10);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 3);
        assertEquals(actual.getAddInputCalls(), 3 * 2);
        assertEquals(actual.getAddInputWall(), new Duration(3 * 3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(3 * 4, NANOSECONDS));
        assertEquals(actual.getAddInputAllocationInBytes(), 3 * 123);
        assertEquals(actual.getRawInputDataSizeInBytes(), 3 * 5);
        assertEquals(actual.getInputDataSizeInBytes(), 3 * 6);
        assertEquals(actual.getInputPositions(), 3 * 7);
        assertEquals(actual.getSumSquaredInputPositions(), 3 * 8.0);

        assertEquals(actual.getGetOutputCalls(), 3 * 9);
        assertEquals(actual.getGetOutputWall(), new Duration(3 * 10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(3 * 11, NANOSECONDS));
        assertEquals(actual.getGetOutputAllocationInBytes(), 3 * 234);
        assertEquals(actual.getOutputDataSizeInBytes(), 3 * 12);
        assertEquals(actual.getOutputPositions(), 3 * 13);

        assertEquals(actual.getPhysicalWrittenDataSizeInBytes(), 3 * 14);
        assertEquals(actual.getAdditionalCpu(), new Duration(3 * 100, NANOSECONDS));
        assertEquals(actual.getBlockedWall(), new Duration(3 * 15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 3 * 16);
        assertEquals(actual.getFinishWall(), new Duration(3 * 17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(3 * 18, NANOSECONDS));
        assertEquals(actual.getFinishAllocationInBytes(), 3 * 345);

        assertEquals(actual.getUserMemoryReservationInBytes(), Long.MAX_VALUE);
        assertEquals(actual.getRevocableMemoryReservationInBytes(), 3 * 20);
        assertEquals(actual.getSystemMemoryReservationInBytes(), 3 * 21);
        assertEquals(actual.getPeakUserMemoryReservationInBytes(), 22);
        assertEquals(actual.getPeakSystemMemoryReservationInBytes(), 23);
        assertEquals(actual.getPeakTotalMemoryReservationInBytes(), 24);
        assertEquals(actual.getSpilledDataSizeInBytes(), 3 * 25);
        assertNull(actual.getInfo());
        RuntimeMetric expectedMetric = RuntimeMetric.merge(TEST_RUNTIME_METRIC_1, TEST_RUNTIME_METRIC_1);
        expectedMetric.mergeWith(TEST_RUNTIME_METRIC_1);
        assertRuntimeMetricEquals(actual.getRuntimeStats().getMetric(TEST_METRIC_NAME), expectedMetric);
        assertEquals(actual.getDynamicFilterStats().getProducerNodeIds(), TEST_DYNAMIC_FILTER_STATS_1.getProducerNodeIds());
    }

    @Test
    public void testMergeWithMergeableInfo()
    {
        OperatorStats actual = OperatorStats.merge(ImmutableList.of(MERGEABLE, MERGEABLE, MERGEABLE)).get();

        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getStageExecutionId(), 10);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 3);
        assertEquals(actual.getAddInputCalls(), 3 * 2);
        assertEquals(actual.getAddInputWall(), new Duration(3 * 3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(3 * 4, NANOSECONDS));
        assertEquals(actual.getAddInputAllocationInBytes(), 3 * 123);
        assertEquals(actual.getRawInputDataSizeInBytes(), 3 * 5);
        assertEquals(actual.getInputDataSizeInBytes(), 3 * 6);
        assertEquals(actual.getInputPositions(), 3 * 7);
        assertEquals(actual.getSumSquaredInputPositions(), 3 * 8.0);

        assertEquals(actual.getGetOutputCalls(), 3 * 9);
        assertEquals(actual.getGetOutputWall(), new Duration(3 * 10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(3 * 11, NANOSECONDS));
        assertEquals(actual.getGetOutputAllocationInBytes(), 3 * 234);
        assertEquals(actual.getOutputDataSizeInBytes(), 3 * 12);
        assertEquals(actual.getOutputPositions(), 3 * 13);

        assertEquals(actual.getPhysicalWrittenDataSizeInBytes(), 3 * 14);

        assertEquals(actual.getAdditionalCpu(), new Duration(3 * 100, NANOSECONDS));
        assertEquals(actual.getBlockedWall(), new Duration(3 * 15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 3 * 16);
        assertEquals(actual.getFinishWall(), new Duration(3 * 17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(3 * 18, NANOSECONDS));
        assertEquals(actual.getFinishAllocationInBytes(), 3 * 345);

        assertEquals(actual.getUserMemoryReservationInBytes(), 3 * 19);
        assertEquals(actual.getRevocableMemoryReservationInBytes(), 3 * 20);
        assertEquals(actual.getSystemMemoryReservationInBytes(), 3 * 21);
        assertEquals(actual.getPeakUserMemoryReservationInBytes(), 22);
        assertEquals(actual.getPeakSystemMemoryReservationInBytes(), 23);
        assertEquals(actual.getPeakTotalMemoryReservationInBytes(), 24);
        assertEquals(actual.getSpilledDataSizeInBytes(), 3 * 25);
        assertEquals(actual.getInfo().getClass(), PartitionedOutputInfo.class);
        assertEquals(((PartitionedOutputInfo) actual.getInfo()).getPagesAdded(), 3 * MERGEABLE_INFO.getPagesAdded());
        RuntimeMetric expectedMetric = RuntimeMetric.merge(TEST_RUNTIME_METRIC_2, TEST_RUNTIME_METRIC_2);
        expectedMetric.mergeWith(TEST_RUNTIME_METRIC_2);
        assertRuntimeMetricEquals(actual.getRuntimeStats().getMetric(TEST_METRIC_NAME), expectedMetric);
        assertEquals(actual.getDynamicFilterStats().getProducerNodeIds(), TEST_DYNAMIC_FILTER_STATS_2.getProducerNodeIds());
    }

    @Test
    public void testMergeEmptyCollection()
    {
        Optional<OperatorStats> merged = OperatorStats.merge(ImmutableList.of());
        assertFalse(merged.isPresent());
    }
}
