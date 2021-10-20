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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestOperatorStats
{
    private static final SplitOperatorInfo NON_MERGEABLE_INFO = new SplitOperatorInfo("some_info");
    private static final PartitionedOutputInfo MERGEABLE_INFO = new PartitionedOutputInfo(1, 2, 1024);
    private static final String TEST_METRIC_NAME = "test_metric";
    private static final RuntimeMetric TEST_RUNTIME_METRIC_1 = new RuntimeMetric(TEST_METRIC_NAME, 10, 2, 9, 1);
    private static final RuntimeMetric TEST_RUNTIME_METRIC_2 = new RuntimeMetric(TEST_METRIC_NAME, 5, 2, 3, 2);

    public static final OperatorStats EXPECTED = new OperatorStats(
            0,
            10,
            1,
            41,
            new PlanNodeId("test"),
            "test",

            1,

            2,
            new Duration(3, NANOSECONDS),
            new Duration(4, NANOSECONDS),
            new DataSize(123, BYTE),
            new DataSize(5, BYTE),
            10,
            new DataSize(6, BYTE),
            7,
            8d,

            9,
            new Duration(10, NANOSECONDS),
            new Duration(11, NANOSECONDS),
            new DataSize(234, BYTE),
            new DataSize(12, BYTE),
            13,

            new DataSize(14, BYTE),

            new Duration(100, NANOSECONDS),
            new Duration(15, NANOSECONDS),

            16,
            new Duration(17, NANOSECONDS),
            new Duration(18, NANOSECONDS),
            new DataSize(345, BYTE),

            new DataSize(19, BYTE),
            new DataSize(20, BYTE),
            new DataSize(21, BYTE),
            new DataSize(22, BYTE),
            new DataSize(23, BYTE),
            new DataSize(24, BYTE),
            new DataSize(25, BYTE),

            Optional.empty(),
            NON_MERGEABLE_INFO,
            new RuntimeStats(ImmutableMap.of(TEST_METRIC_NAME, RuntimeMetric.copyOf(TEST_RUNTIME_METRIC_1))));

    public static final OperatorStats MERGEABLE = new OperatorStats(
            0,
            10,
            1,
            41,
            new PlanNodeId("test"),
            "test",

            1,

            2,
            new Duration(3, NANOSECONDS),
            new Duration(4, NANOSECONDS),
            new DataSize(123, BYTE),
            new DataSize(5, BYTE),
            10,
            new DataSize(6, BYTE),
            7,
            8d,

            9,
            new Duration(10, NANOSECONDS),
            new Duration(11, NANOSECONDS),
            new DataSize(234, BYTE),
            new DataSize(12, BYTE),
            13,

            new DataSize(14, BYTE),

            new Duration(100, NANOSECONDS),
            new Duration(15, NANOSECONDS),

            16,
            new Duration(17, NANOSECONDS),
            new Duration(18, NANOSECONDS),
            new DataSize(345, BYTE),

            new DataSize(19, BYTE),
            new DataSize(20, BYTE),
            new DataSize(21, BYTE),
            new DataSize(22, BYTE),
            new DataSize(23, BYTE),
            new DataSize(24, BYTE),
            new DataSize(25, BYTE),
            Optional.empty(),
            MERGEABLE_INFO,
            new RuntimeStats(ImmutableMap.of(TEST_METRIC_NAME, RuntimeMetric.copyOf(TEST_RUNTIME_METRIC_2))));

    @Test
    public void testJson()
    {
        JsonCodec<OperatorStats> codec = JsonCodec.jsonCodec(OperatorStats.class);

        String json = codec.toJson(EXPECTED);
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
        assertEquals(actual.getAddInputAllocation(), new DataSize(123, BYTE));
        assertEquals(actual.getRawInputDataSize(), new DataSize(5, BYTE));
        assertEquals(actual.getInputDataSize(), new DataSize(6, BYTE));
        assertEquals(actual.getInputPositions(), 7);
        assertEquals(actual.getSumSquaredInputPositions(), 8.0);

        assertEquals(actual.getGetOutputCalls(), 9);
        assertEquals(actual.getGetOutputWall(), new Duration(10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(11, NANOSECONDS));
        assertEquals(actual.getGetOutputAllocation(), new DataSize(234, BYTE));
        assertEquals(actual.getOutputDataSize(), new DataSize(12, BYTE));
        assertEquals(actual.getOutputPositions(), 13);

        assertEquals(actual.getPhysicalWrittenDataSize(), new DataSize(14, BYTE));

        assertEquals(actual.getBlockedWall(), new Duration(15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 16);
        assertEquals(actual.getFinishWall(), new Duration(17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(18, NANOSECONDS));
        assertEquals(actual.getFinishAllocation(), new DataSize(345, BYTE));

        assertEquals(actual.getUserMemoryReservation(), new DataSize(19, BYTE));
        assertEquals(actual.getRevocableMemoryReservation(), new DataSize(20, BYTE));
        assertEquals(actual.getSystemMemoryReservation(), new DataSize(21, BYTE));
        assertEquals(actual.getPeakUserMemoryReservation(), new DataSize(22, BYTE));
        assertEquals(actual.getPeakSystemMemoryReservation(), new DataSize(23, BYTE));
        assertEquals(actual.getPeakTotalMemoryReservation(), new DataSize(24, BYTE));
        assertEquals(actual.getSpilledDataSize(), new DataSize(25, BYTE));
        assertEquals(actual.getInfo().getClass(), SplitOperatorInfo.class);
        assertEquals(((SplitOperatorInfo) actual.getInfo()).getSplitInfo(), NON_MERGEABLE_INFO.getSplitInfo());
        assertRuntimeMetricEquals(actual.getRuntimeStats().getMetric(TEST_METRIC_NAME), TEST_RUNTIME_METRIC_1);
    }

    @Test
    public void testAdd()
    {
        OperatorStats actual = EXPECTED.add(ImmutableList.of(EXPECTED, EXPECTED));

        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getStageExecutionId(), 10);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 3 * 1);
        assertEquals(actual.getAddInputCalls(), 3 * 2);
        assertEquals(actual.getAddInputWall(), new Duration(3 * 3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(3 * 4, NANOSECONDS));
        assertEquals(actual.getAddInputAllocation(), new DataSize(3 * 123, BYTE));
        assertEquals(actual.getRawInputDataSize(), new DataSize(3 * 5, BYTE));
        assertEquals(actual.getInputDataSize(), new DataSize(3 * 6, BYTE));
        assertEquals(actual.getInputPositions(), 3 * 7);
        assertEquals(actual.getSumSquaredInputPositions(), 3 * 8.0);

        assertEquals(actual.getGetOutputCalls(), 3 * 9);
        assertEquals(actual.getGetOutputWall(), new Duration(3 * 10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(3 * 11, NANOSECONDS));
        assertEquals(actual.getGetOutputAllocation(), new DataSize(3 * 234, BYTE));
        assertEquals(actual.getOutputDataSize(), new DataSize(3 * 12, BYTE));
        assertEquals(actual.getOutputPositions(), 3 * 13);

        assertEquals(actual.getPhysicalWrittenDataSize(), new DataSize(3 * 14, BYTE));
        assertEquals(actual.getAdditionalCpu(), new Duration(3 * 100, NANOSECONDS));
        assertEquals(actual.getBlockedWall(), new Duration(3 * 15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 3 * 16);
        assertEquals(actual.getFinishWall(), new Duration(3 * 17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(3 * 18, NANOSECONDS));
        assertEquals(actual.getFinishAllocation(), new DataSize(3 * 345, BYTE));

        assertEquals(actual.getUserMemoryReservation(), new DataSize(3 * 19, BYTE));
        assertEquals(actual.getRevocableMemoryReservation(), new DataSize(3 * 20, BYTE));
        assertEquals(actual.getSystemMemoryReservation(), new DataSize(3 * 21, BYTE));
        assertEquals(actual.getPeakUserMemoryReservation(), new DataSize(22, BYTE));
        assertEquals(actual.getPeakSystemMemoryReservation(), new DataSize(23, BYTE));
        assertEquals(actual.getPeakTotalMemoryReservation(), new DataSize(24, BYTE));
        assertEquals(actual.getSpilledDataSize(), new DataSize(3 * 25, BYTE));
        assertNull(actual.getInfo());
        RuntimeMetric expectedMetric = RuntimeMetric.merge(TEST_RUNTIME_METRIC_1, TEST_RUNTIME_METRIC_1);
        expectedMetric.mergeWith(TEST_RUNTIME_METRIC_1);
        assertRuntimeMetricEquals(actual.getRuntimeStats().getMetric(TEST_METRIC_NAME), expectedMetric);
    }

    @Test
    public void testAddMergeable()
    {
        OperatorStats actual = MERGEABLE.add(ImmutableList.of(MERGEABLE, MERGEABLE));

        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getStageExecutionId(), 10);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 3 * 1);
        assertEquals(actual.getAddInputCalls(), 3 * 2);
        assertEquals(actual.getAddInputWall(), new Duration(3 * 3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(3 * 4, NANOSECONDS));
        assertEquals(actual.getAddInputAllocation(), new DataSize(3 * 123, BYTE));
        assertEquals(actual.getRawInputDataSize(), new DataSize(3 * 5, BYTE));
        assertEquals(actual.getInputDataSize(), new DataSize(3 * 6, BYTE));
        assertEquals(actual.getInputPositions(), 3 * 7);
        assertEquals(actual.getSumSquaredInputPositions(), 3 * 8.0);

        assertEquals(actual.getGetOutputCalls(), 3 * 9);
        assertEquals(actual.getGetOutputWall(), new Duration(3 * 10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(3 * 11, NANOSECONDS));
        assertEquals(actual.getGetOutputAllocation(), new DataSize(3 * 234, BYTE));
        assertEquals(actual.getOutputDataSize(), new DataSize(3 * 12, BYTE));
        assertEquals(actual.getOutputPositions(), 3 * 13);

        assertEquals(actual.getPhysicalWrittenDataSize(), new DataSize(3 * 14, BYTE));

        assertEquals(actual.getAdditionalCpu(), new Duration(3 * 100, NANOSECONDS));
        assertEquals(actual.getBlockedWall(), new Duration(3 * 15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 3 * 16);
        assertEquals(actual.getFinishWall(), new Duration(3 * 17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(3 * 18, NANOSECONDS));
        assertEquals(actual.getFinishAllocation(), new DataSize(3 * 345, BYTE));

        assertEquals(actual.getUserMemoryReservation(), new DataSize(3 * 19, BYTE));
        assertEquals(actual.getRevocableMemoryReservation(), new DataSize(3 * 20, BYTE));
        assertEquals(actual.getSystemMemoryReservation(), new DataSize(3 * 21, BYTE));
        assertEquals(actual.getPeakUserMemoryReservation(), new DataSize(22, BYTE));
        assertEquals(actual.getPeakSystemMemoryReservation(), new DataSize(23, BYTE));
        assertEquals(actual.getPeakTotalMemoryReservation(), new DataSize(24, BYTE));
        assertEquals(actual.getSpilledDataSize(), new DataSize(3 * 25, BYTE));
        assertEquals(actual.getInfo().getClass(), PartitionedOutputInfo.class);
        assertEquals(((PartitionedOutputInfo) actual.getInfo()).getPagesAdded(), 3 * MERGEABLE_INFO.getPagesAdded());
        RuntimeMetric expectedMetric = RuntimeMetric.merge(TEST_RUNTIME_METRIC_2, TEST_RUNTIME_METRIC_2);
        expectedMetric.mergeWith(TEST_RUNTIME_METRIC_2);
        assertRuntimeMetricEquals(actual.getRuntimeStats().getMetric(TEST_METRIC_NAME), expectedMetric);
    }
}
