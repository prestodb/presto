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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.stats.Distribution;
import com.facebook.airlift.testing.TestingTicker;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.operator.DynamicFilterStats;
import com.facebook.presto.operator.ExchangeOperator;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.HashBuilderOperator;
import com.facebook.presto.operator.LookupJoinOperator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.ScanFilterAndProjectOperator;
import com.facebook.presto.operator.TableWriterOperator;
import com.facebook.presto.operator.TaskOutputOperator;
import com.facebook.presto.operator.exchange.LocalExchangeSinkOperator;
import com.facebook.presto.operator.exchange.LocalExchangeSource;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.units.DataSize.Unit.BYTE;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestQueryStats
{
    private static final String TEST_METRIC_NAME = "test_metric";
    private static final RuntimeMetric TEST_RUNTIME_METRIC_1 = new RuntimeMetric(TEST_METRIC_NAME, NONE, 10, 2, 9, 1);
    private static final RuntimeMetric TEST_RUNTIME_METRIC_2 = new RuntimeMetric(TEST_METRIC_NAME, NONE, 5, 2, 3, 2);
    private static final DynamicFilterStats TEST_DYNAMIC_FILTER_STATS_1 = new DynamicFilterStats(ImmutableSet.of(new PlanNodeId("1"), new PlanNodeId("2")));
    private static final DynamicFilterStats TEST_DYNAMIC_FILTER_STATS_2 = new DynamicFilterStats(ImmutableSet.of(new PlanNodeId("2"), new PlanNodeId("3")));
    private static final List<OperatorStats> OPERATOR_SUMMARIES = ImmutableList.of(
            new OperatorStats(
                    10,
                    101,
                    11,
                    12,
                    new PlanNodeId("13"),
                    TableWriterOperator.class.getSimpleName(),
                    14L,
                    14L,
                    new Duration(15, NANOSECONDS),
                    new Duration(16, NANOSECONDS),
                    121,
                    15L,
                    new Duration(16, NANOSECONDS),
                    new Duration(17, NANOSECONDS),
                    123,
                    18L,
                    200,
                    19L,
                    110L,
                    111.0,
                    112L,
                    new Duration(113, NANOSECONDS),
                    new Duration(114, NANOSECONDS),
                    234,
                    116L,
                    117L,
                    118L,
                    new Duration(1000, NANOSECONDS),
                    new Duration(119, NANOSECONDS),
                    120L,
                    new Duration(121, NANOSECONDS),
                    new Duration(122, NANOSECONDS),
                    345,
                    124L,
                    125L,
                    126L,
                    127L,
                    128L,
                    129L,
                    130L,
                    Optional.empty(),
                    null,
                    new RuntimeStats(ImmutableMap.of(TEST_METRIC_NAME, RuntimeMetric.copyOf(TEST_RUNTIME_METRIC_1))),
                    TEST_DYNAMIC_FILTER_STATS_1,
                    0,
                    0,
                    0,
                    0),
            new OperatorStats(
                    20,
                    201,
                    21,
                    22,
                    new PlanNodeId("23"),
                    FilterAndProjectOperator.class.getSimpleName(),
                    24L,
                    24L,
                    new Duration(25, NANOSECONDS),
                    new Duration(26, NANOSECONDS),
                    1210,
                    25L,
                    new Duration(26, NANOSECONDS),
                    new Duration(27, NANOSECONDS),
                    1230,
                    28L,
                    250,
                    29L,
                    210L,
                    211.0,
                    212L,
                    new Duration(213, NANOSECONDS),
                    new Duration(214, NANOSECONDS),
                    2340,
                    216L,
                    217L,
                    218L,
                    new Duration(2000, NANOSECONDS),
                    new Duration(219, NANOSECONDS),
                    220L,
                    new Duration(221, NANOSECONDS),
                    new Duration(222, NANOSECONDS),
                    3450,
                    224L,
                    225L,
                    226L,
                    227L,
                    228L,
                    229L,
                    230L,
                    Optional.empty(),
                    null,
                    new RuntimeStats(ImmutableMap.of(TEST_METRIC_NAME, RuntimeMetric.copyOf(TEST_RUNTIME_METRIC_2))),
                    TEST_DYNAMIC_FILTER_STATS_2,
                    0,
                    0,
                    0,
                    0),
            new OperatorStats(
                    30,
                    301,
                    31,
                    32,
                    new PlanNodeId("33"),
                    TableWriterOperator.class.getSimpleName(),
                    34L,
                    34L,
                    new Duration(35, NANOSECONDS),
                    new Duration(36, NANOSECONDS),
                    12100,
                    35L,
                    new Duration(36, NANOSECONDS),
                    new Duration(37, NANOSECONDS),
                    12300,
                    38L,
                    350,
                    39L,
                    310L,
                    311.0,
                    312L,
                    new Duration(313, NANOSECONDS),
                    new Duration(314, NANOSECONDS),
                    23400,
                    316L,
                    317L,
                    318L,
                    new Duration(3000, NANOSECONDS),
                    new Duration(319, NANOSECONDS),
                    320L,
                    new Duration(321, NANOSECONDS),
                    new Duration(322, NANOSECONDS),
                    34500,
                    324L,
                    325L,
                    326L,
                    327L,
                    328L,
                    329L,
                    330L,
                    Optional.empty(),
                    null,
                    new RuntimeStats(),
                    new DynamicFilterStats(ImmutableSet.of()),
                    0,
                    0,
                    0,
                    0));

    static final QueryStats EXPECTED = new QueryStats(
            1L,
            2L,
            3L,
            4L,
            new Duration(6, NANOSECONDS),
            new Duration(7, NANOSECONDS),
            new Duration(5, NANOSECONDS),
            new Duration(31, NANOSECONDS),
            new Duration(15, NANOSECONDS),
            new Duration(15, NANOSECONDS),
            new Duration(32, NANOSECONDS),
            new Duration(41, NANOSECONDS),
            new Duration(7, NANOSECONDS),

            new Duration(100, NANOSECONDS),
            new Duration(200, NANOSECONDS),

            9,
            10,
            11,
            11,

            12,
            13,
            15,
            30,
            16,

            12,
            13,
            15,
            16,

            12,
            13,
            15,
            16,

            17.0,
            43.0,
            new DataSize(18, BYTE),
            new DataSize(19, BYTE),
            new DataSize(20, BYTE),
            new DataSize(21, BYTE),
            new DataSize(22, BYTE),
            new DataSize(23, BYTE),
            new DataSize(42, BYTE),

            true,
            new Duration(20, NANOSECONDS),
            new Duration(21, NANOSECONDS),
            new Duration(0, NANOSECONDS),
            new Duration(23, NANOSECONDS),
            false,
            ImmutableSet.of(),

            new DataSize(123, BYTE),

            new DataSize(24, BYTE),
            25,

            new DataSize(26, BYTE),
            27,

            new DataSize(30, BYTE),
            29,

            new DataSize(28, BYTE),
            29,

            30,
            new DataSize(31, BYTE),
            new DataSize(32, BYTE),

            new DataSize(33, BYTE),

            ImmutableList.of(new StageGcStatistics(
                    101,
                    1001,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107)),

            OPERATOR_SUMMARIES,
            new RuntimeStats(ImmutableMap.of(TEST_METRIC_NAME, RuntimeMetric.merge(TEST_RUNTIME_METRIC_1, TEST_RUNTIME_METRIC_2))));

    @Test
    public void testInputAndOutputStatsCalculation()
    {
        // First of all, we build a StageInfo including 2 stages, it's architecture would be as follows:
        //  stage_0:
        //      pipeline_0: ExchangeOperator->TaskOutputOperator
        //  stage_1:
        //      pipeline_0: ScanFilterAndProjectOperator->LocalExchangeSinkOperator
        //      pipeline_1: ScanFilterAndProjectOperator->LocalExchangeSinkOperator
        //      pipeline_2: LocalExchangeSource->HashBuilderOperator
        //      pipeline_3: LocalExchangeSource->LookupJoinOperator->TaskOutputOperator
        PlanFragment testPlanFragment = TaskTestUtils.createPlanFragment();

        // build stage_0 execution info
        int stageId0 = 0;
        int stageExecutionId0 = 1;
        List<OperatorStats> pipeline00 = ImmutableList.of(
                createOperatorStats(stageId0, stageExecutionId0, 0, 0, new PlanNodeId("101"),
                        ExchangeOperator.class,
                        5384L, 100L,
                        5040L, 100L,
                        5040L, 100L),
                createOperatorStats(stageId0, stageExecutionId0, 0, 1, new PlanNodeId("102"),
                        TaskOutputOperator.class,
                        0L, 0L,
                        5040L, 100L,
                        5040L, 100L));
        StageExecutionStats stageExecutionStats0 = createStageStats(stageId0, stageExecutionId0,
                5384L, 100L,
                5040L, 100L,
                5040L, 100L,
                pipeline00);
        StageExecutionInfo stageExecutionInfo0 = new StageExecutionInfo(
                StageExecutionState.FINISHED,
                stageExecutionStats0,
                ImmutableList.of(),
                Optional.empty());

        // build stage_1 execution info
        int stageId1 = 1;
        int stageExecutionId1 = 11;
        List<OperatorStats> pipeline10 = ImmutableList.of(
                createOperatorStats(stageId1, stageExecutionId1, 0, 0, new PlanNodeId("1001"),
                        ScanFilterAndProjectOperator.class,
                        6150L, 100L,
                        6150L, 100L,
                        4400L, 100L),
                createOperatorStats(stageId1, stageExecutionId1, 0, 1, new PlanNodeId("1002"),
                        LocalExchangeSinkOperator.class,
                        0L, 0L,
                        4400L, 100L,
                        4400L, 100L));

        List<OperatorStats> pipeline11 = ImmutableList.of(
                createOperatorStats(stageId1, stageExecutionId1, 1, 0, new PlanNodeId("1003"),
                        ScanFilterAndProjectOperator.class,
                        2470L, 50L,
                        2470L, 50L,
                        1670L, 50L),
                createOperatorStats(stageId1, stageExecutionId1, 1, 1, new PlanNodeId("1004"),
                        LocalExchangeSinkOperator.class,
                        0L, 0L,
                        1670L, 50L,
                        1670L, 50L));

        List<OperatorStats> pipeline12 = ImmutableList.of(
                createOperatorStats(stageId1, stageExecutionId1, 2, 0, new PlanNodeId("1005"),
                        LocalExchangeSource.class,
                        0L, 0L,
                        1670L, 50L,
                        1670L, 50L),
                createOperatorStats(stageId1, stageExecutionId1, 2, 1, new PlanNodeId("1006"),
                        HashBuilderOperator.class,
                        0L, 0L,
                        1670L, 50L,
                        1670L, 50L));

        List<OperatorStats> pipeline13 = ImmutableList.of(
                createOperatorStats(stageId1, stageExecutionId1, 3, 0, new PlanNodeId("1007"),
                        LocalExchangeSource.class,
                        0L, 0L,
                        4400L, 100L,
                        4400L, 100L),
                createOperatorStats(stageId1, stageExecutionId1, 3, 1, new PlanNodeId("1008"),
                        LookupJoinOperator.class,
                        0L, 0L,
                        4400L, 100L,
                        5040L, 100L),
                createOperatorStats(stageId1, stageExecutionId1, 3, 2, new PlanNodeId("1009"),
                        TaskOutputOperator.class,
                        0L, 0L,
                        5040L, 100L,
                        5040L, 100L));
        Builder<OperatorStats> stageOperatorStatsBuilder = ImmutableList.builder();
        StageExecutionStats stageExecutionStats1 = createStageStats(stageId1, stageExecutionId1,
                8620L, 150L,
                8620L, 150L,
                5040L, 100L,
                stageOperatorStatsBuilder.addAll(pipeline10)
                        .addAll(pipeline11)
                        .addAll(pipeline12)
                        .addAll(pipeline13)
                        .build());
        StageExecutionInfo stageExecutionInfo1 = new StageExecutionInfo(
                StageExecutionState.FINISHED,
                stageExecutionStats1,
                ImmutableList.of(),
                Optional.empty());

        // build whole stage info architecture
        StageInfo stageInfo1 = new StageInfo(StageId.valueOf("0.1"), URI.create("127.0.0.1"),
                Optional.of(testPlanFragment),
                stageExecutionInfo1, ImmutableList.of(), ImmutableList.of(), false);
        StageInfo stageInfo0 = new StageInfo(StageId.valueOf("0.0"), URI.create("127.0.0.1"),
                Optional.of(testPlanFragment),
                stageExecutionInfo0, ImmutableList.of(), ImmutableList.of(stageInfo1), false);

        // calculate query stats
        Optional<StageInfo> rootStage = Optional.of(stageInfo0);
        List<StageInfo> allStages = StageInfo.getAllStages(rootStage);
        QueryStats queryStats = QueryStats.create(new QueryStateTimer(new TestingTicker()), rootStage, allStages, 0,
                0L, 0L, 0L, 0L, 0L,
                new RuntimeStats(ImmutableMap.of(TEST_METRIC_NAME, RuntimeMetric.copyOf(TEST_RUNTIME_METRIC_1))));

        assertEquals(queryStats.getRawInputDataSize().toBytes(), 8620);
        assertEquals(queryStats.getRawInputPositions(), 150);
        assertEquals(queryStats.getShuffledDataSize().toBytes(), 5384);
        assertEquals(queryStats.getShuffledPositions(), 100);
        assertEquals(queryStats.getProcessedInputDataSize().toBytes(), 13660);
        assertEquals(queryStats.getProcessedInputPositions(), 250);
        assertEquals(queryStats.getOutputDataSize().toBytes(), 5040);
        assertEquals(queryStats.getOutputPositions(), 100);
    }

    @Test
    public void testJson()
    {
        JsonCodec<QueryStats> codec = JsonCodec.jsonCodec(QueryStats.class);

        String json = codec.toJson(EXPECTED);
        QueryStats actual = codec.fromJson(json);

        assertExpectedQueryStats(actual);
    }

    static void assertExpectedQueryStats(QueryStats actual)
    {
        assertEquals(actual.getCreateTimeInMillis(), new DateTime(1, UTC).getMillis());
        assertEquals(actual.getExecutionStartTimeInMillis(), new DateTime(2, UTC).getMillis());
        assertEquals(actual.getLastHeartbeatInMillis(), new DateTime(3, UTC).getMillis());
        assertEquals(actual.getEndTimeInMillis(), new DateTime(4, UTC).getMillis());

        assertEquals(actual.getElapsedTime(), new Duration(6, NANOSECONDS));
        assertEquals(actual.getQueuedTime(), new Duration(5, NANOSECONDS));
        assertEquals(actual.getResourceWaitingTime(), new Duration(31, NANOSECONDS));
        assertEquals(actual.getSemanticAnalyzingTime(), new Duration(15, NANOSECONDS));
        assertEquals(actual.getColumnAccessPermissionCheckingTime(), new Duration(15, NANOSECONDS));
        assertEquals(actual.getDispatchingTime(), new Duration(32, NANOSECONDS));
        assertEquals(actual.getExecutionTime(), new Duration(41, NANOSECONDS));
        assertEquals(actual.getAnalysisTime(), new Duration(7, NANOSECONDS));

        assertEquals(actual.getTotalPlanningTime(), new Duration(100, NANOSECONDS));
        assertEquals(actual.getFinishingTime(), new Duration(200, NANOSECONDS));

        assertEquals(actual.getTotalTasks(), 9);
        assertEquals(actual.getRunningTasks(), 10);
        assertEquals(actual.getPeakRunningTasks(), 11);
        assertEquals(actual.getCompletedTasks(), 11);

        assertEquals(actual.getTotalDrivers(), 12);
        assertEquals(actual.getQueuedDrivers(), 13);
        assertEquals(actual.getRunningDrivers(), 15);
        assertEquals(actual.getBlockedDrivers(), 30);
        assertEquals(actual.getCompletedDrivers(), 16);

        assertEquals(actual.getCumulativeUserMemory(), 17.0);
        assertEquals(actual.getUserMemoryReservation(), new DataSize(18, BYTE));
        assertEquals(actual.getTotalMemoryReservation(), new DataSize(19, BYTE));
        assertEquals(actual.getPeakUserMemoryReservation(), new DataSize(20, BYTE));
        assertEquals(actual.getPeakTotalMemoryReservation(), new DataSize(21, BYTE));
        assertEquals(actual.getPeakTaskUserMemory(), new DataSize(22, BYTE));
        assertEquals(actual.getPeakTaskTotalMemory(), new DataSize(23, BYTE));
        assertEquals(actual.getSpilledDataSize(), new DataSize(690, BYTE));

        assertEquals(actual.getTotalScheduledTime(), new Duration(20, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(21, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(23, NANOSECONDS));

        assertEquals(actual.getTotalAllocation(), new DataSize(123, BYTE));

        assertEquals(actual.getRawInputDataSize(), new DataSize(24, BYTE));
        assertEquals(actual.getRawInputPositions(), 25);

        assertEquals(actual.getProcessedInputDataSize(), new DataSize(26, BYTE));
        assertEquals(actual.getProcessedInputPositions(), 27);

        assertEquals(actual.getShuffledDataSize(), new DataSize(30, BYTE));
        assertEquals(actual.getShuffledPositions(), 29);

        assertEquals(actual.getOutputDataSize(), new DataSize(28, BYTE));
        assertEquals(actual.getOutputPositions(), 29);

        assertEquals(actual.getWrittenOutputPositions(), 30);

        assertEquals(actual.getWrittenOutputLogicalDataSize(), new DataSize(31, BYTE));
        assertEquals(actual.getWrittenOutputPhysicalDataSize(), new DataSize(32, BYTE));

        assertEquals(actual.getWrittenIntermediatePhysicalDataSize(), new DataSize(33, BYTE));

        assertEquals(actual.getStageGcStatistics().size(), 1);
        StageGcStatistics gcStatistics = actual.getStageGcStatistics().get(0);
        assertEquals(gcStatistics.getStageId(), 101);
        assertEquals(gcStatistics.getStageExecutionId(), 1001);
        assertEquals(gcStatistics.getTasks(), 102);
        assertEquals(gcStatistics.getFullGcTasks(), 103);
        assertEquals(gcStatistics.getMinFullGcSec(), 104);
        assertEquals(gcStatistics.getMaxFullGcSec(), 105);
        assertEquals(gcStatistics.getTotalFullGcSec(), 106);
        assertEquals(gcStatistics.getAverageFullGcSec(), 107);

        assertRuntimeMetricEquals(actual.getRuntimeStats().getMetric(TEST_METRIC_NAME), RuntimeMetric.merge(TEST_RUNTIME_METRIC_1, TEST_RUNTIME_METRIC_2));
    }

    private static void assertRuntimeMetricEquals(RuntimeMetric m1, RuntimeMetric m2)
    {
        assertEquals(m1.getName(), m2.getName());
        assertEquals(m1.getUnit(), m2.getUnit());
        assertEquals(m1.getSum(), m2.getSum());
        assertEquals(m1.getCount(), m2.getCount());
        assertEquals(m1.getMax(), m2.getMax());
        assertEquals(m1.getMin(), m2.getMin());
    }

    private static OperatorStats createOperatorStats(int stageId, int stageExecutionId, int pipelineId,
            int operatorId, PlanNodeId planNodeId, Class operatorCls,
            long rawInputDataSize, long rawInputPositions,
            long inputDataSize, long inputPositions,
            long outputDataSize, long outputPositions)
    {
        return new OperatorStats(
                stageId,
                stageExecutionId,
                pipelineId,
                operatorId,
                planNodeId,
                operatorCls.getSimpleName(),
                0L,
                0L,
                new Duration(0, NANOSECONDS),
                new Duration(0, NANOSECONDS),
                0,
                0L,
                new Duration(0, NANOSECONDS),
                new Duration(0, NANOSECONDS),
                0,
                rawInputDataSize,
                rawInputPositions,
                inputDataSize,
                inputPositions,
                0.0,
                0L,
                new Duration(0, NANOSECONDS),
                new Duration(0, NANOSECONDS),
                0,
                outputDataSize,
                outputPositions,
                0L,
                new Duration(0, NANOSECONDS),
                new Duration(0, NANOSECONDS),
                0L,
                new Duration(0, NANOSECONDS),
                new Duration(0, NANOSECONDS),
                0,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                Optional.empty(),
                null,
                new RuntimeStats(ImmutableMap.of(TEST_METRIC_NAME, RuntimeMetric.copyOf(TEST_RUNTIME_METRIC_1))),
                DynamicFilterStats.copyOf(TEST_DYNAMIC_FILTER_STATS_1),
                0,
                0,
                0,
                0);
    }

    private static StageExecutionStats createStageStats(int stageId, int stageExecutionId, long rawInputDataSize, long rawInputPositions,
            long inputDataSize, long inputPositions,
            long outputDataSize, long outputPositions,
            List<OperatorStats> operatorSummaries)
    {
        return new StageExecutionStats(
                0L,

                new Distribution(0).snapshot(),

                1,
                0,
                1,

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
                0,
                0,
                0,

                0.0,
                0.0,
                0L,
                0L,
                0L,
                0L,

                new Duration(0, NANOSECONDS),
                new Duration(0, NANOSECONDS),
                new Duration(0, NANOSECONDS),
                new Duration(0, NANOSECONDS),
                false,
                ImmutableSet.of(),

                0L,

                rawInputDataSize,
                rawInputPositions,

                inputDataSize,
                inputPositions,

                0L,
                outputDataSize,
                outputPositions,

                0L,

                new StageGcStatistics(
                        stageId,
                        stageExecutionId,
                        102,
                        103,
                        104,
                        105,
                        106,
                        107),

                operatorSummaries,
                new RuntimeStats());
    }
}
