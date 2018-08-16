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

import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.TableWriterOperator;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestQueryStats
{
    public static final List<OperatorStats> operatorSummaries = ImmutableList.of(
            new OperatorStats(
                    1,
                    1,
                    new PlanNodeId("1"),
                    TableWriterOperator.class.getSimpleName(),
                    0L,
                    0L,
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    succinctBytes(500L),
                    100L,
                    1.0,
                    0L,
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    succinctBytes(1L),
                    1L,
                    succinctBytes(1L),
                    new Duration(1, NANOSECONDS),
                    0L,
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    Optional.empty(),
                    null),
            new OperatorStats(
                    1,
                    1,
                    new PlanNodeId("2"),
                    FilterAndProjectOperator.class.getSimpleName(),
                    0L,
                    0L,
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    succinctBytes(1L),
                    1L,
                    1.0,
                    0L,
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    succinctBytes(500L),
                    100L,
                    succinctBytes(1L),
                    new Duration(1, NANOSECONDS),
                    0L,
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    Optional.empty(),
                    null),
            new OperatorStats(
                    1,
                    1,
                    new PlanNodeId("3"),
                    TableWriterOperator.class.getSimpleName(),
                    0L,
                    0L,
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    succinctBytes(1000L),
                    300L,
                    1.0,
                    0L,
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    succinctBytes(1L),
                    1L,
                    succinctBytes(1L),
                    new Duration(1, NANOSECONDS),
                    0L,
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    new Duration(1, NANOSECONDS),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    succinctBytes(1L),
                    Optional.empty(),
                    null));

    public static final QueryStats EXPECTED = new QueryStats(
            new DateTime(1),
            new DateTime(2),
            new DateTime(3),
            new DateTime(4),
            new Duration(6, NANOSECONDS),
            new Duration(5, NANOSECONDS),
            new Duration(31, NANOSECONDS),
            new Duration(7, NANOSECONDS),
            new Duration(8, NANOSECONDS),

            new Duration(100, NANOSECONDS),
            new Duration(200, NANOSECONDS),

            9,
            10,
            11,

            12,
            13,
            15,
            30,
            16,

            17.0,
            new DataSize(18, BYTE),
            new DataSize(19, BYTE),
            new DataSize(20, BYTE),
            new DataSize(21, BYTE),
            new DataSize(22, BYTE),

            true,
            new Duration(20, NANOSECONDS),
            new Duration(21, NANOSECONDS),
            new Duration(22, NANOSECONDS),
            new Duration(23, NANOSECONDS),
            false,
            ImmutableSet.of(),

            new DataSize(24, BYTE),
            25,

            new DataSize(26, BYTE),
            27,

            new DataSize(28, BYTE),
            29,

            new DataSize(30, BYTE),

            ImmutableList.of(new StageGcStatistics(
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107)),

            operatorSummaries);

    @Test
    public void testJson()
    {
        JsonCodec<QueryStats> codec = JsonCodec.jsonCodec(QueryStats.class);

        String json = codec.toJson(EXPECTED);
        QueryStats actual = codec.fromJson(json);

        assertExpectedQueryStats(actual);
    }

    public static void assertExpectedQueryStats(QueryStats actual)
    {
        assertEquals(actual.getCreateTime(), new DateTime(1, UTC));
        assertEquals(actual.getExecutionStartTime(), new DateTime(2, UTC));
        assertEquals(actual.getLastHeartbeat(), new DateTime(3, UTC));
        assertEquals(actual.getEndTime(), new DateTime(4, UTC));

        assertEquals(actual.getElapsedTime(), new Duration(6, NANOSECONDS));
        assertEquals(actual.getQueuedTime(), new Duration(5, NANOSECONDS));
        assertEquals(actual.getExecutionTime(), new Duration(1, NANOSECONDS));
        assertEquals(actual.getAnalysisTime(), new Duration(7, NANOSECONDS));
        assertEquals(actual.getDistributedPlanningTime(), new Duration(8, NANOSECONDS));

        assertEquals(actual.getTotalPlanningTime(), new Duration(100, NANOSECONDS));
        assertEquals(actual.getFinishingTime(), new Duration(200, NANOSECONDS));

        assertEquals(actual.getTotalTasks(), 9);
        assertEquals(actual.getRunningTasks(), 10);
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

        assertEquals(actual.getTotalScheduledTime(), new Duration(20, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(21, NANOSECONDS));
        assertEquals(actual.getTotalUserTime(), new Duration(22, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(23, NANOSECONDS));

        assertEquals(actual.getRawInputDataSize(), new DataSize(24, BYTE));
        assertEquals(actual.getRawInputPositions(), 25);

        assertEquals(actual.getProcessedInputDataSize(), new DataSize(26, BYTE));
        assertEquals(actual.getProcessedInputPositions(), 27);

        assertEquals(actual.getOutputDataSize(), new DataSize(28, BYTE));
        assertEquals(actual.getOutputPositions(), 29);

        assertEquals(actual.getPhysicalWrittenDataSize(), new DataSize(30, BYTE));

        assertEquals(actual.getStageGcStatistics().size(), 1);
        StageGcStatistics gcStatistics = actual.getStageGcStatistics().get(0);
        assertEquals(gcStatistics.getStageId(), 101);
        assertEquals(gcStatistics.getTasks(), 102);
        assertEquals(gcStatistics.getFullGcTasks(), 103);
        assertEquals(gcStatistics.getMinFullGcSec(), 104);
        assertEquals(gcStatistics.getMaxFullGcSec(), 105);
        assertEquals(gcStatistics.getTotalFullGcSec(), 106);
        assertEquals(gcStatistics.getAverageFullGcSec(), 107);

        assertEquals(400L, actual.getWrittenPositions());
        assertEquals(1500L, actual.getLogicalWrittenDataSize().toBytes());
    }
}
