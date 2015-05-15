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

import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestQueryStats
{
    public static final QueryStats EXPECTED = new QueryStats(
            new DateTime(1),
            new DateTime(2),
            new DateTime(3),
            new DateTime(4),
            new Duration(5, NANOSECONDS),
            new Duration(6, NANOSECONDS),
            new Duration(7, NANOSECONDS),
            new Duration(8, NANOSECONDS),

            new Duration(100, NANOSECONDS),

            9,
            10,
            11,

            12,
            13,
            15,
            16,

            new DataSize(17, BYTE),

            new Duration(18, NANOSECONDS),
            new Duration(19, NANOSECONDS),
            new Duration(20, NANOSECONDS),
            new Duration(21, NANOSECONDS),
            false,
            ImmutableSet.of(),

            new DataSize(22, BYTE),
            23,

            new DataSize(24, BYTE),
            25,

            new DataSize(26, BYTE),
            27);

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

        assertEquals(actual.getElapsedTime(), new Duration(5, NANOSECONDS));
        assertEquals(actual.getQueuedTime(), new Duration(6, NANOSECONDS));
        assertEquals(actual.getAnalysisTime(), new Duration(7, NANOSECONDS));
        assertEquals(actual.getDistributedPlanningTime(), new Duration(8, NANOSECONDS));

        assertEquals(actual.getTotalPlanningTime(), new Duration(100, NANOSECONDS));

        assertEquals(actual.getTotalTasks(), 9);
        assertEquals(actual.getRunningTasks(), 10);
        assertEquals(actual.getCompletedTasks(), 11);

        assertEquals(actual.getTotalDrivers(), 12);
        assertEquals(actual.getQueuedDrivers(), 13);
        assertEquals(actual.getRunningDrivers(), 15);
        assertEquals(actual.getCompletedDrivers(), 16);

        assertEquals(actual.getTotalMemoryReservation(), new DataSize(17, BYTE));

        assertEquals(actual.getTotalScheduledTime(), new Duration(18, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(19, NANOSECONDS));
        assertEquals(actual.getTotalUserTime(), new Duration(20, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(21, NANOSECONDS));

        assertEquals(actual.getRawInputDataSize(), new DataSize(22, BYTE));
        assertEquals(actual.getRawInputPositions(), 23);

        assertEquals(actual.getProcessedInputDataSize(), new DataSize(24, BYTE));
        assertEquals(actual.getProcessedInputPositions(), 25);

        assertEquals(actual.getOutputDataSize(), new DataSize(26, BYTE));
        assertEquals(actual.getOutputPositions(), 27);
    }
}
