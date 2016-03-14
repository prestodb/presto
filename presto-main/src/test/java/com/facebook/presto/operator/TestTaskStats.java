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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.TestPipelineStats.assertExpectedPipelineStats;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestTaskStats
{
    public static final TaskStats EXPECTED = new TaskStats(
            new DateTime(1),
            new DateTime(2),
            new DateTime(100),
            new DateTime(101),
            new DateTime(3),
            new Duration(4, NANOSECONDS),
            new Duration(5, NANOSECONDS),

            6,
            7,
            5,
            8,
            6,
            10,

            11.0,
            new DataSize(12, BYTE),
            new DataSize(13, BYTE),
            new Duration(14, NANOSECONDS),
            new Duration(15, NANOSECONDS),
            new Duration(16, NANOSECONDS),
            new Duration(17, NANOSECONDS),
            false,
            ImmutableSet.of(),

            new DataSize(18, BYTE),
            19,

            new DataSize(20, BYTE),
            21,

            new DataSize(22, BYTE),
            23,

            ImmutableList.of(TestPipelineStats.EXPECTED));

    @Test
    public void testJson()
    {
        JsonCodec<TaskStats> codec = JsonCodec.jsonCodec(TaskStats.class);

        String json = codec.toJson(EXPECTED);
        TaskStats actual = codec.fromJson(json);

        assertExpectedTaskStats(actual);
    }

    public static void assertExpectedTaskStats(TaskStats actual)
    {
        assertEquals(actual.getCreateTime(), new DateTime(1, UTC));
        assertEquals(actual.getFirstStartTime(), new DateTime(2, UTC));
        assertEquals(actual.getLastStartTime(), new DateTime(100, UTC));
        assertEquals(actual.getLastEndTime(), new DateTime(101, UTC));
        assertEquals(actual.getEndTime(), new DateTime(3, UTC));
        assertEquals(actual.getElapsedTime(), new Duration(4, NANOSECONDS));
        assertEquals(actual.getQueuedTime(), new Duration(5, NANOSECONDS));

        assertEquals(actual.getTotalDrivers(), 6);
        assertEquals(actual.getQueuedDrivers(), 7);
        assertEquals(actual.getQueuedPartitionedDrivers(), 5);
        assertEquals(actual.getRunningDrivers(), 8);
        assertEquals(actual.getRunningPartitionedDrivers(), 6);
        assertEquals(actual.getCompletedDrivers(), 10);

        assertEquals(actual.getCumulativeMemory(), 11.0);
        assertEquals(actual.getMemoryReservation(), new DataSize(12, BYTE));
        assertEquals(actual.getSystemMemoryReservation(), new DataSize(13, BYTE));

        assertEquals(actual.getTotalScheduledTime(), new Duration(14, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(15, NANOSECONDS));
        assertEquals(actual.getTotalUserTime(), new Duration(16, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(17, NANOSECONDS));

        assertEquals(actual.getRawInputDataSize(), new DataSize(18, BYTE));
        assertEquals(actual.getRawInputPositions(), 19);

        assertEquals(actual.getProcessedInputDataSize(), new DataSize(20, BYTE));
        assertEquals(actual.getProcessedInputPositions(), 21);

        assertEquals(actual.getOutputDataSize(), new DataSize(22, BYTE));
        assertEquals(actual.getOutputPositions(), 23);

        assertEquals(actual.getPipelines().size(), 1);
        assertExpectedPipelineStats(actual.getPipelines().get(0));
    }
}
