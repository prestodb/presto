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
import com.facebook.presto.common.RuntimeStats;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.TestPipelineStats.assertExpectedPipelineStats;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestTaskStats
{
    private static final TaskStats EXPECTED = new TaskStats(
            1L,
            2L,
            100L,
            101L,
            3L,
            4,
            5,

            6,
            7,
            5,
            28L,
            8,
            6,
            29L,
            24,
            10,

            11.0,
            43.0,
            12,
            13,
            14,
            26,
            27,
            42,
            15,
            16,
            18,
            false,
            ImmutableSet.of(),

            123,

            19,
            20,

            21,
            22,

            23,
            24,

            25,

            26,
            27,

            ImmutableList.of(TestPipelineStats.EXPECTED),
            new RuntimeStats());

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
        assertEquals(actual.getCreateTimeInMillis(), new DateTime(1, UTC).getMillis());
        assertEquals(actual.getFirstStartTimeInMillis(), new DateTime(2, UTC).getMillis());
        assertEquals(actual.getLastStartTimeInMillis(), new DateTime(100, UTC).getMillis());
        assertEquals(actual.getLastEndTimeInMillis(), new DateTime(101, UTC).getMillis());
        assertEquals(actual.getEndTimeInMillis(), new DateTime(3, UTC).getMillis());
        assertEquals(actual.getElapsedTimeInNanos(), 4);
        assertEquals(actual.getQueuedTimeInNanos(), 5);

        assertEquals(actual.getTotalDrivers(), 6);
        assertEquals(actual.getQueuedDrivers(), 7);
        assertEquals(actual.getQueuedPartitionedDrivers(), 5);
        assertEquals(actual.getQueuedPartitionedSplitsWeight(), 28L);
        assertEquals(actual.getRunningDrivers(), 8);
        assertEquals(actual.getRunningPartitionedDrivers(), 6);
        assertEquals(actual.getRunningPartitionedSplitsWeight(), 29L);
        assertEquals(actual.getBlockedDrivers(), 24);
        assertEquals(actual.getCompletedDrivers(), 10);

        assertEquals(actual.getCumulativeUserMemory(), 11.0);
        assertEquals(actual.getUserMemoryReservationInBytes(), 12);
        assertEquals(actual.getRevocableMemoryReservationInBytes(), 13);
        assertEquals(actual.getSystemMemoryReservationInBytes(), 14);
        assertEquals(actual.getPeakTotalMemoryInBytes(), 26);
        assertEquals(actual.getPeakNodeTotalMemoryInBytes(), 42);

        assertEquals(actual.getTotalScheduledTimeInNanos(), 15);
        assertEquals(actual.getTotalCpuTimeInNanos(), 16);
        assertEquals(actual.getTotalBlockedTimeInNanos(), 18);
        assertEquals(actual.getTotalAllocationInBytes(), 123);

        assertEquals(actual.getRawInputDataSizeInBytes(), 19);
        assertEquals(actual.getRawInputPositions(), 20);

        assertEquals(actual.getProcessedInputDataSizeInBytes(), 21);
        assertEquals(actual.getProcessedInputPositions(), 22);

        assertEquals(actual.getOutputDataSizeInBytes(), 23);
        assertEquals(actual.getOutputPositions(), 24);

        assertEquals(actual.getPhysicalWrittenDataSizeInBytes(), 25);

        assertEquals(actual.getPipelines().size(), 1);
        assertExpectedPipelineStats(actual.getPipelines().get(0));
    }
}
