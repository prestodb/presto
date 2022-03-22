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
import com.facebook.airlift.stats.Distribution;
import com.facebook.airlift.stats.Distribution.DistributionSnapshot;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.TestDriverStats.assertExpectedDriverStats;
import static com.facebook.presto.operator.TestOperatorStats.assertExpectedOperatorStats;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestPipelineStats
{
    public static final PipelineStats EXPECTED = new PipelineStats(
            0,

            new DateTime(100),
            new DateTime(101),
            new DateTime(102),

            true,
            false,

            1,
            2,
            1,
            3,
            2,
            19,
            4,

            5,
            6,
            7,

            getTestDistribution(8),
            getTestDistribution(9),

            10,
            11,
            13,
            false,
            ImmutableSet.of(),

            123,

            14,
            15,

            16,
            17,

            18,
            19,

            20,

            ImmutableList.of(TestOperatorStats.EXPECTED),
            ImmutableList.of(TestDriverStats.EXPECTED));

    @Test
    public void testJson()
    {
        JsonCodec<PipelineStats> codec = JsonCodec.jsonCodec(PipelineStats.class);

        String json = codec.toJson(EXPECTED);
        PipelineStats actual = codec.fromJson(json);

        assertExpectedPipelineStats(actual);
    }

    public static void assertExpectedPipelineStats(PipelineStats actual)
    {
        assertEquals(actual.getFirstStartTime(), new DateTime(100, UTC));
        assertEquals(actual.getLastStartTime(), new DateTime(101, UTC));
        assertEquals(actual.getLastEndTime(), new DateTime(102, UTC));
        assertEquals(actual.isInputPipeline(), true);
        assertEquals(actual.isOutputPipeline(), false);

        assertEquals(actual.getTotalDrivers(), 1);
        assertEquals(actual.getQueuedDrivers(), 2);
        assertEquals(actual.getQueuedPartitionedDrivers(), 1);
        assertEquals(actual.getRunningDrivers(), 3);
        assertEquals(actual.getRunningPartitionedDrivers(), 2);
        assertEquals(actual.getBlockedDrivers(), 19);
        assertEquals(actual.getCompletedDrivers(), 4);

        assertEquals(actual.getUserMemoryReservationInBytes(), 5);
        assertEquals(actual.getRevocableMemoryReservationInBytes(), 6);
        assertEquals(actual.getSystemMemoryReservationInBytes(), 7);

        assertEquals(actual.getQueuedTime().getCount(), 8.0);
        assertEquals(actual.getElapsedTime().getCount(), 9.0);

        assertEquals(actual.getTotalScheduledTimeInNanos(), 10);
        assertEquals(actual.getTotalCpuTimeInNanos(), 11);
        assertEquals(actual.getTotalBlockedTimeInNanos(), 13);

        assertEquals(actual.getTotalAllocationInBytes(), 123);

        assertEquals(actual.getRawInputDataSizeInBytes(), 14);
        assertEquals(actual.getRawInputPositions(), 15);

        assertEquals(actual.getProcessedInputDataSizeInBytes(), 16);
        assertEquals(actual.getProcessedInputPositions(), 17);

        assertEquals(actual.getOutputDataSizeInBytes(), 18);
        assertEquals(actual.getOutputPositions(), 19);

        assertEquals(actual.getPhysicalWrittenDataSizeInBytes(), 20);

        assertEquals(actual.getOperatorSummaries().size(), 1);
        assertExpectedOperatorStats(actual.getOperatorSummaries().get(0));

        assertEquals(actual.getDrivers().size(), 1);
        assertExpectedDriverStats(actual.getDrivers().get(0));
    }

    private static DistributionSnapshot getTestDistribution(int count)
    {
        Distribution distribution = new Distribution();
        for (int i = 0; i < count; i++) {
            distribution.add(i);
        }
        return distribution.snapshot();
    }
}
