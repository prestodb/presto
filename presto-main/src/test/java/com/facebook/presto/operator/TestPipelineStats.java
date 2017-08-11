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
import io.airlift.stats.Distribution;
import io.airlift.stats.Distribution.DistributionSnapshot;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.TestDriverStats.assertExpectedDriverStats;
import static com.facebook.presto.operator.TestOperatorStats.assertExpectedOperatorStats;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
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

            new DataSize(5, BYTE),
            new DataSize(6, BYTE),
            new DataSize(7, BYTE),

            getTestDistribution(8),
            getTestDistribution(9),

            new Duration(10, NANOSECONDS),
            new Duration(11, NANOSECONDS),
            new Duration(12, NANOSECONDS),
            new Duration(13, NANOSECONDS),
            false,
            ImmutableSet.of(),

            new DataSize(14, BYTE),
            15,

            new DataSize(16, BYTE),
            17,

            new DataSize(18, BYTE),
            19,

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

        assertEquals(actual.getMemoryReservation(), new DataSize(5, BYTE));
        assertEquals(actual.getRevocableMemoryReservation(), new DataSize(6, BYTE));
        assertEquals(actual.getSystemMemoryReservation(), new DataSize(7, BYTE));

        assertEquals(actual.getQueuedTime().getCount(), 8.0);
        assertEquals(actual.getElapsedTime().getCount(), 9.0);

        assertEquals(actual.getTotalScheduledTime(), new Duration(10, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(11, NANOSECONDS));
        assertEquals(actual.getTotalUserTime(), new Duration(12, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(13, NANOSECONDS));

        assertEquals(actual.getRawInputDataSize(), new DataSize(14, BYTE));
        assertEquals(actual.getRawInputPositions(), 15);

        assertEquals(actual.getProcessedInputDataSize(), new DataSize(16, BYTE));
        assertEquals(actual.getProcessedInputPositions(), 17);

        assertEquals(actual.getOutputDataSize(), new DataSize(18, BYTE));
        assertEquals(actual.getOutputPositions(), 19);

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
