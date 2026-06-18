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
import com.facebook.airlift.stats.Distribution.DistributionSnapshot;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;

public class TestStageExecutionStats
{
    private static final StageExecutionStats EXPECTED = new StageExecutionStats(
            0L,

            getTestDistribution(1),

            4,
            5,
            6,

            69,
            31,

            7,
            8,
            10,
            26,
            11,
            7,
            8,
            10,
            11,
            7,
            8,
            10,
            11,

            12.0,
            27.0,
            13L,
            14L,
            15L,
            42L,

            new Duration(15, NANOSECONDS),
            new Duration(16, NANOSECONDS),
            new Duration(17, NANOSECONDS),
            new Duration(18, NANOSECONDS),
            false,
            ImmutableSet.of(),

            123L,

            19L,
            20,

            21L,
            22,

            23L,
            24L,
            25,

            26L,

            new StageGcStatistics(
                    101,
                    1001,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107),

            ImmutableList.of(),
            new RuntimeStats());

    @Test
    public void testJson()
    {
        JsonCodec<StageExecutionStats> codec = JsonCodec.jsonCodec(StageExecutionStats.class);

        String json = codec.toJson(EXPECTED);
        StageExecutionStats actual = codec.fromJson(json);

        assertExpectedStageStats(actual);
    }

    private static void assertExpectedStageStats(StageExecutionStats actual)
    {
        assertEquals(actual.getSchedulingCompleteInMillis(), 0);

        assertEquals(actual.getGetSplitDistribution().getCount(), 1.0);

        assertEquals(actual.getTotalTasks(), 4);
        assertEquals(actual.getRunningTasks(), 5);
        assertEquals(actual.getCompletedTasks(), 6);

        assertEquals(actual.getTotalLifespans(), 69);
        assertEquals(actual.getCompletedLifespans(), 31);

        assertEquals(actual.getTotalDrivers(), 7);
        assertEquals(actual.getQueuedDrivers(), 8);
        assertEquals(actual.getRunningDrivers(), 10);
        assertEquals(actual.getBlockedDrivers(), 26);
        assertEquals(actual.getCompletedDrivers(), 11);

        assertEquals(actual.getCumulativeUserMemory(), 12.0);
        assertEquals(actual.getUserMemoryReservationInBytes(), 13L);
        assertEquals(actual.getTotalMemoryReservationInBytes(), 14L);
        assertEquals(actual.getPeakUserMemoryReservationInBytes(), 15L);
        assertEquals(actual.getPeakNodeTotalMemoryReservationInBytes(), 42L);

        assertEquals(actual.getTotalScheduledTime(), new Duration(15, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(16, NANOSECONDS));
        assertEquals(actual.getRetriedCpuTime(), new Duration(17, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(18, NANOSECONDS));

        assertEquals(actual.getTotalAllocationInBytes(), 123L);

        assertEquals(actual.getRawInputDataSizeInBytes(), 19L);
        assertEquals(actual.getRawInputPositions(), 20);

        assertEquals(actual.getProcessedInputDataSizeInBytes(), 21L);
        assertEquals(actual.getProcessedInputPositions(), 22);

        assertEquals(actual.getBufferedDataSizeInBytes(), 23L);
        assertEquals(actual.getOutputDataSizeInBytes(), 24L);
        assertEquals(actual.getOutputPositions(), 25);

        assertEquals(actual.getPhysicalWrittenDataSizeInBytes(), 26L);

        assertEquals(actual.getGcInfo().getStageId(), 101);
        assertEquals(actual.getGcInfo().getStageExecutionId(), 1001);
        assertEquals(actual.getGcInfo().getTasks(), 102);
        assertEquals(actual.getGcInfo().getFullGcTasks(), 103);
        assertEquals(actual.getGcInfo().getMinFullGcSec(), 104);
        assertEquals(actual.getGcInfo().getMaxFullGcSec(), 105);
        assertEquals(actual.getGcInfo().getTotalFullGcSec(), 106);
        assertEquals(actual.getGcInfo().getAverageFullGcSec(), 107);
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
