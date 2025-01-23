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
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;

public class TestStageExecutionStats
{
    private static final StageExecutionStats EXPECTED = new StageExecutionStats(
            new DateTime(0),

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

            12.0,
            27.0,
            new DataSize(13, BYTE),
            new DataSize(14, BYTE),
            new DataSize(15, BYTE),
            new DataSize(42, BYTE),

            new Duration(15, NANOSECONDS),
            new Duration(16, NANOSECONDS),
            new Duration(17, NANOSECONDS),
            new Duration(18, NANOSECONDS),
            false,
            ImmutableSet.of(),

            new DataSize(123, BYTE),

            new DataSize(19, BYTE),
            20,

            new DataSize(21, BYTE),
            22,

            new DataSize(23, BYTE),
            new DataSize(24, BYTE),
            25,

            new DataSize(26, BYTE),

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
        assertEquals(actual.getSchedulingComplete().getMillis(), 0);

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
        assertEquals(actual.getUserMemoryReservation(), new DataSize(13, BYTE));
        assertEquals(actual.getTotalMemoryReservation(), new DataSize(14, BYTE));
        assertEquals(actual.getPeakUserMemoryReservation(), new DataSize(15, BYTE));
        assertEquals(actual.getPeakNodeTotalMemoryReservation(), new DataSize(42, BYTE));

        assertEquals(actual.getTotalScheduledTime(), new Duration(15, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(16, NANOSECONDS));
        assertEquals(actual.getRetriedCpuTime(), new Duration(17, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(18, NANOSECONDS));

        assertEquals(actual.getTotalAllocation(), new DataSize(123, BYTE));

        assertEquals(actual.getRawInputDataSize(), new DataSize(19, BYTE));
        assertEquals(actual.getRawInputPositions(), 20);

        assertEquals(actual.getProcessedInputDataSize(), new DataSize(21, BYTE));
        assertEquals(actual.getProcessedInputPositions(), 22);

        assertEquals(actual.getBufferedDataSize(), new DataSize(23, BYTE));
        assertEquals(actual.getOutputDataSize(), new DataSize(24, BYTE));
        assertEquals(actual.getOutputPositions(), 25);

        assertEquals(actual.getPhysicalWrittenDataSize(), new DataSize(26, BYTE));

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
