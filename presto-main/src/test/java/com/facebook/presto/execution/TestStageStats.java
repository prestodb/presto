package com.facebook.presto.execution;

import io.airlift.json.JsonCodec;
import io.airlift.stats.Distribution;
import io.airlift.stats.Distribution.DistributionSnapshot;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;

public class TestStageStats
{
    public static final StageStats EXPECTED = new StageStats(
            getTestDistribution(1),
            getTestDistribution(2),
            getTestDistribution(3),

            4,
            5,
            6,

            7,
            8,
            9,
            10,
            11,

            new DataSize(12, BYTE),

            new Duration(13, NANOSECONDS),
            new Duration(14, NANOSECONDS),
            new Duration(15, NANOSECONDS),
            new Duration(16, NANOSECONDS),

            new DataSize(17, BYTE),
            18,
            new DataSize(19, BYTE),
            20);

    @Test
    public void testJson()
    {
        JsonCodec<StageStats> codec = JsonCodec.jsonCodec(StageStats.class);

        String json = codec.toJson(EXPECTED);
        StageStats actual = codec.fromJson(json);

        assertExpectedStageStats(actual);
    }

    public static void assertExpectedStageStats(StageStats actual)
    {
        assertEquals(actual.getGetSplitDistribution().getCount(), 1.0);
        assertEquals(actual.getScheduleTaskDistribution().getCount(), 2.0);
        assertEquals(actual.getAddSplitDistribution().getCount(), 3.0);

        assertEquals(actual.getTotalTasks(), 4);
        assertEquals(actual.getRunningTasks(), 5);
        assertEquals(actual.getCompletedTasks(), 6);

        assertEquals(actual.getTotalDrivers(), 7);
        assertEquals(actual.getQueuedDrivers(), 8);
        assertEquals(actual.getRunningDrivers(), 9);
        assertEquals(actual.getStartedDrivers(), 10);
        assertEquals(actual.getCompletedDrivers(), 11);

        assertEquals(actual.getTotalMemoryReservation(), new DataSize(12, BYTE));

        assertEquals(actual.getTotalScheduledTime(), new Duration(13, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(14, NANOSECONDS));
        assertEquals(actual.getTotalUserTime(), new Duration(15, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(16, NANOSECONDS));

        assertEquals(actual.getInputDataSize(), new DataSize(17, BYTE));
        assertEquals(actual.getInputPositions(), 18);

        assertEquals(actual.getOutputDataSize(), new DataSize(19, BYTE));
        assertEquals(actual.getOutputPositions(), 20);
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
