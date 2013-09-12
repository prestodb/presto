package com.facebook.presto.noperator;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.stats.Distribution;
import io.airlift.stats.Distribution.DistributionSnapshot;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import static com.facebook.presto.noperator.TestDriverStats.assertExpectedDriverStats;
import static com.facebook.presto.noperator.TestOperatorStats.assertExpectedOperatorStats;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;

public class TestPipelineStats
{
    public static final PipelineStats EXPECTED = new PipelineStats(
            true,
            false,

            1,
            2,
            3,
            4,

            new DataSize(5, BYTE),

            getTestDistribution(6),
            getTestDistribution(7),

            new Duration(8, NANOSECONDS),
            new Duration(9, NANOSECONDS),
            new Duration(10, NANOSECONDS),
            new Duration(11, NANOSECONDS),

            new DataSize(12, BYTE),
            13,
            
            new DataSize(14, BYTE),
            15,

            new DataSize(16, BYTE),
            17,

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
        assertEquals(actual.isInputPipeline(), true);
        assertEquals(actual.isOutputPipeline(), false);

        assertEquals(actual.getTotalDrivers(), 1);
        assertEquals(actual.getQueuedDrivers(), 2);
        assertEquals(actual.getStartedDrivers(), 3);
        assertEquals(actual.getCompletedDrivers(), 4);

        assertEquals(actual.getMemoryReservation(), new DataSize(5, BYTE));

        assertEquals(actual.getQueuedTime().getCount(), 6.0);
        assertEquals(actual.getElapsedTime().getCount(), 7.0);

        assertEquals(actual.getTotalScheduledTime(), new Duration(8, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(9, NANOSECONDS));
        assertEquals(actual.getTotalUserTime(), new Duration(10, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(11, NANOSECONDS));

        assertEquals(actual.getRawInputDataSize(), new DataSize(12, BYTE));
        assertEquals(actual.getRawInputPositions(), 13);

        assertEquals(actual.getProcessedInputDataSize(), new DataSize(14, BYTE));
        assertEquals(actual.getProcessedInputPositions(), 15);

        assertEquals(actual.getOutputDataSize(), new DataSize(16, BYTE));
        assertEquals(actual.getOutputPositions(), 17);

        assertEquals(actual.getOperatorSummaries().size(), 1);
        assertExpectedOperatorStats(actual.getOperatorSummaries().get(0));

        assertEquals(actual.getRunningDrivers().size(), 1);
        assertExpectedDriverStats(actual.getRunningDrivers().get(0));
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
