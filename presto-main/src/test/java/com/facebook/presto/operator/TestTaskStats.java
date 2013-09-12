package com.facebook.presto.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.TestPipelineStats.assertExpectedPipelineStats;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;

public class TestTaskStats
{
    public static final TaskStats EXPECTED = new TaskStats(
            new DateTime(1),
            new DateTime(2),
            new DateTime(3),
            new Duration(4, NANOSECONDS),
            new Duration(5, NANOSECONDS),

            6,
            7,
            8,
            9,
            10,

            new DataSize(11, BYTE),
            new Duration(12, NANOSECONDS),
            new Duration(13, NANOSECONDS),
            new Duration(14, NANOSECONDS),
            new Duration(15, NANOSECONDS),

            new DataSize(16, BYTE),
            17,

            new DataSize(18, BYTE),
            19,

            new DataSize(20, BYTE),
            21,

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
        assertEquals(actual.getCreateTime(), new DateTime(1));
        assertEquals(actual.getStartTime(), new DateTime(2));
        assertEquals(actual.getEndTime(), new DateTime(3));
        assertEquals(actual.getElapsedTime(), new Duration(4, NANOSECONDS));
        assertEquals(actual.getQueuedTime(), new Duration(5, NANOSECONDS));

        assertEquals(actual.getTotalDrivers(), 6);
        assertEquals(actual.getQueuedDrivers(), 7);
        assertEquals(actual.getStartedDrivers(), 8);
        assertEquals(actual.getRunningDrivers(), 9);
        assertEquals(actual.getCompletedDrivers(), 10);

        assertEquals(actual.getMemoryReservation(), new DataSize(11, BYTE));

        assertEquals(actual.getTotalScheduledTime(), new Duration(12, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(13, NANOSECONDS));
        assertEquals(actual.getTotalUserTime(), new Duration(14, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(15, NANOSECONDS));

        assertEquals(actual.getRawInputDataSize(), new DataSize(16, BYTE));
        assertEquals(actual.getRawInputPositions(), 17);

        assertEquals(actual.getProcessedInputDataSize(), new DataSize(18, BYTE));
        assertEquals(actual.getProcessedInputPositions(), 19);

        assertEquals(actual.getOutputDataSize(), new DataSize(20, BYTE));
        assertEquals(actual.getOutputPositions(), 21);

        assertEquals(actual.getPipelines().size(), 1);
        assertExpectedPipelineStats(actual.getPipelines().get(0));
    }
}
