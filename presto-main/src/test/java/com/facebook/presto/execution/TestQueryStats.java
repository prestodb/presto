package com.facebook.presto.execution;

import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
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

            8,
            9,
            10,

            11,
            12,
            13,
            14,
            15,

            new DataSize(16, BYTE),

            new Duration(17, NANOSECONDS),
            new Duration(18, NANOSECONDS),
            new Duration(19, NANOSECONDS),
            new Duration(20, NANOSECONDS),

            new DataSize(21, BYTE),
            22,

            new DataSize(23, BYTE),
            24,

            new DataSize(25, BYTE),
            26);

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
        assertEquals(actual.getCreateTime(), new DateTime(1));
        assertEquals(actual.getExecutionStartTime(), new DateTime(2));
        assertEquals(actual.getLastHeartbeat(), new DateTime(3));
        assertEquals(actual.getEndTime(), new DateTime(4));
        assertEquals(actual.getQueuedTime(), new Duration(5, NANOSECONDS));
        assertEquals(actual.getAnalysisTime(), new Duration(6, NANOSECONDS));
        assertEquals(actual.getDistributedPlanningTime(), new Duration(7, NANOSECONDS));

        assertEquals(actual.getTotalTasks(), 8);
        assertEquals(actual.getRunningTasks(), 9);
        assertEquals(actual.getCompletedTasks(), 10);

        assertEquals(actual.getTotalDrivers(), 11);
        assertEquals(actual.getQueuedDrivers(), 12);
        assertEquals(actual.getRunningDrivers(), 13);
        assertEquals(actual.getStartedDrivers(), 14);
        assertEquals(actual.getCompletedDrivers(), 15);

        assertEquals(actual.getTotalMemoryReservation(), new DataSize(16, BYTE));

        assertEquals(actual.getTotalScheduledTime(), new Duration(17, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(18, NANOSECONDS));
        assertEquals(actual.getTotalUserTime(), new Duration(19, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(20, NANOSECONDS));

        assertEquals(actual.getRawInputDataSize(), new DataSize(21, BYTE));
        assertEquals(actual.getRawInputPositions(), 22);

        assertEquals(actual.getProcessedInputDataSize(), new DataSize(23, BYTE));
        assertEquals(actual.getProcessedInputPositions(), 24);

        assertEquals(actual.getOutputDataSize(), new DataSize(25, BYTE));
        assertEquals(actual.getOutputPositions(), 26);
    }
}
