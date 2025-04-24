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
import com.facebook.airlift.units.Duration;
import com.facebook.presto.execution.Lifespan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.TestOperatorStats.assertExpectedOperatorStats;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestDriverStats
{
    public static final DriverStats EXPECTED = new DriverStats(
            Lifespan.driverGroup(21),

            1L,
            2L,
            3L,

            new Duration(4, NANOSECONDS),
            new Duration(5, NANOSECONDS),

            6L,
            7L,
            8L,

            new Duration(9, NANOSECONDS),
            new Duration(10, NANOSECONDS),
            new Duration(12, NANOSECONDS),
            false,
            ImmutableSet.of(),

            123L,

            13L,
            14,
            new Duration(15, NANOSECONDS),

            16L,
            17,

            18L,
            19,

            20L,

            ImmutableList.of(TestOperatorStats.NON_MERGEABLE));

    @Test
    public void testJson()
    {
        JsonCodec<DriverStats> codec = JsonCodec.jsonCodec(DriverStats.class);

        String json = codec.toJson(EXPECTED);
        DriverStats actual = codec.fromJson(json);

        assertExpectedDriverStats(actual);
    }

    public static void assertExpectedDriverStats(DriverStats actual)
    {
        assertEquals(actual.getLifespan(), Lifespan.driverGroup(21));

        assertEquals(actual.getCreateTimeInMillis(), new DateTime(1, UTC).getMillis());
        assertEquals(actual.getStartTimeInMillis(), new DateTime(2, UTC).getMillis());
        assertEquals(actual.getEndTimeInMillis(), new DateTime(3, UTC).getMillis());
        assertEquals(actual.getQueuedTime(), new Duration(4, NANOSECONDS));
        assertEquals(actual.getElapsedTime(), new Duration(5, NANOSECONDS));

        assertEquals(actual.getUserMemoryReservationInBytes(), 6L);
        assertEquals(actual.getRevocableMemoryReservationInBytes(), 7L);
        assertEquals(actual.getSystemMemoryReservationInBytes(), 8L);

        assertEquals(actual.getTotalScheduledTime(), new Duration(9, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(10, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(12, NANOSECONDS));

        assertEquals(actual.getTotalAllocationInBytes(), 123L);

        assertEquals(actual.getRawInputDataSizeInBytes(), 13L);
        assertEquals(actual.getRawInputPositions(), 14);
        assertEquals(actual.getRawInputReadTime(), new Duration(15, NANOSECONDS));

        assertEquals(actual.getProcessedInputDataSizeInBytes(), 16L);
        assertEquals(actual.getProcessedInputPositions(), 17);

        assertEquals(actual.getOutputDataSizeInBytes(), 18L);
        assertEquals(actual.getOutputPositions(), 19);

        assertEquals(actual.getPhysicalWrittenDataSizeInBytes(), 20L);

        assertEquals(actual.getOperatorStats().size(), 1);
        assertExpectedOperatorStats(actual.getOperatorStats().get(0));
    }
}
