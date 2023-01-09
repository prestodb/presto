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
package com.facebook.presto.server;

import com.facebook.airlift.json.JsonCodec;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestQueryProgressStats
{
    @Test
    public void testJson()
    {
        QueryProgressStats expected = new QueryProgressStats(
                123456,
                1111,
                33333,
                22222,
                3333,
                100000,
                34230492,
                34230493,
                34230494,
                1200.0,
                1300.0,
                1000,
                100000,
                true,
                Optional.of(ImmutableSet.of(WAITING_FOR_MEMORY)),
                OptionalDouble.of(33.33),
                1200,
                1100,
                1000);
        JsonCodec<QueryProgressStats> codec = JsonCodec.jsonCodec(QueryProgressStats.class);

        String json = codec.toJson(expected);
        QueryProgressStats actual = codec.fromJson(json);

        assertEquals(actual.getElapsedTimeMillis(), 123456);
        assertEquals(actual.getQueuedTimeMillis(), 1111);
        assertEquals(actual.getExecutionTimeMillis(), 33333);
        assertEquals(actual.getCpuTimeMillis(), 22222);
        assertEquals(actual.getScheduledTimeMillis(), 3333);
        assertEquals(actual.getCurrentMemoryBytes(), 100000);
        assertEquals(actual.getPeakMemoryBytes(), 34230492);
        assertEquals(actual.getPeakTotalMemoryBytes(), 34230493);
        assertEquals(actual.getPeakTaskTotalMemoryBytes(), 34230494);
        assertEquals(actual.getCumulativeUserMemory(), 1200.0);
        assertEquals(actual.getCumulativeTotalMemory(), 1300.0);
        assertEquals(actual.getInputRows(), 1000);
        assertEquals(actual.getInputBytes(), 100000);
        assertTrue(actual.isBlocked());
        assertEquals(actual.getBlockedReasons(), Optional.of(ImmutableSet.of(WAITING_FOR_MEMORY)));
        assertEquals(actual.getProgressPercentage(), OptionalDouble.of(33.33));
        assertEquals(actual.getQueuedDrivers(), 1200);
        assertEquals(actual.getRunningDrivers(), 1100);
        assertEquals(actual.getCompletedDrivers(), 1000);
    }
}
