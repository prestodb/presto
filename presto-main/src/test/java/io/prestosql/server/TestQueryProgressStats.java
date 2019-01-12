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
package io.prestosql.server;

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.OptionalDouble;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestQueryProgressStats
{
    @Test
    public void testJson()
    {
        QueryProgressStats expected = new QueryProgressStats(
                123456,
                1111,
                22222,
                3333,
                100000,
                34230492,
                1000,
                100000,
                false,
                OptionalDouble.of(33.33));
        JsonCodec<QueryProgressStats> codec = JsonCodec.jsonCodec(QueryProgressStats.class);

        String json = codec.toJson(expected);
        QueryProgressStats actual = codec.fromJson(json);

        assertEquals(actual.getElapsedTimeMillis(), 123456);
        assertEquals(actual.getQueuedTimeMillis(), 1111);
        assertEquals(actual.getCpuTimeMillis(), 22222);
        assertEquals(actual.getScheduledTimeMillis(), 3333);
        assertEquals(actual.getCurrentMemoryBytes(), 100000);
        assertEquals(actual.getPeakMemoryBytes(), 34230492);
        assertEquals(actual.getInputRows(), 1000);
        assertEquals(actual.getInputBytes(), 100000);
        assertFalse(actual.isBlocked());
        assertEquals(actual.getProgressPercentage(), OptionalDouble.of(33.33));
    }
}
