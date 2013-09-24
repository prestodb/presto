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

import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.Assert;
import org.testng.annotations.Test;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TestOperatorStats
{
    public static final OperatorStats EXPECTED = new OperatorStats(
            41,
            "test",

            1,
            new Duration(2, NANOSECONDS),
            new Duration(3, NANOSECONDS),
            new Duration(4, NANOSECONDS),
            new DataSize(5, BYTE),
            6,

            7,
            new Duration(8, NANOSECONDS),
            new Duration(9, NANOSECONDS),
            new Duration(10, NANOSECONDS),
            new DataSize(11, BYTE),
            12,

            new Duration(13, NANOSECONDS),

            14,
            new Duration(15, NANOSECONDS),
            new Duration(16, NANOSECONDS),
            new Duration(17, NANOSECONDS),

            new DataSize(18, BYTE),
            "19");

    @Test
    public void testJson()
    {
        JsonCodec<OperatorStats> codec = JsonCodec.jsonCodec(OperatorStats.class);

        String json = codec.toJson(EXPECTED);
        OperatorStats actual = codec.fromJson(json);

        assertExpectedOperatorStats(actual);
    }

    public static void assertExpectedOperatorStats(OperatorStats actual)
    {
        Assert.assertEquals(actual.getOperatorId(), 41);
        Assert.assertEquals(actual.getOperatorType(), "test");

        Assert.assertEquals(actual.getAddInputCalls(), 1);
        Assert.assertEquals(actual.getAddInputWall(), new Duration(2, NANOSECONDS));
        Assert.assertEquals(actual.getAddInputCpu(), new Duration(3, NANOSECONDS));
        Assert.assertEquals(actual.getAddInputUser(), new Duration(4, NANOSECONDS));
        Assert.assertEquals(actual.getInputDataSize(), new DataSize(5, BYTE));
        Assert.assertEquals(actual.getInputPositions(), 6);

        Assert.assertEquals(actual.getGetOutputCalls(), 7);
        Assert.assertEquals(actual.getGetOutputWall(), new Duration(8, NANOSECONDS));
        Assert.assertEquals(actual.getGetOutputCpu(), new Duration(9, NANOSECONDS));
        Assert.assertEquals(actual.getGetOutputUser(), new Duration(10, NANOSECONDS));
        Assert.assertEquals(actual.getOutputDataSize(), new DataSize(11, BYTE));
        Assert.assertEquals(actual.getOutputPositions(), 12);

        Assert.assertEquals(actual.getBlockedWall(), new Duration(13, NANOSECONDS));

        Assert.assertEquals(actual.getFinishCalls(), 14);
        Assert.assertEquals(actual.getFinishWall(), new Duration(15, NANOSECONDS));
        Assert.assertEquals(actual.getFinishCpu(), new Duration(16, NANOSECONDS));
        Assert.assertEquals(actual.getFinishUser(), new Duration(17, NANOSECONDS));

        Assert.assertEquals(actual.getMemoryReservation(), new DataSize(18, BYTE));
        Assert.assertEquals(actual.getInfo(), "19");
    }

    @Test
    public void testAdd()
    {
        OperatorStats actual = EXPECTED.add(EXPECTED, EXPECTED);

        Assert.assertEquals(actual.getOperatorId(), 41);
        Assert.assertEquals(actual.getOperatorType(), "test");

        Assert.assertEquals(actual.getAddInputCalls(), 3 * 1);
        Assert.assertEquals(actual.getAddInputWall(), new Duration(3 * 2, NANOSECONDS));
        Assert.assertEquals(actual.getAddInputCpu(), new Duration(3 * 3, NANOSECONDS));
        Assert.assertEquals(actual.getAddInputUser(), new Duration(3 * 4, NANOSECONDS));
        Assert.assertEquals(actual.getInputDataSize(), new DataSize(3 * 5, BYTE));
        Assert.assertEquals(actual.getInputPositions(), 3 * 6);

        Assert.assertEquals(actual.getGetOutputCalls(), 3 * 7);
        Assert.assertEquals(actual.getGetOutputWall(), new Duration(3 * 8, NANOSECONDS));
        Assert.assertEquals(actual.getGetOutputCpu(), new Duration(3 * 9, NANOSECONDS));
        Assert.assertEquals(actual.getGetOutputUser(), new Duration(3 * 10, NANOSECONDS));
        Assert.assertEquals(actual.getOutputDataSize(), new DataSize(3 * 11, BYTE));
        Assert.assertEquals(actual.getOutputPositions(), 3 * 12);

        Assert.assertEquals(actual.getBlockedWall(), new Duration(3 * 13, NANOSECONDS));

        Assert.assertEquals(actual.getFinishCalls(), 3 * 14);
        Assert.assertEquals(actual.getFinishWall(), new Duration(3 * 15, NANOSECONDS));
        Assert.assertEquals(actual.getFinishCpu(), new Duration(3 * 16, NANOSECONDS));
        Assert.assertEquals(actual.getFinishUser(), new Duration(3 * 17, NANOSECONDS));
        Assert.assertEquals(actual.getMemoryReservation(), new DataSize(3 * 18, BYTE));
        Assert.assertEquals(actual.getInfo(), null);
    }
}
