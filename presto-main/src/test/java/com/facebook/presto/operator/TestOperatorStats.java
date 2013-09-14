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
            new Duration(1, NANOSECONDS),
            new Duration(2, NANOSECONDS),
            new Duration(3, NANOSECONDS),
            new DataSize(4, BYTE),
            5,

            new Duration(6, NANOSECONDS),
            new Duration(7, NANOSECONDS),
            new Duration(8, NANOSECONDS),
            new DataSize(9, BYTE),
            10,

            new Duration(11, NANOSECONDS),

            new Duration(12, NANOSECONDS),
            new Duration(13, NANOSECONDS),
            new Duration(14, NANOSECONDS),

            new DataSize(15, BYTE),
            "16");

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
        Assert.assertEquals(actual.getGetOutputWall(), new Duration(1, NANOSECONDS));
        Assert.assertEquals(actual.getGetOutputCpu(), new Duration(2, NANOSECONDS));
        Assert.assertEquals(actual.getGetOutputUser(), new Duration(3, NANOSECONDS));
        Assert.assertEquals(actual.getOutputDataSize(), new DataSize(4, BYTE));
        Assert.assertEquals(actual.getOutputPositions(), 5);
        Assert.assertEquals(actual.getAddInputWall(), new Duration(6, NANOSECONDS));
        Assert.assertEquals(actual.getAddInputCpu(), new Duration(7, NANOSECONDS));
        Assert.assertEquals(actual.getAddInputUser(), new Duration(8, NANOSECONDS));
        Assert.assertEquals(actual.getInputDataSize(), new DataSize(9, BYTE));
        Assert.assertEquals(actual.getInputPositions(), 10);
        Assert.assertEquals(actual.getBlockedWall(), new Duration(11, NANOSECONDS));
        Assert.assertEquals(actual.getFinishWall(), new Duration(12, NANOSECONDS));
        Assert.assertEquals(actual.getFinishCpu(), new Duration(13, NANOSECONDS));
        Assert.assertEquals(actual.getFinishUser(), new Duration(14, NANOSECONDS));
        Assert.assertEquals(actual.getMemoryReservation(), new DataSize(15, BYTE));
        Assert.assertEquals(actual.getInfo(), "16");
    }

    @Test
    public void testAdd()
    {
        OperatorStats actual = EXPECTED.add(EXPECTED, EXPECTED);

        Assert.assertEquals(actual.getOperatorId(), 41);
        Assert.assertEquals(actual.getOperatorType(), "test");
        Assert.assertEquals(actual.getGetOutputWall(), new Duration(3 * 1, NANOSECONDS));
        Assert.assertEquals(actual.getGetOutputCpu(), new Duration(3 * 2, NANOSECONDS));
        Assert.assertEquals(actual.getGetOutputUser(), new Duration(3 * 3, NANOSECONDS));
        Assert.assertEquals(actual.getOutputDataSize(), new DataSize(3 * 4, BYTE));
        Assert.assertEquals(actual.getOutputPositions(), 3 * 5);
        Assert.assertEquals(actual.getAddInputWall(), new Duration(3 * 6, NANOSECONDS));
        Assert.assertEquals(actual.getAddInputCpu(), new Duration(3 * 7, NANOSECONDS));
        Assert.assertEquals(actual.getAddInputUser(), new Duration(3 * 8, NANOSECONDS));
        Assert.assertEquals(actual.getInputDataSize(), new DataSize(3 * 9, BYTE));
        Assert.assertEquals(actual.getInputPositions(), 3 * 10);
        Assert.assertEquals(actual.getBlockedWall(), new Duration(3 * 11, NANOSECONDS));
        Assert.assertEquals(actual.getFinishWall(), new Duration(3 * 12, NANOSECONDS));
        Assert.assertEquals(actual.getFinishCpu(), new Duration(3 * 13, NANOSECONDS));
        Assert.assertEquals(actual.getFinishUser(), new Duration(3 * 14, NANOSECONDS));
        Assert.assertEquals(actual.getMemoryReservation(), new DataSize(3 * 15, BYTE));
        Assert.assertEquals(actual.getInfo(), null);
    }
}
