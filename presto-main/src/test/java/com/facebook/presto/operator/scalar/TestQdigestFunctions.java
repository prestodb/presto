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
package com.facebook.presto.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.stats.QuantileDigest;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.doubleToSortableLong;
import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.floatToSortableInt;
import static java.lang.Float.intBitsToFloat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestQdigestFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testPercentile()
    {
        assertEquals(QuantileDigestFunctions.valueAtQuantileBigint(buildDigestLong(ImmutableList.of(1L, 2L, 3L, 4L, 5L)), 0.5), 3L);
        assertEquals(QuantileDigestFunctions.valueAtQuantileBigint(buildDigestLong(ImmutableList.of(-1L, 10L, 30L)), 0.1), -1L);

        assertEquals(QuantileDigestFunctions.valueAtQuantileDouble(buildDigestDouble(ImmutableList.of(1.0, 2.0, 3.5, 4.5, 5.0)), 0.5), 3.5);
        assertEquals(QuantileDigestFunctions.valueAtQuantileDouble(buildDigestDouble(ImmutableList.of(-0.3, 10.0, 30.3)), 0.1), -0.3);

        assertEquals(QuantileDigestFunctions.valueAtQuantileReal(buildDigestFloat(ImmutableList.of(1.0f, 2.0f, 3.5f, 4.5f, 5.0f)), 0.5), 3);
        assertEquals(QuantileDigestFunctions.valueAtQuantileReal(buildDigestFloat(ImmutableList.of(-0.3f, 10.0f, 30.3f)), 0.1), 0);
    }

    @Test
    public void testTruncatedMean()
    {
        assertNull(QuantileDigestFunctions.truncatedMeanBigint(buildDigestLong(ImmutableList.of(1L)), 0, 0.5));
        assertEquals(QuantileDigestFunctions.truncatedMeanBigint(buildDigestLong(ImmutableList.of(1L, 2L, 3L, 4L, 5L)), 0, 0.5), 1.5);
        assertEquals(QuantileDigestFunctions.truncatedMeanBigint(buildDigestLong(ImmutableList.of(-1L, 5L, 8L)), 0, 0.9), 2.0);

        assertNull(QuantileDigestFunctions.truncatedMeanDouble(buildDigestDouble(ImmutableList.of(0.3, 0.7)), 0, 0.2));
        assertEquals(QuantileDigestFunctions.truncatedMeanDouble(buildDigestDouble(ImmutableList.of(0.3, 0.7, 1.5, 4.0, 5.0)), 0, 0.5), 0.5);
        assertEquals(QuantileDigestFunctions.truncatedMeanDouble(buildDigestDouble(ImmutableList.of(-0.5, 0.0, 45.0)), 0, 0.9), -0.25);

        assertNull(QuantileDigestFunctions.truncatedMeanReal(buildDigestFloat(ImmutableList.of(1f, 5f)), 0, 0.3));
        assertEquals(QuantileDigestFunctions.truncatedMeanReal(buildDigestFloat(ImmutableList.of(1f, 2f, 3f, 4f, 5f)), 0, 0.5), Long.valueOf(2));
        assertEquals(QuantileDigestFunctions.truncatedMeanReal(buildDigestFloat(ImmutableList.of(-0.5f, 1.1f, 8f)), 0, 0.9), Long.valueOf(0));
    }

    private Slice buildDigestLong(List<Long> vals)
    {
        QuantileDigest digest = new QuantileDigest(0.01);
        for (long v : vals) {
            digest.add(v);
        }
        return digest.serialize();
    }

    private Slice buildDigestDouble(List<Double> vals)
    {
        QuantileDigest digest = new QuantileDigest(0.01);
        for (double v : vals) {
            digest.add(doubleToSortableLong(v));
        }
        return digest.serialize();
    }

    private Slice buildDigestFloat(List<Float> vals)
    {
        QuantileDigest digest = new QuantileDigest(0.01);
        for (double v : vals) {
            digest.add(floatToSortableInt(intBitsToFloat((int) v)));
        }
        return digest.serialize();
    }
}
