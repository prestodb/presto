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
package com.facebook.presto.raptorx.util;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.raptorx.util.FloatingPointUtil.doubleToSortableLong;
import static com.facebook.presto.raptorx.util.FloatingPointUtil.floatToSortableInt;
import static com.facebook.presto.raptorx.util.FloatingPointUtil.sortableIntToFloat;
import static com.facebook.presto.raptorx.util.FloatingPointUtil.sortableLongToDouble;
import static java.util.Collections.shuffle;
import static java.util.Comparator.comparingInt;
import static java.util.Comparator.comparingLong;
import static org.testng.Assert.assertEquals;

public class TestFloatingPointUtil
{
    @Test(invocationCount = 10)
    public void testDoubleComparison()
    {
        List<Double> expected = ImmutableList.of(
                Double.NEGATIVE_INFINITY,
                -456.78,
                -123.45,
                -0.0,
                0.0,
                123.45,
                456.78,
                Double.POSITIVE_INFINITY,
                Double.NaN);

        List<Double> values = new ArrayList<>(expected);
        shuffle(values);
        values.sort(comparingLong(FloatingPointUtil::doubleToSortableLong));

        assertEquals(values, expected);
    }

    @Test(invocationCount = 10)
    public void testFloatComparison()
    {
        List<Float> expected = ImmutableList.of(
                Float.NEGATIVE_INFINITY,
                -456.78f,
                -123.45f,
                -0.0f,
                0.0f,
                123.45f,
                456.78f,
                Float.POSITIVE_INFINITY,
                Float.NaN);

        List<Float> values = new ArrayList<>(expected);
        shuffle(values);
        values.sort(comparingInt(FloatingPointUtil::floatToSortableInt));

        assertEquals(values, expected);
    }

    @Test
    public void testDoubleRoundTrip()
    {
        assertDoubleRoundTrip(0.0);
        assertDoubleRoundTrip(-0.0);
        assertDoubleRoundTrip(123.45);
        assertDoubleRoundTrip(-123.45);
        assertDoubleRoundTrip(Double.NEGATIVE_INFINITY);
        assertDoubleRoundTrip(Double.POSITIVE_INFINITY);
        assertDoubleRoundTrip(Double.NaN);
    }

    @Test
    public void testFloatRoundTrip()
    {
        assertFloatRoundTrip(0.0f);
        assertFloatRoundTrip(-0.0f);
        assertFloatRoundTrip(123.45f);
        assertFloatRoundTrip(-123.45f);
        assertFloatRoundTrip(Float.NEGATIVE_INFINITY);
        assertFloatRoundTrip(Float.POSITIVE_INFINITY);
        assertFloatRoundTrip(Float.NaN);
    }

    private static void assertDoubleRoundTrip(double value)
    {
        double transformed = sortableLongToDouble(doubleToSortableLong(value));
        assertEquals(Double.compare(transformed, value), 0);
    }

    private static void assertFloatRoundTrip(float value)
    {
        float transformed = sortableIntToFloat(floatToSortableInt(value));
        assertEquals(Float.compare(transformed, value), 0);
    }
}
