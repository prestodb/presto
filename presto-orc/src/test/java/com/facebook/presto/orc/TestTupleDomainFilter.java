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
package com.facebook.presto.orc;

import com.facebook.presto.orc.TupleDomainFilter.BigintMultiRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintValues;
import com.facebook.presto.orc.TupleDomainFilter.BooleanValue;
import com.facebook.presto.orc.TupleDomainFilter.BytesRange;
import com.facebook.presto.orc.TupleDomainFilter.BytesValues;
import com.facebook.presto.orc.TupleDomainFilter.DoubleRange;
import com.facebook.presto.orc.TupleDomainFilter.FloatRange;
import com.facebook.presto.orc.TupleDomainFilter.LongDecimalRange;
import com.facebook.presto.orc.TupleDomainFilter.MultiRange;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTupleDomainFilter
{
    @Test
    public void testBigintRange()
    {
        TupleDomainFilter filter = BigintRange.of(1, 1, false);

        assertTrue(filter.testLong(1));

        assertFalse(filter.testNull());
        assertFalse(filter.testLong(0));
        assertFalse(filter.testLong(11));

        filter = BigintRange.of(1, 10, false);

        assertTrue(filter.testLong(1));
        assertTrue(filter.testLong(10));

        assertFalse(filter.testNull());
        assertFalse(filter.testLong(0));
        assertFalse(filter.testLong(11));
    }

    @Test
    public void testBigintValues()
    {
        TupleDomainFilter filter = BigintValues.of(new long[] {1, 10, 100, 1000}, false);

        assertTrue(filter.testLong(1));
        assertTrue(filter.testLong(10));
        assertTrue(filter.testLong(100));
        assertTrue(filter.testLong(1000));

        assertFalse(filter.testNull());
        assertFalse(filter.testLong(-1));
        assertFalse(filter.testLong(2));
        assertFalse(filter.testLong(102));
        assertFalse(filter.testLong(Long.MAX_VALUE));
    }

    @Test
    public void testBigintMultiRange()
    {
        TupleDomainFilter filter = BigintMultiRange.of(ImmutableList.of(
                BigintRange.of(1, 10, false),
                BigintRange.of(100, 120, false)), false);

        assertTrue(filter.testLong(1));
        assertTrue(filter.testLong(5));
        assertTrue(filter.testLong(10));
        assertTrue(filter.testLong(100));
        assertTrue(filter.testLong(110));
        assertTrue(filter.testLong(120));

        assertFalse(filter.testNull());
        assertFalse(filter.testLong(0));
        assertFalse(filter.testLong(50));
        assertFalse(filter.testLong(150));
    }

    @Test
    public void testBooleanValue()
    {
        TupleDomainFilter filter = BooleanValue.of(true, false);
        assertTrue(filter.testBoolean(true));

        assertFalse(filter.testNull());
        assertFalse(filter.testBoolean(false));

        filter = BooleanValue.of(false, false);
        assertTrue(filter.testBoolean(false));

        assertFalse(filter.testNull());
        assertFalse(filter.testBoolean(true));
    }

    @Test
    public void testDoubleRange()
    {
        TupleDomainFilter filter = DoubleRange.of(1.2, false, false, 1.2, false, false, false);
        assertTrue(filter.testDouble(1.2));

        assertFalse(filter.testNull());
        assertFalse(filter.testDouble(1.3));

        filter = DoubleRange.of(Double.MIN_VALUE, true, true, 1.2, false, false, false);
        assertTrue(filter.testDouble(1.2));
        assertTrue(filter.testDouble(1.1));

        assertFalse(filter.testNull());
        assertFalse(filter.testDouble(1.3));

        filter = DoubleRange.of(1.2, false, true, Double.MAX_VALUE, true, true, false);
        assertTrue(filter.testDouble(1.3));
        assertTrue(filter.testDouble(5.6));

        assertFalse(filter.testNull());
        assertFalse(filter.testDouble(1.2));
        assertFalse(filter.testDouble(-19.267));

        filter = DoubleRange.of(1.2, false, false, 3.4, false, false, false);
        assertTrue(filter.testDouble(1.2));
        assertTrue(filter.testDouble(1.5));
        assertTrue(filter.testDouble(3.4));

        assertFalse(filter.testNull());
        assertFalse(filter.testDouble(-0.3));
        assertFalse(filter.testDouble(55.6));
    }

    @Test
    public void testFloatRange()
    {
        TupleDomainFilter filter = FloatRange.of(1.2f, false, false, 1.2f, false, false, false);
        assertTrue(filter.testFloat(1.2f));

        assertFalse(filter.testNull());
        assertFalse(filter.testFloat(1.1f));

        filter = FloatRange.of(Float.MIN_VALUE, true, true, 1.2f, false, true, false);
        assertTrue(filter.testFloat(1.1f));

        assertFalse(filter.testNull());
        assertFalse(filter.testFloat(1.2f));
        assertFalse(filter.testFloat(15.632f));

        filter = FloatRange.of(1.2f, false, false, 3.4f, false, false, false);
        assertTrue(filter.testFloat(1.2f));
        assertTrue(filter.testFloat(2.3f));
        assertTrue(filter.testFloat(3.4f));

        assertFalse(filter.testNull());
        assertFalse(filter.testFloat(1.1f));
        assertFalse(filter.testFloat(15.632f));
    }

    @Test
    public void testLongDecimalRange()
    {
        Slice decimal = decimal("123.45");
        TupleDomainFilter filter = LongDecimalRange.of(decimal.getLong(0), decimal.getLong(SIZE_OF_LONG), false, false, decimal.getLong(0), decimal.getLong(SIZE_OF_LONG), false, false, false);
        assertTrue(filter.testDecimal(decimal.getLong(0), decimal.getLong(SIZE_OF_LONG)));

        assertFalse(filter.testNull());
        assertFalse(filter.testDecimal(decimal("12.34").getLong(0), decimal("12.34").getLong(SIZE_OF_LONG)));

        filter = LongDecimalRange.of(Long.MIN_VALUE, Long.MIN_VALUE, true, true, decimal.getLong(0), decimal.getLong(SIZE_OF_LONG), false, false, false);
        assertTrue(filter.testDecimal(decimal.getLong(0), decimal.getLong(SIZE_OF_LONG)));
        assertTrue(filter.testDecimal(decimal("12.34").getLong(0), decimal("12.34").getLong(SIZE_OF_LONG)));

        assertFalse(filter.testNull());
        assertFalse(filter.testDecimal(decimal("1234.56").getLong(0), decimal("1234.56").getLong(SIZE_OF_LONG)));
    }

    private static Slice decimal(String value)
    {
        return encodeScaledValue(new BigDecimal(value));
    }

    @Test
    public void testBytesRange()
    {
        TupleDomainFilter filter = BytesRange.of(toBytes("abc"), false, toBytes("abc"), false, false);
        assertTrue(filter.testBytes(toBytes("abc"), 0, 3));
        assertTrue(filter.testLength(3));

        assertFalse(filter.testNull());
        assertFalse(filter.testBytes(toBytes("apple"), 0, 5));
        assertFalse(filter.testLength(4));

        String theBestOfTimes = "It was the best of times, it was the worst of times, it was the age of wisdom, it was the age of foolishness, it was the epoch of belief, it was the epoch of incredulity,...";
        filter = BytesRange.of(null, true, toBytes(theBestOfTimes), false, false);
        assertTrue(filter.testBytes(toBytes(theBestOfTimes), 0, theBestOfTimes.length()));
        assertTrue(filter.testBytes(toBytes(theBestOfTimes), 0, 5));
        assertTrue(filter.testBytes(toBytes(theBestOfTimes), 0, 50));
        assertTrue(filter.testBytes(toBytes(theBestOfTimes), 0, 100));
        // testLength is true of all lengths for a range filter.
        assertTrue(filter.testLength(1));
        assertTrue(filter.testLength(1000));

        assertFalse(filter.testNull());
        assertFalse(filter.testBytes(toBytes("Zzz"), 0, 3));
        assertFalse(filter.testBytes(toBytes("It was the best of times, zzz"), 0, 30));

        filter = BytesRange.of(toBytes("abc"), false, null, true, false);
        assertTrue(filter.testBytes(toBytes("abc"), 0, 3));
        assertTrue(filter.testBytes(toBytes("ad"), 0, 2));
        assertTrue(filter.testBytes(toBytes("apple"), 0, 5));
        assertTrue(filter.testBytes(toBytes("banana"), 0, 6));

        assertFalse(filter.testNull());
        assertFalse(filter.testBytes(toBytes("ab"), 0, 2));
        assertFalse(filter.testBytes(toBytes("_abc"), 0, 4));

        filter = BytesRange.of(toBytes("apple"), false, toBytes("banana"), false, false);
        assertTrue(filter.testBytes(toBytes("apple"), 0, 5));
        assertTrue(filter.testBytes(toBytes("banana"), 0, 6));
        assertTrue(filter.testBytes(toBytes("avocado"), 0, 7));

        assertFalse(filter.testNull());
        assertFalse(filter.testBytes(toBytes("camel"), 0, 5));
        assertFalse(filter.testBytes(toBytes("_abc"), 0, 4));

        filter = BytesRange.of(toBytes("apple"), true, toBytes("banana"), false, false);
        assertTrue(filter.testBytes(toBytes("banana"), 0, 6));
        assertTrue(filter.testBytes(toBytes("avocado"), 0, 7));

        assertFalse(filter.testNull());
        assertFalse(filter.testBytes(toBytes("apple"), 0, 5));
        assertFalse(filter.testBytes(toBytes("camel"), 0, 5));
        assertFalse(filter.testBytes(toBytes("_abc"), 0, 4));

        filter = BytesRange.of(toBytes("apple"), true, toBytes("banana"), true, false);
        assertTrue(filter.testBytes(toBytes("avocado"), 0, 7));

        assertFalse(filter.testNull());
        assertFalse(filter.testBytes(toBytes("apple"), 0, 5));
        assertFalse(filter.testBytes(toBytes("banana"), 0, 6));
        assertFalse(filter.testBytes(toBytes("camel"), 0, 5));
        assertFalse(filter.testBytes(toBytes("_abc"), 0, 4));
    }

    @Test
    public void testBytesValues()
    {
        // The filter has values of size on either side of 8 bytes.
        TupleDomainFilter filter = BytesValues.of(new byte[][] {toBytes("Igne"), toBytes("natura"), toBytes("renovitur"), toBytes("integra.")}, false);
        assertTrue(filter.testBytes(toBytes("Igne"), 0, 4));
        assertTrue(filter.testBytes(toBytes("natura"), 0, 6));
        assertTrue(filter.testBytes(toBytes("renovitur"), 0, 9));
        assertTrue(filter.testBytes(toBytes("integra."), 0, 8));

        assertFalse(filter.testNull());
        assertFalse(filter.testBytes(toBytes("natura"), 0, 5));
        assertFalse(filter.testBytes(toBytes("apple"), 0, 5));

        byte[][] testValues = new byte[1000][];
        byte[][] filterValues = new byte[(testValues.length / 9) + 1][];
        byte base = 0;
        int numFilterValues = 0;
        for (int i = 0; i < testValues.length; i++) {
            testValues[i] = sequentialBytes(base, i);
            base = (byte) (base + i);
            if (i % 9 == 0) {
                filterValues[numFilterValues++] = testValues[i];
            }
        }
        filter = BytesValues.of(filterValues, false);
        assertFalse(filter.testLength(10000));
        for (int i = 0; i < testValues.length; i++) {
            assertEquals(filter.testLength(i), i % 9 == 0);
            assertEquals(i % 9 == 0, filter.testBytes(testValues[i], 0, testValues[i].length));
        }
    }

    private static byte[] sequentialBytes(byte base, int length)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (base + i);
        }
        return bytes;
    }

    private static byte[] toBytes(String value)
    {
        return Slices.utf8Slice(value).getBytes();
    }

    @Test
    public void testMultiRange()
    {
        TupleDomainFilter filter = MultiRange.of(ImmutableList.of(
                BytesRange.of(toBytes("abc"), false, toBytes("abc"), false, false),
                BytesRange.of(toBytes("dragon"), false, null, true, false)), false);

        assertTrue(filter.testBytes(toBytes("abc"), 0, 3));
        assertTrue(filter.testBytes(toBytes("dragon"), 0, 6));
        assertTrue(filter.testBytes(toBytes("dragonfly"), 0, 9));
        assertTrue(filter.testBytes(toBytes("drought"), 0, 7));

        assertFalse(filter.testNull());
        assertFalse(filter.testBytes(toBytes("apple"), 0, 5));

        filter = MultiRange.of(ImmutableList.of(
                DoubleRange.of(Double.MIN_VALUE, true, true, 1.2, false, true, false),
                DoubleRange.of(1.2, false, true, Double.MAX_VALUE, true, true, false)), false);

        assertTrue(filter.testDouble(1.1));
        assertTrue(filter.testDouble(1.3));

        assertFalse(filter.testNull());
        assertFalse(filter.testDouble(1.2));

        Slice decimal = decimal("123.45");
        filter = MultiRange.of(ImmutableList.of(
                LongDecimalRange.of(Long.MIN_VALUE, Long.MIN_VALUE, true, true, decimal.getLong(0), decimal.getLong(SIZE_OF_LONG), false, true, false),
                LongDecimalRange.of(decimal.getLong(0), decimal.getLong(SIZE_OF_LONG), false, true, Long.MAX_VALUE, Long.MAX_VALUE, true, true, false)), false);

        assertTrue(filter.testDecimal(decimal("1.23").getLong(0), decimal("1.23").getLong(SIZE_OF_LONG)));
        assertTrue(filter.testDecimal(decimal("12.34").getLong(0), decimal("12.34").getLong(SIZE_OF_LONG)));

        assertFalse(filter.testNull());
        assertFalse(filter.testDecimal(decimal.getLong(0), decimal.getLong(SIZE_OF_LONG)));

        filter = MultiRange.of(ImmutableList.of(
                FloatRange.of(Float.MIN_VALUE, true, true, 1.2f, false, true, false),
                FloatRange.of(1.2f, false, true, Float.MAX_VALUE, true, true, false)), false);

        assertTrue(filter.testFloat(1.1f));
        assertTrue(filter.testFloat(1.3f));

        assertFalse(filter.testNull());
        assertFalse(filter.testFloat(1.2f));
    }
}
