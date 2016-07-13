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
package com.facebook.presto.spi.type;

import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static com.facebook.presto.spi.type.Decimals.MAX_DECIMAL_UNSCALED_VALUE;
import static com.facebook.presto.spi.type.Decimals.MIN_DECIMAL_UNSCALED_VALUE;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.compare;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.divide;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.hash;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.isNegative;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.multiply;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.overflows;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.rescale;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.shiftRight;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToBigInteger;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToUnscaledLong;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.wrappedIntArray;
import static io.airlift.slice.Slices.wrappedLongArray;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestUnscaledDecimal128Arithmetic
{
    private static final Slice MAX_DECIMAL = unscaledDecimal(MAX_DECIMAL_UNSCALED_VALUE);
    private static final Slice MIN_DECIMAL = unscaledDecimal(MIN_DECIMAL_UNSCALED_VALUE);

    @Test
    public void testUnscaledBigIntegerToDecimal()
    {
        assertConvertsUnscaledBigIntegerToDecimal(MAX_DECIMAL_UNSCALED_VALUE);
        assertConvertsUnscaledBigIntegerToDecimal(MIN_DECIMAL_UNSCALED_VALUE);
        assertConvertsUnscaledBigIntegerToDecimal(BigInteger.ZERO);
        assertConvertsUnscaledBigIntegerToDecimal(BigInteger.ONE);
        assertConvertsUnscaledBigIntegerToDecimal(BigInteger.ONE.negate());
    }

    @Test
    public void testUnscaledBigIntegerToDecimalOverflow()
    {
        assertUnscaledBigIntegerToDecimalOverflows(MAX_DECIMAL_UNSCALED_VALUE.add(BigInteger.ONE));
        assertUnscaledBigIntegerToDecimalOverflows(MAX_DECIMAL_UNSCALED_VALUE.setBit(95));
        assertUnscaledBigIntegerToDecimalOverflows(MAX_DECIMAL_UNSCALED_VALUE.setBit(127));
        assertUnscaledBigIntegerToDecimalOverflows(MIN_DECIMAL_UNSCALED_VALUE.subtract(BigInteger.ONE));
    }

    @Test
    public void testUnscaledLongToDecimal()
    {
        assertConvertsUnscaledLongToDecimal(0);
        assertConvertsUnscaledLongToDecimal(1);
        assertConvertsUnscaledLongToDecimal(-1);
        assertConvertsUnscaledLongToDecimal(Long.MAX_VALUE);
        assertConvertsUnscaledLongToDecimal(Long.MIN_VALUE);
    }

    @Test
    public void testDecimalToUnscaledLongOverflow()
    {
        assertDecimalToUnscaledLongOverflows(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
        assertDecimalToUnscaledLongOverflows(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE));
        assertDecimalToUnscaledLongOverflows(MAX_DECIMAL_UNSCALED_VALUE);
        assertDecimalToUnscaledLongOverflows(MIN_DECIMAL_UNSCALED_VALUE);
    }

    @Test
    public void testRescale()
    {
        assertEquals(rescale(unscaledDecimal(10), 0), unscaledDecimal(10L));
        assertEquals(rescale(unscaledDecimal(10), -20), unscaledDecimal(0L));
        assertEquals(rescale(unscaledDecimal(15), -1), unscaledDecimal(2));
        assertEquals(rescale(unscaledDecimal(1050), -3), unscaledDecimal(1));
        assertEquals(rescale(unscaledDecimal(15), 1), unscaledDecimal(150));
        assertEquals(rescale(unscaledDecimal(-14), -1), unscaledDecimal(-1));
        assertEquals(rescale(unscaledDecimal(-14), 1), unscaledDecimal(-140));
        assertEquals(rescale(unscaledDecimal(0), 1), unscaledDecimal(0));
        assertEquals(rescale(unscaledDecimal(5), -1), unscaledDecimal(1));
        assertEquals(rescale(unscaledDecimal(10), 10), unscaledDecimal(100000000000L));
        assertEquals(rescale(unscaledDecimal("150000000000000000000"), -20), unscaledDecimal(2));
        assertEquals(rescale(unscaledDecimal("-140000000000000000000"), -20), unscaledDecimal(-1));
        assertEquals(rescale(unscaledDecimal("50000000000000000000"), -20), unscaledDecimal(1));
        assertEquals(rescale(unscaledDecimal("150500000000000000000"), -18), unscaledDecimal(151));
        assertEquals(rescale(unscaledDecimal("-140000000000000000000"), -18), unscaledDecimal(-140));
        assertEquals(rescale(unscaledDecimal(BigInteger.ONE.shiftLeft(63)), -18), unscaledDecimal(9L));
        assertEquals(rescale(unscaledDecimal(BigInteger.ONE.shiftLeft(62)), -18), unscaledDecimal(5L));
        assertEquals(rescale(unscaledDecimal(BigInteger.ONE.shiftLeft(62)), -19), unscaledDecimal(0L));
        assertEquals(rescale(MAX_DECIMAL, -1), unscaledDecimal(MAX_DECIMAL_UNSCALED_VALUE.divide(BigInteger.TEN).add(BigInteger.ONE)));
        assertEquals(rescale(MIN_DECIMAL, -10), unscaledDecimal(MIN_DECIMAL_UNSCALED_VALUE.divide(BigInteger.valueOf(10000000000L)).subtract(BigInteger.ONE)));
        assertEquals(rescale(unscaledDecimal(1), 37), unscaledDecimal("10000000000000000000000000000000000000"));
        assertEquals(rescale(unscaledDecimal(-1), 37), unscaledDecimal("-10000000000000000000000000000000000000"));
        assertEquals(rescale(unscaledDecimal("10000000000000000000000000000000000000"), -37), unscaledDecimal(1));
    }

    @Test
    public void testRescaleOverflows()
    {
        assertRescaleOverflows(unscaledDecimal(1), 38);
    }

    @Test
    public void testMultiply()
    {
        assertEquals(multiply(unscaledDecimal(0), MAX_DECIMAL), unscaledDecimal(0));
        assertEquals(multiply(unscaledDecimal(1), MAX_DECIMAL), MAX_DECIMAL);
        assertEquals(multiply(unscaledDecimal(1), MIN_DECIMAL), MIN_DECIMAL);
        assertEquals(multiply(unscaledDecimal(-1), MAX_DECIMAL), MIN_DECIMAL);
        assertEquals(multiply(unscaledDecimal(-1), MIN_DECIMAL), MAX_DECIMAL);
        assertEquals(multiply(wrappedIntArray(0xFFFFFFFF, 0xFFFFFFFF, 0, 0), wrappedIntArray(0xFFFFFFFF, 0x00FFFFFF, 0, 0)), wrappedLongArray(0xff00000000000001L, 0xfffffffffffffeL));
        assertEquals(multiply(wrappedLongArray(0xFFFFFF0096BFB800L, 0), wrappedLongArray(0x39003539D9A51600L, 0)), wrappedLongArray(0x1CDBB17E11D00000L, 0x39003500FB00AB76L));
        assertEquals(multiply(unscaledDecimal(Integer.MAX_VALUE), unscaledDecimal(Integer.MIN_VALUE)), unscaledDecimal((long) Integer.MAX_VALUE * Integer.MIN_VALUE));
        assertEquals(multiply(unscaledDecimal("99999999999999"), unscaledDecimal("-1000000000000000000000000")), unscaledDecimal("-99999999999999000000000000000000000000"));
        assertEquals(multiply(unscaledDecimal("12380837221737387489365741632769922889"), unscaledDecimal("3")), unscaledDecimal("37142511665212162468097224898309768667"));
    }

    @Test
    public void testMultiplyOverflow()
    {
        assertMultiplyOverflows(unscaledDecimal("99999999999999"), unscaledDecimal("-10000000000000000000000000"));
        assertMultiplyOverflows(MAX_DECIMAL, unscaledDecimal("10"));
    }

    @Test
    public void testShiftRight()
    {
        assertShiftRight(unscaledDecimal(0), 0, true, unscaledDecimal(0));
        assertShiftRight(unscaledDecimal(0), 33, true, unscaledDecimal(0));

        assertShiftRight(unscaledDecimal(1), 1, true, unscaledDecimal(1));
        assertShiftRight(unscaledDecimal(-4), 1, true, unscaledDecimal(-2));

        assertShiftRight(unscaledDecimal(1L << 32), 32, true, unscaledDecimal(1));
        assertShiftRight(unscaledDecimal(1L << 31), 32, true, unscaledDecimal(1));
        assertShiftRight(unscaledDecimal(1L << 31), 32, false, unscaledDecimal(0));
        assertShiftRight(unscaledDecimal(3L << 33), 34, true, unscaledDecimal(2));
        assertShiftRight(unscaledDecimal(3L << 33), 34, false, unscaledDecimal(1));
        assertShiftRight(unscaledDecimal(BigInteger.valueOf(0x7FFFFFFFFFFFFFFFL).setBit(63).setBit(64)), 1, true, unscaledDecimal(BigInteger.ONE.shiftLeft(64)));

        assertShiftRight(MAX_DECIMAL, 1, true, unscaledDecimal(MAX_DECIMAL_UNSCALED_VALUE.shiftRight(1).add(BigInteger.ONE)));
        assertShiftRight(MIN_DECIMAL, 1, true, unscaledDecimal(MAX_DECIMAL_UNSCALED_VALUE.shiftRight(1).add(BigInteger.ONE).negate()));
        assertShiftRight(MAX_DECIMAL, 66, true, unscaledDecimal(MAX_DECIMAL_UNSCALED_VALUE.shiftRight(66).add(BigInteger.ONE)));
    }

    @Test
    public void testDivideCheckRound()
    {
        assertDivide(unscaledDecimal(0), 10, unscaledDecimal(0), 0);
        assertDivide(unscaledDecimal(5), 10, unscaledDecimal(0), 5);
        assertDivide(unscaledDecimal(-5), 10, negate(unscaledDecimal(0)), 5);
        assertDivide(unscaledDecimal(50), 100, unscaledDecimal(0), 50);

        assertDivide(unscaledDecimal(99), 10, unscaledDecimal(9), 9);
        assertDivide(unscaledDecimal(95), 10, unscaledDecimal(9), 5);
        assertDivide(unscaledDecimal(91), 10, unscaledDecimal(9), 1);

        assertDivide(unscaledDecimal("1000000000000000000000000"), 10, unscaledDecimal("100000000000000000000000"), 0);
        assertDivide(unscaledDecimal("-1000000000000000000000000"), 3, unscaledDecimal("-333333333333333333333333"), 1);
        assertDivide(unscaledDecimal("-1000000000000000000000000"), 9, unscaledDecimal("-111111111111111111111111"), 1);
    }

    @Test
    public void testOverflows()
    {
        assertTrue(overflows(unscaledDecimal("100"), 2));
        assertTrue(overflows(unscaledDecimal("-100"), 2));
        assertFalse(overflows(unscaledDecimal("99"), 2));
        assertFalse(overflows(unscaledDecimal("-99"), 2));
    }

    @Test
    public void testCompare()
    {
        assertCompare(unscaledDecimal(0), unscaledDecimal(0), 0);
        assertCompare(negate(unscaledDecimal(0)), unscaledDecimal(0), 0);
        assertCompare(unscaledDecimal(0), negate(unscaledDecimal(0)), 0);

        assertCompare(unscaledDecimal(0), unscaledDecimal(10), -1);
        assertCompare(unscaledDecimal(10), unscaledDecimal(0), 1);
        assertCompare(negate(unscaledDecimal(0)), unscaledDecimal(10), -1);
        assertCompare(unscaledDecimal(10), negate(unscaledDecimal(0)), 1);

        assertCompare(negate(unscaledDecimal(0)), MAX_DECIMAL, -1);
        assertCompare(MAX_DECIMAL, negate(unscaledDecimal(0)), 1);

        assertCompare(unscaledDecimal(-10), unscaledDecimal(-11), 1);
        assertCompare(unscaledDecimal(-11), unscaledDecimal(-11), 0);
        assertCompare(unscaledDecimal(-12), unscaledDecimal(-11), -1);

        assertCompare(unscaledDecimal(10), unscaledDecimal(11), -1);
        assertCompare(unscaledDecimal(11), unscaledDecimal(11), 0);
        assertCompare(unscaledDecimal(12), unscaledDecimal(11), 1);
    }

    @Test
    public void testNegate()
    {
        assertEquals(negate(negate(MIN_DECIMAL)), MIN_DECIMAL);
        assertEquals(negate(MIN_DECIMAL), MAX_DECIMAL);
        assertEquals(negate(MIN_DECIMAL), MAX_DECIMAL);

        assertEquals(negate(unscaledDecimal(1)), unscaledDecimal(-1));
        assertEquals(negate(unscaledDecimal(-1)), unscaledDecimal(1));
        assertEquals(negate(negate(unscaledDecimal(0))), unscaledDecimal(0));
    }

    @Test
    public void testIsNegative()
    {
        assertEquals(isNegative(MIN_DECIMAL), true);
        assertEquals(isNegative(MAX_DECIMAL), false);
        assertEquals(isNegative(unscaledDecimal(0)), false);
    }

    @Test
    public void testHash()
    {
        assertEquals(hash(unscaledDecimal(0)), hash(negate(unscaledDecimal(0))));
        assertNotEquals(hash(unscaledDecimal(0)), unscaledDecimal(1));
    }

    private static void assertUnscaledBigIntegerToDecimalOverflows(BigInteger value)
    {
        try {
            unscaledDecimal(value);
            fail();
        }
        catch (ArithmeticException ignored) {
        }
    }

    private static void assertDecimalToUnscaledLongOverflows(BigInteger value)
    {
        Slice decimal = unscaledDecimal(value);
        try {
            unscaledDecimalToUnscaledLong(decimal);
            fail();
        }
        catch (ArithmeticException ignored) {
        }
    }

    private static void assertMultiplyOverflows(Slice left, Slice right)
    {
        try {
            multiply(left, right);
            fail();
        }
        catch (ArithmeticException ignored) {
        }
    }

    private static void assertRescaleOverflows(Slice decimal, int rescaleFactor)
    {
        try {
            rescale(decimal, rescaleFactor);
            fail();
        }
        catch (ArithmeticException ignored) {
        }
    }

    private static void assertCompare(Slice left, Slice right, int expectedResult)
    {
        assertEquals(compare(left, right), expectedResult);
        assertEquals(compare(left.getLong(0), left.getLong(SIZE_OF_LONG), right.getLong(0), right.getLong(SIZE_OF_LONG)), expectedResult);
    }

    private static void assertConvertsUnscaledBigIntegerToDecimal(BigInteger value)
    {
        assertEquals(unscaledDecimalToBigInteger(unscaledDecimal(value)), value);
    }

    private static void assertConvertsUnscaledLongToDecimal(long value)
    {
        assertEquals(unscaledDecimalToUnscaledLong(unscaledDecimal(value)), value);
        assertEquals(unscaledDecimal(value), unscaledDecimal(BigInteger.valueOf(value)));
    }

    private static void assertShiftRight(Slice decimal, int rightShifts, boolean roundUp, Slice expectedResult)
    {
        Slice result = unscaledDecimal();
        shiftRight(decimal, rightShifts, roundUp, result);
        assertEquals(result, expectedResult);
    }

    private static void assertDivide(Slice decimal, int divisor, Slice expectedResult, int expectedRemainder)
    {
        Slice result = unscaledDecimal();
        int remainder = divide(decimal, divisor, result);
        assertEquals(result, expectedResult);
        assertEquals(remainder, expectedRemainder);
    }

    private static Slice negate(Slice slice)
    {
        Slice copy = unscaledDecimal(slice);
        UnscaledDecimal128Arithmetic.negate(copy);
        return copy;
    }
}
