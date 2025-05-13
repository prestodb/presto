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

import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

public class TestBitwiseFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testBitCount()
    {
        assertFunction("bit_count(0, 64)", BIGINT, 0L);
        assertFunction("bit_count(7, 64)", BIGINT, 3L);
        assertFunction("bit_count(24, 64)", BIGINT, 2L);
        assertFunction("bit_count(-8, 64)", BIGINT, 61L);
        assertFunction("bit_count(" + Integer.MAX_VALUE + ", 64)", BIGINT, 31L);
        assertFunction("bit_count(" + Integer.MIN_VALUE + ", 64)", BIGINT, 33L);
        assertFunction("bit_count(" + Long.MAX_VALUE + ", 64)", BIGINT, 63L);
        assertFunction("bit_count(-" + Long.MAX_VALUE + "-1, 64)", BIGINT, 1L); // bit_count(MIN_VALUE, 64)

        assertFunction("bit_count(0, 32)", BIGINT, 0L);
        assertFunction("bit_count(CAST (-8 AS SMALLINT), 6)", BIGINT, 3L);
        assertFunction("bit_count(7, 32)", BIGINT, 3L);
        assertFunction("bit_count(24, 32)", BIGINT, 2L);
        assertFunction("bit_count(-8, 32)", BIGINT, 29L);
        assertFunction("bit_count(" + Integer.MAX_VALUE + ", 32)", BIGINT, 31L);
        assertFunction("bit_count(" + Integer.MIN_VALUE + ", 32)", BIGINT, 1L);
        assertInvalidFunction("bit_count(" + (Integer.MAX_VALUE + 1L) + ", 32)", "Number must be representable with the bits specified. 2147483648 can not be represented with 32 bits");
        assertInvalidFunction("bit_count(" + (Integer.MIN_VALUE - 1L) + ", 32)", "Number must be representable with the bits specified. -2147483649 can not be represented with 32 bits");

        assertFunction("bit_count(1152921504598458367, 62)", BIGINT, 59L);
        assertFunction("bit_count(-1, 62)", BIGINT, 62L);
        assertFunction("bit_count(33554132, 26)", BIGINT, 20L);
        assertFunction("bit_count(-1, 26)", BIGINT, 26L);
        assertInvalidFunction("bit_count(1152921504598458367, 60)", "Number must be representable with the bits specified. 1152921504598458367 can not be represented with 60 bits");
        assertInvalidFunction("bit_count(33554132, 25)", "Number must be representable with the bits specified. 33554132 can not be represented with 25 bits");

        assertInvalidFunction("bit_count(0, -1)", "Bits specified in bit_count must be between 2 and 64, got -1");
        assertInvalidFunction("bit_count(0, 1)", "Bits specified in bit_count must be between 2 and 64, got 1");
        assertInvalidFunction("bit_count(0, 65)", "Bits specified in bit_count must be between 2 and 64, got 65");
    }

    @Test
    public void testBitwiseNot()
    {
        assertFunction("bitwise_not(0)", BIGINT, ~0L);
        assertFunction("bitwise_not(-1)", BIGINT, ~-1L);
        assertFunction("bitwise_not(8)", BIGINT, ~8L);
        assertFunction("bitwise_not(-8)", BIGINT, ~-8L);
        assertFunction("bitwise_not(" + Long.MAX_VALUE + ")", BIGINT, ~Long.MAX_VALUE);
        assertFunction("bitwise_not(-" + Long.MAX_VALUE + "-1)", BIGINT, ~Long.MIN_VALUE); // bitwise_not(MIN_VALUE)
    }

    @Test
    public void testBitwiseAnd()
    {
        assertFunction("bitwise_and(0, -1)", BIGINT, 0L);
        assertFunction("bitwise_and(3, 8)", BIGINT, 3L & 8L);
        assertFunction("bitwise_and(-4, 12)", BIGINT, -4L & 12L);
        assertFunction("bitwise_and(60, 21)", BIGINT, 60L & 21L);
    }

    @Test
    public void testBitwiseOr()
    {
        assertFunction("bitwise_or(0, -1)", BIGINT, -1L);
        assertFunction("bitwise_or(3, 8)", BIGINT, 3L | 8L);
        assertFunction("bitwise_or(-4, 12)", BIGINT, -4L | 12L);
        assertFunction("bitwise_or(60, 21)", BIGINT, 60L | 21L);
    }

    @Test
    public void testBitwiseXor()
    {
        assertFunction("bitwise_xor(0, -1)", BIGINT, -1L);
        assertFunction("bitwise_xor(3, 8)", BIGINT, 3L ^ 8L);
        assertFunction("bitwise_xor(-4, 12)", BIGINT, -4L ^ 12L);
        assertFunction("bitwise_xor(60, 21)", BIGINT, 60L ^ 21L);
    }

    @Test
    public void testBitwiseSll()
    {
        assertFunction("bitwise_shift_left(7, 2, 4)", BIGINT, 12L);
        assertFunction("bitwise_shift_left(7, 2, 64)", BIGINT, 7L << 2L);
        assertFunction("bitwise_shift_left(-4, 6, 64)", BIGINT, -4L << 6L);
        assertFunction("bitwise_shift_left(-4, 6, 5)", BIGINT, 0L);
        assertFunction("bitwise_shift_left(-4, 6, 9)", BIGINT, 256L);

        assertInvalidFunction("bitwise_shift_left(7, -3, 2)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testBitwiseSrl()
    {
        assertFunction("bitwise_logical_shift_right(7, 2, 4)", BIGINT, 1L);
        assertFunction("bitwise_logical_shift_right(7, 2, 64)", BIGINT, 7L >>> 2L);
        assertFunction("bitwise_logical_shift_right(-4, 6, 64)", BIGINT, -4L >>> 6L);
        assertFunction("bitwise_logical_shift_right(-8, 2, 5)", BIGINT, 6L);
        assertFunction(format("bitwise_logical_shift_right(%s, 62, 64)", 0xF0000F0000F00000L), BIGINT, 3L);
        assertFunction(format("bitwise_logical_shift_right(%s, 62, 4)", 0xF0000F0000F00000L), BIGINT, 0L);
        assertFunction(format("bitwise_logical_shift_right(%s, 1, 4)", 0xF0000F0000F00000L), BIGINT, 0L);

        assertInvalidFunction("bitwise_logical_shift_right(7, -3, 2)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testBitwiseSra()
    {
        assertFunction("bitwise_arithmetic_shift_right(7, 2)", BIGINT, 7L >> 2L);
        assertFunction("bitwise_arithmetic_shift_right(-4, 6)", BIGINT, -4L >> 6L);
        assertFunction("bitwise_arithmetic_shift_right(-256, 3)", BIGINT, -32L);
        assertFunction("bitwise_arithmetic_shift_right(-8, 2)", BIGINT, -2L);
        assertFunction(format("bitwise_arithmetic_shift_right(%s, 62)", 0xF0000F0000F00000L), BIGINT, -1L);

        assertInvalidFunction("bitwise_arithmetic_shift_right(7, -3)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testBitwiseLeftShift()
    {
        assertFunction("bitwise_left_shift(TINYINT'7', 2)", TINYINT, (byte) (7 << 2));
        assertFunction("bitwise_left_shift(TINYINT '-7', 2)", TINYINT, (byte) (-7 << 2));
        assertFunction("bitwise_left_shift(TINYINT '1', 7)", TINYINT, (byte) (1 << 7));
        assertFunction("bitwise_left_shift(TINYINT '-128', 1)", TINYINT, (byte) 0);
        assertFunction("bitwise_left_shift(TINYINT '-65', 1)", TINYINT, (byte) (-65 << 1));
        assertFunction("bitwise_left_shift(TINYINT '-7', 64)", TINYINT, (byte) 0);
        assertFunction("bitwise_left_shift(TINYINT '-128', 0)", TINYINT, (byte) -128);
        assertFunction("bitwise_left_shift(SMALLINT '7', 2)", SMALLINT, (short) (7 << 2));
        assertFunction("bitwise_left_shift(SMALLINT '-7', 2)", SMALLINT, (short) (-7 << 2));
        assertFunction("bitwise_left_shift(SMALLINT '1', 7)", SMALLINT, (short) (1 << 7));
        assertFunction("bitwise_left_shift(SMALLINT '-32768', 1)", SMALLINT, (short) 0);
        assertFunction("bitwise_left_shift(SMALLINT '-65', 1)", SMALLINT, (short) (-65 << 1));
        assertFunction("bitwise_left_shift(SMALLINT '-7', 64)", SMALLINT, (short) 0);
        assertFunction("bitwise_left_shift(SMALLINT '-32768', 0)", SMALLINT, (short) -32768);
        assertFunction("bitwise_left_shift(INTEGER '7', 2)", INTEGER, 7 << 2);
        assertFunction("bitwise_left_shift(INTEGER '-7', 2)", INTEGER, -7 << 2);
        assertFunction("bitwise_left_shift(INTEGER '1', 7)", INTEGER, 1 << 7);
        assertFunction("bitwise_left_shift(INTEGER '-2147483648', 1)", INTEGER, 0);
        assertFunction("bitwise_left_shift(INTEGER '-65', 1)", INTEGER, -65 << 1);
        assertFunction("bitwise_left_shift(INTEGER '-7', 64)", INTEGER, 0);
        assertFunction("bitwise_left_shift(INTEGER '-2147483648', 0)", INTEGER, -2147483648);
        assertFunction("bitwise_left_shift(BIGINT '7', 2)", BIGINT, 7L << 2);
        assertFunction("bitwise_left_shift(BIGINT '-7', 2)", BIGINT, -7L << 2);
        assertFunction("bitwise_left_shift(BIGINT '-7', 64)", BIGINT, 0L);
    }

    @Test
    public void testBitwiseRightShift()
    {
        assertFunction("bitwise_right_shift(TINYINT '7', 2)", TINYINT, (byte) (7 >>> 2));
        assertFunction("bitwise_right_shift(TINYINT '-7', 2)", TINYINT, (byte) 62);
        assertFunction("bitwise_right_shift(TINYINT '-7', 64)", TINYINT, (byte) 0);
        assertFunction("bitwise_right_shift(TINYINT '-128', 0)", TINYINT, (byte) -128);
        assertFunction("bitwise_right_shift(SMALLINT '7', 2)", SMALLINT, (short) (7 >>> 2));
        assertFunction("bitwise_right_shift(SMALLINT '-7', 2)", SMALLINT, (short) 16382);
        assertFunction("bitwise_right_shift(SMALLINT '-7', 64)", SMALLINT, (short) 0);
        assertFunction("bitwise_right_shift(SMALLINT '-32768', 0)", SMALLINT, (short) -32768);
        assertFunction("bitwise_right_shift(INTEGER '7', 2)", INTEGER, 7 >>> 2);
        assertFunction("bitwise_right_shift(INTEGER '-7', 2)", INTEGER, 1073741822);
        assertFunction("bitwise_right_shift(INTEGER '-7', 64)", INTEGER, 0);
        assertFunction("bitwise_right_shift(INTEGER '-2147483648', 0)", INTEGER, -2147483648);
        assertFunction("bitwise_right_shift(BIGINT '7', 2)", BIGINT, 7L >>> 2);
        assertFunction("bitwise_right_shift(BIGINT '-7', 2)", BIGINT, -7L >>> 2);
        assertFunction("bitwise_right_shift(BIGINT '-7', 64)", BIGINT, 0L);
    }

    @Test
    public void testBitwiseRightShiftArithmetic()
    {
        assertFunction("bitwise_right_shift_arithmetic(TINYINT '7', 2)", TINYINT, (byte) (7 >> 2));
        assertFunction("bitwise_right_shift_arithmetic(TINYINT '-7', 2)", TINYINT, (byte) (-7 >> 2));
        assertFunction("bitwise_right_shift_arithmetic(TINYINT '7', 64)", TINYINT, (byte) 0);
        assertFunction("bitwise_right_shift_arithmetic(TINYINT '-7', 64)", TINYINT, (byte) -1);
        assertFunction("bitwise_right_shift_arithmetic(TINYINT '-128', 0)", TINYINT, (byte) -128);
        assertFunction("bitwise_right_shift_arithmetic(SMALLINT '7', 2)", SMALLINT, (short) (7 >> 2));
        assertFunction("bitwise_right_shift_arithmetic(SMALLINT '-7', 2)", SMALLINT, (short) (-7 >> 2));
        assertFunction("bitwise_right_shift_arithmetic(SMALLINT '7', 64)", SMALLINT, (short) 0);
        assertFunction("bitwise_right_shift_arithmetic(SMALLINT '-7', 64)", SMALLINT, (short) -1);
        assertFunction("bitwise_right_shift_arithmetic(SMALLINT '-32768', 0)", SMALLINT, (short) -32768);
        assertFunction("bitwise_right_shift_arithmetic(INTEGER '7', 2)", INTEGER, (7 >> 2));
        assertFunction("bitwise_right_shift_arithmetic(INTEGER '-7', 2)", INTEGER, -7 >> 2);
        assertFunction("bitwise_right_shift_arithmetic(INTEGER '7', 64)", INTEGER, 0);
        assertFunction("bitwise_right_shift_arithmetic(INTEGER '-7', 64)", INTEGER, -1);
        assertFunction("bitwise_right_shift_arithmetic(INTEGER '-2147483648', 0)", INTEGER, -2147483648);
        assertFunction("bitwise_right_shift_arithmetic(BIGINT '7', 2)", BIGINT, 7L >> 2);
        assertFunction("bitwise_right_shift_arithmetic(BIGINT '-7', 2)", BIGINT, -7L >> 2);
        assertFunction("bitwise_right_shift_arithmetic(BIGINT '7', 64)", BIGINT, 0L);
        assertFunction("bitwise_right_shift_arithmetic(BIGINT '-7', 64)", BIGINT, -1L);
    }
}
