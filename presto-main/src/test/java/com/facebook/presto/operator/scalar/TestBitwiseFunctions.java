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
    public void testBitwiseTrailingZeros()
    {
        assertFunction("bitwise_trailing_zeros(-1)", BIGINT, 0L);
        assertFunction("bitwise_trailing_zeros(-2)", BIGINT, 1L);
        assertFunction("bitwise_trailing_zeros(-4)", BIGINT, 2L);
        assertFunction("bitwise_trailing_zeros(-8)", BIGINT, 3L);
        assertFunction("bitwise_trailing_zeros(-16)", BIGINT, 4L);
        assertFunction("bitwise_trailing_zeros(-32)", BIGINT, 5L);
        assertFunction("bitwise_trailing_zeros(-64)", BIGINT, 6L);
        assertFunction("bitwise_trailing_zeros(-128)", BIGINT, 7L);
        assertFunction("bitwise_trailing_zeros(-256)", BIGINT, 8L);
        assertFunction("bitwise_trailing_zeros(-512)", BIGINT, 9L);
        assertFunction("bitwise_trailing_zeros(-1024)", BIGINT, 10L);
        assertFunction("bitwise_trailing_zeros(-2048)", BIGINT, 11L);
        assertFunction("bitwise_trailing_zeros(-4096)", BIGINT, 12L);
        assertFunction("bitwise_trailing_zeros(-8192)", BIGINT, 13L);
        assertFunction("bitwise_trailing_zeros(-16384)", BIGINT, 14L);
        assertFunction("bitwise_trailing_zeros(-32768)", BIGINT, 15L);
        assertFunction("bitwise_trailing_zeros(-65536)", BIGINT, 16L);
        assertFunction("bitwise_trailing_zeros(-131072)", BIGINT, 17L);
        assertFunction("bitwise_trailing_zeros(-262144)", BIGINT, 18L);
        assertFunction("bitwise_trailing_zeros(-524288)", BIGINT, 19L);
        assertFunction("bitwise_trailing_zeros(-1048576)", BIGINT, 20L);
        assertFunction("bitwise_trailing_zeros(-2097152)", BIGINT, 21L);
        assertFunction("bitwise_trailing_zeros(-4194304)", BIGINT, 22L);
        assertFunction("bitwise_trailing_zeros(-8388608)", BIGINT, 23L);
        assertFunction("bitwise_trailing_zeros(-16777216)", BIGINT, 24L);
        assertFunction("bitwise_trailing_zeros(-33554432)", BIGINT, 25L);
        assertFunction("bitwise_trailing_zeros(-67108864)", BIGINT, 26L);
        assertFunction("bitwise_trailing_zeros(-134217728)", BIGINT, 27L);
        assertFunction("bitwise_trailing_zeros(-268435456)", BIGINT, 28L);
        assertFunction("bitwise_trailing_zeros(-536870912)", BIGINT, 29L);
        assertFunction("bitwise_trailing_zeros(-1073741824)", BIGINT, 30L);
        assertFunction("bitwise_trailing_zeros(-2147483648)", BIGINT, 31L);
        assertFunction("bitwise_trailing_zeros(-4294967296)", BIGINT, 32L);
        assertFunction("bitwise_trailing_zeros(-8589934592)", BIGINT, 33L);
        assertFunction("bitwise_trailing_zeros(-17179869184)", BIGINT, 34L);
        assertFunction("bitwise_trailing_zeros(-34359738368)", BIGINT, 35L);
        assertFunction("bitwise_trailing_zeros(-68719476736)", BIGINT, 36L);
        assertFunction("bitwise_trailing_zeros(-137438953472)", BIGINT, 37L);
        assertFunction("bitwise_trailing_zeros(-274877906944)", BIGINT, 38L);
        assertFunction("bitwise_trailing_zeros(-549755813888)", BIGINT, 39L);
        assertFunction("bitwise_trailing_zeros(-1099511627776)", BIGINT, 40L);
        assertFunction("bitwise_trailing_zeros(-2199023255552)", BIGINT, 41L);
        assertFunction("bitwise_trailing_zeros(-4398046511104)", BIGINT, 42L);
        assertFunction("bitwise_trailing_zeros(-8796093022208)", BIGINT, 43L);
        assertFunction("bitwise_trailing_zeros(-17592186044416)", BIGINT, 44L);
        assertFunction("bitwise_trailing_zeros(-35184372088832)", BIGINT, 45L);
        assertFunction("bitwise_trailing_zeros(-70368744177664)", BIGINT, 46L);
        assertFunction("bitwise_trailing_zeros(-140737488355328)", BIGINT, 47L);
        assertFunction("bitwise_trailing_zeros(-281474976710656)", BIGINT, 48L);
        assertFunction("bitwise_trailing_zeros(-562949953421312)", BIGINT, 49L);
        assertFunction("bitwise_trailing_zeros(-1125899906842624)", BIGINT, 50L);
        assertFunction("bitwise_trailing_zeros(-2251799813685248)", BIGINT, 51L);
        assertFunction("bitwise_trailing_zeros(-4503599627370496)", BIGINT, 52L);
        assertFunction("bitwise_trailing_zeros(-9007199254740992)", BIGINT, 53L);
        assertFunction("bitwise_trailing_zeros(-18014398509481984)", BIGINT, 54L);
        assertFunction("bitwise_trailing_zeros(-36028797018963968)", BIGINT, 55L);
        assertFunction("bitwise_trailing_zeros(-72057594037927936)", BIGINT, 56L);
        assertFunction("bitwise_trailing_zeros(-144115188075855872)", BIGINT, 57L);
        assertFunction("bitwise_trailing_zeros(-288230376151711744)", BIGINT, 58L);
        assertFunction("bitwise_trailing_zeros(-576460752303423488)", BIGINT, 59L);
        assertFunction("bitwise_trailing_zeros(-1152921504606846976)", BIGINT, 60L);
        assertFunction("bitwise_trailing_zeros(-2305843009213693952)", BIGINT, 61L);
        assertFunction("bitwise_trailing_zeros(-4611686018427387904)", BIGINT, 62L);
        /* Presto does not accept -9223372036854775808 as a valid bigint literal, even though it is a valid value */
        /* assertFunction("bitwise_trailing_zeros(-9223372036854775808)", BIGINT, 63L); */
        assertFunction("bitwise_trailing_zeros(cast(-power(2, 63) as bigint))", BIGINT, 63L);
        assertFunction("bitwise_trailing_zeros(0)", BIGINT, 64L);
        assertFunction("bitwise_trailing_zeros(1)", BIGINT, 0L);
        assertFunction("bitwise_trailing_zeros(2)", BIGINT, 1L);
        assertFunction("bitwise_trailing_zeros(4)", BIGINT, 2L);
        assertFunction("bitwise_trailing_zeros(8)", BIGINT, 3L);
        assertFunction("bitwise_trailing_zeros(16)", BIGINT, 4L);
        assertFunction("bitwise_trailing_zeros(32)", BIGINT, 5L);
        assertFunction("bitwise_trailing_zeros(64)", BIGINT, 6L);
        assertFunction("bitwise_trailing_zeros(128)", BIGINT, 7L);
        assertFunction("bitwise_trailing_zeros(256)", BIGINT, 8L);
        assertFunction("bitwise_trailing_zeros(512)", BIGINT, 9L);
        assertFunction("bitwise_trailing_zeros(1024)", BIGINT, 10L);
        assertFunction("bitwise_trailing_zeros(2048)", BIGINT, 11L);
        assertFunction("bitwise_trailing_zeros(4096)", BIGINT, 12L);
        assertFunction("bitwise_trailing_zeros(8192)", BIGINT, 13L);
        assertFunction("bitwise_trailing_zeros(16384)", BIGINT, 14L);
        assertFunction("bitwise_trailing_zeros(32768)", BIGINT, 15L);
        assertFunction("bitwise_trailing_zeros(65536)", BIGINT, 16L);
        assertFunction("bitwise_trailing_zeros(131072)", BIGINT, 17L);
        assertFunction("bitwise_trailing_zeros(262144)", BIGINT, 18L);
        assertFunction("bitwise_trailing_zeros(524288)", BIGINT, 19L);
        assertFunction("bitwise_trailing_zeros(1048576)", BIGINT, 20L);
        assertFunction("bitwise_trailing_zeros(2097152)", BIGINT, 21L);
        assertFunction("bitwise_trailing_zeros(4194304)", BIGINT, 22L);
        assertFunction("bitwise_trailing_zeros(8388608)", BIGINT, 23L);
        assertFunction("bitwise_trailing_zeros(16777216)", BIGINT, 24L);
        assertFunction("bitwise_trailing_zeros(33554432)", BIGINT, 25L);
        assertFunction("bitwise_trailing_zeros(67108864)", BIGINT, 26L);
        assertFunction("bitwise_trailing_zeros(134217728)", BIGINT, 27L);
        assertFunction("bitwise_trailing_zeros(268435456)", BIGINT, 28L);
        assertFunction("bitwise_trailing_zeros(536870912)", BIGINT, 29L);
        assertFunction("bitwise_trailing_zeros(1073741824)", BIGINT, 30L);
        assertFunction("bitwise_trailing_zeros(2147483648)", BIGINT, 31L);
        assertFunction("bitwise_trailing_zeros(4294967296)", BIGINT, 32L);
        assertFunction("bitwise_trailing_zeros(8589934592)", BIGINT, 33L);
        assertFunction("bitwise_trailing_zeros(17179869184)", BIGINT, 34L);
        assertFunction("bitwise_trailing_zeros(34359738368)", BIGINT, 35L);
        assertFunction("bitwise_trailing_zeros(68719476736)", BIGINT, 36L);
        assertFunction("bitwise_trailing_zeros(137438953472)", BIGINT, 37L);
        assertFunction("bitwise_trailing_zeros(274877906944)", BIGINT, 38L);
        assertFunction("bitwise_trailing_zeros(549755813888)", BIGINT, 39L);
        assertFunction("bitwise_trailing_zeros(1099511627776)", BIGINT, 40L);
        assertFunction("bitwise_trailing_zeros(2199023255552)", BIGINT, 41L);
        assertFunction("bitwise_trailing_zeros(4398046511104)", BIGINT, 42L);
        assertFunction("bitwise_trailing_zeros(8796093022208)", BIGINT, 43L);
        assertFunction("bitwise_trailing_zeros(17592186044416)", BIGINT, 44L);
        assertFunction("bitwise_trailing_zeros(35184372088832)", BIGINT, 45L);
        assertFunction("bitwise_trailing_zeros(70368744177664)", BIGINT, 46L);
        assertFunction("bitwise_trailing_zeros(140737488355328)", BIGINT, 47L);
        assertFunction("bitwise_trailing_zeros(281474976710656)", BIGINT, 48L);
        assertFunction("bitwise_trailing_zeros(562949953421312)", BIGINT, 49L);
        assertFunction("bitwise_trailing_zeros(1125899906842624)", BIGINT, 50L);
        assertFunction("bitwise_trailing_zeros(2251799813685248)", BIGINT, 51L);
        assertFunction("bitwise_trailing_zeros(4503599627370496)", BIGINT, 52L);
        assertFunction("bitwise_trailing_zeros(9007199254740992)", BIGINT, 53L);
        assertFunction("bitwise_trailing_zeros(18014398509481984)", BIGINT, 54L);
        assertFunction("bitwise_trailing_zeros(36028797018963968)", BIGINT, 55L);
        assertFunction("bitwise_trailing_zeros(72057594037927936)", BIGINT, 56L);
        assertFunction("bitwise_trailing_zeros(144115188075855872)", BIGINT, 57L);
        assertFunction("bitwise_trailing_zeros(288230376151711744)", BIGINT, 58L);
        assertFunction("bitwise_trailing_zeros(576460752303423488)", BIGINT, 59L);
        assertFunction("bitwise_trailing_zeros(1152921504606846976)", BIGINT, 60L);
        assertFunction("bitwise_trailing_zeros(2305843009213693952)", BIGINT, 61L);
        assertFunction("bitwise_trailing_zeros(4611686018427387904)", BIGINT, 62L);
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
