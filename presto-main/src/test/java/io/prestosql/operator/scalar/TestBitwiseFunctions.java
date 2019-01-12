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
package io.prestosql.operator.scalar;

import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;

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
}
