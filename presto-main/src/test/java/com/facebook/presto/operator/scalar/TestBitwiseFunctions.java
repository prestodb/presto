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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.Int128ArrayBlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static java.sql.Types.DECIMAL;

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
        assertFunction("bit_count(-8, 6)", BIGINT, 3L);
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
        assertFunction("bitwise_sll(7, 2, 4)", BIGINT, 12L);
        assertFunction("bitwise_sll(7, 2)", BIGINT, 7L << 2L);
        assertFunction("bitwise_sll(-4, 6)", BIGINT, -4L << 6L);
        assertFunction("bitwise_sll(-4, 6, 5)", BIGINT, 0L);
        assertFunction("bitwise_sll(-4, 6, 9)", BIGINT, 256L);

        assertInvalidFunction("bitwise_sll(7, -3, 2)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testBitwiseSrl()
    {
        assertFunction("bitwise_srl(7, 2, 4)", BIGINT, 1L);
        assertFunction("bitwise_srl(7, 2)", BIGINT, 7L >>> 2L);
        assertFunction("bitwise_srl(-4, 6)", BIGINT, -4L >>> 6L);
        assertFunction("bitwise_srl(-8, 2, 5)", BIGINT, 6L);
        //-1152905011916701696 = 0xF0000F0000F00000
        assertFunction("bitwise_srl(-1152905011916701696, 62)", BIGINT, 3L);
        assertFunction("bitwise_srl(-1152905011916701696, 62, 4)", BIGINT, 0L);
        assertFunction("bitwise_srl(-1152905011916701696, 1, 4)", BIGINT, 0L);

        assertInvalidFunction("bitwise_srl(7, -3, 2)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testBitwiseSra()
    {
        assertFunction("bitwise_sra(7, 2, 4)", BIGINT, 1L);
        assertFunction("bitwise_sra(7, 2)", BIGINT, 7L >> 2L);
        assertFunction("bitwise_sra(-4, 6)", BIGINT, -4L >> 6L);
        assertFunction("bitwise_sra(-256, 3, 5)", BIGINT, 0L);
        assertFunction("bitwise_sra(-8, 2)", BIGINT, -2L);
        assertFunction("bitwise_sra(-256, 3, 12)", BIGINT, 4064L);
        //-1152905011916701696 = 0xF0000F0000F00000
        assertFunction("bitwise_sra(-1152905011916701696, 62)", BIGINT, -1L);
        assertFunction("bitwise_sra(-1152905011916701696, 62, 4)", BIGINT, 0L);
        assertFunction("bitwise_sra(-1152905011916701696, 1, 4)", BIGINT, 0L);

        assertInvalidFunction("bitwise_sra(7, -3, 2)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testBitwiseDecimalSra()
    {
        //assertDecimalFunction("DECIMAL '107.7'", decimal("107.7"));
       // assertDecimalFunction("bitwise_decimal_srl(DECIMAL '7', 2)", decimal("1"));


        SliceOutput sliceOutput = new DynamicSliceOutput(16);
      //  sliceOutput.writeLong(0);
        byte[] bytes =  {-1, -1, -1, -1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 0, 0, 7};
        BigInteger x = new BigInteger("-18446744073709551609");
        byte[] xArr = x.toByteArray();
      //  Slice x = UnscaledDecimal128Arithmetic.unscaledDecimal("-18446744073709551609");


       // testDecimal.
        SliceOutput sliceOutputTest = new DynamicSliceOutput(16);

        sliceOutputTest.writeBytes(bytes);
      //  sliceOutputTest.writeLong(new Long("-5000000000000"));
      //  sliceOutputTest.writeLong(new Long("40000000000000"));

        SliceOutput sliceOutputCompare = new DynamicSliceOutput(16);
     //   Long temp = new Long("-5000000000000");
        sliceOutputCompare.writeLong(-1);
        sliceOutputCompare.writeLong(7);
     //   sliceOutputCompare.writeLong(new Long("40000000000000"));
        Slice y = sliceOutputCompare.slice();

        Slice a = BitwiseFunctions.bitwiseDecimalSra(sliceOutputTest.slice(), 2);
        Slice b = BitwiseFunctions.sraLong(sliceOutputCompare.slice(), 2);
        System.out.println(BitwiseFunctions.bitwiseDecimalSra(sliceOutputTest.slice(), 2).getLong(0));
        System.out.println(BitwiseFunctions.bitwiseDecimalSra(sliceOutputTest.slice(), 2).getLong(1));
        System.out.println(BitwiseFunctions.sraLong(sliceOutputCompare.slice(), 2).getLong(0));
        System.out.println(BitwiseFunctions.sraLong(sliceOutputCompare.slice(), 2).getLong(8));
        //System.out.println(7 >> 2);

/*
      //  assertFunction("bitwise_decimal_sra(CAST(INTEGER '234' AS DECIMAL(4, 1)), CAST (INTEGER '2' AS BIGINT))", DecimalType.createDecimalType(), decimal("28"));
        assertFunction("bitwise_sra(7, 2)", BIGINT, 7L >> 2L);
        assertFunction("bitwise_sra(-4, 6)", BIGINT, -4L >> 6L);
        assertFunction("bitwise_sra(-256, 3, 5)", BIGINT, 0L);
        assertFunction("bitwise_sra(-8, 2)", BIGINT, -2L);
        assertFunction("bitwise_sra(-256, 3, 12)", BIGINT, 4064L);
        //-1152905011916701696 = 0xF0000F0000F00000
        assertFunction("bitwise_sra(-1152905011916701696, 62)", BIGINT, -1L);
        assertFunction("bitwise_sra(-1152905011916701696, 62, 4)", BIGINT, 0L);
        assertFunction("bitwise_sra(-1152905011916701696, 1, 4)", BIGINT, 0L);

        assertInvalidFunction("bitwise_sra(7, -3, 2)", INVALID_FUNCTION_ARGUMENT);
       */
    }
}
