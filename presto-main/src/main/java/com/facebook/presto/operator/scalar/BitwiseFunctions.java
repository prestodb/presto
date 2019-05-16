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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.Int128ArrayBlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic;
import com.facebook.presto.type.LiteralParameter;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.math.BigInteger;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class BitwiseFunctions
{
    private BitwiseFunctions() {}

    @Description("count number of set bits in 2's complement representation")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitCount(@SqlType(StandardTypes.BIGINT) long num, @SqlType(StandardTypes.BIGINT) long bits)
    {
        if (bits == 64) {
            return Long.bitCount(num);
        }
        if (bits <= 1 || bits > 64) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Bits specified in bit_count must be between 2 and 64, got " + bits);
        }
        long lowBitsMask = (1L << (bits - 1)) - 1; // set the least (bits - 1) bits
        if (num > lowBitsMask || num < ~lowBitsMask) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Number must be representable with the bits specified. " + num + " can not be represented with " + bits + " bits");
        }
        long mask = (1L << bits) - 1;
        return Long.bitCount(num & mask);
    }

    @Description("bitwise NOT in 2's complement arithmetic")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseNot(@SqlType(StandardTypes.BIGINT) long num)
    {
        return ~num;
    }

    @Description("bitwise AND in 2's complement arithmetic")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseAnd(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left & right;
    }

    @Description("bitwise OR in 2's complement arithmetic")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseOr(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left | right;
    }

    @Description("bitwise XOR in 2's complement arithmetic")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseXor(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left ^ right;
    }

    @Description("default BIGINT logical left shift operation")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseSll(@SqlType(StandardTypes.BIGINT) long number, @SqlType(StandardTypes.BIGINT) long shift)
    {
        return number << shift;
    }

    @Description("logical left shift operation with specified bits")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseSll(@SqlType(StandardTypes.BIGINT) long number, @SqlType(StandardTypes.BIGINT) long shift,
            @SqlType(StandardTypes.BIGINT) long bits)
    {
        if (bits == 64) {
            return bitwiseSll(number, shift);
        }

        if (bits <= 1 || bits > 64) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Bits specified in bit_count must be between 2 and 64, got " + bits);
        }

        if (shift < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Specified shift must be positive");
        }

        return (number << shift) & (long) (Math.pow(2, bits) - 1);
    }

    @Description("default BIGINT logical right shift operation")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseSrl(@SqlType(StandardTypes.BIGINT) long number, @SqlType(StandardTypes.BIGINT) long shift)
    {
        return number >>> shift;
    }

    @Description("logical right shift operation with specified bits")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseSrl(@SqlType(StandardTypes.BIGINT) long number, @SqlType(StandardTypes.BIGINT) long shift,
            @SqlType(StandardTypes.BIGINT) long bits)
    {
        if (bits == 64) {
            return bitwiseSrl(number, shift);
        }

        if (bits <= 1 || bits > 64) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Bits specified in bit_count must be between 2 and 64, got " + bits);
        }

        if (shift < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Specified shift must be positive");
        }

        number = number << (64 - bits);

        return (number >>> (64 - bits + shift)) & (long) (Math.pow(2, bits) - 1);
    }

    @Description("default BIGINT arithmetic right shift operation")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseSra(@SqlType(StandardTypes.BIGINT) long number, @SqlType(StandardTypes.BIGINT) long shift)
    {
        return number >> shift;
    }

    @Description("arithmetic right shift operation with specified bits")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseSra(@SqlType(StandardTypes.BIGINT) long number, @SqlType(StandardTypes.BIGINT) long shift,
            @SqlType(StandardTypes.BIGINT) long bits)
    {
        if (bits == 64) {
            return bitwiseSra(number, shift);
        }

        if (bits <= 1 || bits > 64) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Bits specified in bit_count must be between 2 and 64, got " + bits);
        }

        if (shift < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Specified shift must be positive");
        }

        number = number << (64 - bits);

        return (number >> (64 - bits + shift)) & (long) (Math.pow(2, bits) - 1);
    }

    @Description("default DECIMAL logical left shift operation")
    @ScalarFunction("bitwise_decimal_sll")
    @LiteralParameters({"p", "s"})
    @SqlType("decimal(p, s)")
    public static Slice bitwiseDecimalSll(@SqlType("decimal(p, s)") Slice number,
            @SqlType(StandardTypes.INTEGER) int shift)
    {
        BigInteger decimal = UnscaledDecimal128Arithmetic.unscaledDecimalToBigInteger(number);

        decimal.shiftLeft(shift);

        return UnscaledDecimal128Arithmetic.unscaledDecimal(decimal);
    }

    @ScalarFunction("bitwise_decimal_srl")
    @Description("default DECIMAL logical right shift operation for ShortDecimalType")
    public static final class bitwiseDecimalSrl
    {
        private bitwiseDecimalSrl() {}

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static long srlShort(@LiteralParameter("s") long numScale,
                @LiteralParameter("p") long resultPrecision,
                @SqlType("decimal(p, s)") long number, @SqlType(StandardTypes.INTEGER) int shift)
        {
            return bitwiseSrl(number, shift);
        }

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static Slice srlLong(@LiteralParameter("s") long numScale,
                @LiteralParameter("p") long resultPrecision,
                @SqlType("decimal(p, s)") Slice number,
                @SqlType(StandardTypes.INTEGER) int shift)
        {
            long high = number.getLong(0);
            long low = number.getLong(8);
            long highShift;

            if (shift > 64) {
                highShift = high >>> (shift - 64);
            } else {
                highShift = high << (64 - shift);
            }

            low = low >>> shift;
            low = low | highShift;
            high = high >>> shift;
            System.out.println(high);
            System.out.println(low);

            SliceOutput sliceOutput = new DynamicSliceOutput(16);
            sliceOutput.writeLong(high);
            sliceOutput.writeLong(low);

            return sliceOutput.slice();

        }
    }

    @Description("default DECIMAL arithmetic right shift operation")
    @ScalarFunction("bitwise_decimal_sra")
    @LiteralParameters({"p", "s"})
    @SqlType("decimal(p, s)")
    public static Slice bitwiseDecimalSra(@SqlType("decimal(p, s)") Slice number,
            @SqlType(StandardTypes.INTEGER) int shift)
    {
        BigInteger decimal = UnscaledDecimal128Arithmetic.unscaledDecimalToBigInteger(number);
        decimal = decimal.shiftRight(shift);
        return UnscaledDecimal128Arithmetic.unscaledDecimal(decimal);
    }

    @Description("default DECIMAL arithmetic right shift operation")
    @ScalarFunction("bitwise_decimal_sra")
    @LiteralParameters({"p", "s"})
    @SqlType("decimal(p, s)")
    public static Slice sraLong(@SqlType("decimal(p, s)") Slice number,
            @SqlType(StandardTypes.INTEGER) int shift)
    {
        long high = number.getLong(0);
        long low = number.getLong(8);
        long highShift;

        if (shift > 64) {
            highShift = high >>> (shift - 64);
        } else {
            highShift = high << (64 - shift);
        }

        low = low >>> shift;
        low = low | highShift;
        high = high >> shift;

        SliceOutput sliceOutput = new DynamicSliceOutput(16);
        sliceOutput.writeLong(high);
        sliceOutput.writeLong(low);

        return sliceOutput.slice();
    }
}
