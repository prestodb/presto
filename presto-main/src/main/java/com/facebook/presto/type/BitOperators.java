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
package com.facebook.presto.type;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import java.util.Arrays;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.type.BitType.binaryStrings;
import static com.facebook.presto.type.BitType.byteCount;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

public final class BitOperators
{
    private static final long[] BINARY_LONGS = binaryLongs();
    private static final Slice[] BINARY_SLICES = binarySlices();

    private BitOperators() {}

    @LiteralParameters("x")
    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType("bit(x)") Slice left, @SqlType("bit(x)") Slice right)
    {
        return left.equals(right);
    }

    @LiteralParameters("x")
    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType("bit(x)") Slice left, @SqlType("bit(x)") Slice right)
    {
        return !left.equals(right);
    }

    @LiteralParameters("x")
    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType("bit(x)") Slice left, @SqlType("bit(x)") Slice right)
    {
        return left.compareTo(right) < 0;
    }

    @LiteralParameters("x")
    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType("bit(x)") Slice left, @SqlType("bit(x)") Slice right)
    {
        return left.compareTo(right) <= 0;
    }

    @LiteralParameters("x")
    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType("bit(x)") Slice left, @SqlType("bit(x)") Slice right)
    {
        return left.compareTo(right) > 0;
    }

    @LiteralParameters("x")
    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType("bit(x)") Slice left, @SqlType("bit(x)") Slice right)
    {
        return left.compareTo(right) >= 0;
    }

    @LiteralParameters("x")
    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType("bit(x)") Slice value, @SqlType("bit(x)") Slice min, @SqlType("bit(x)") Slice max)
    {
        return min.compareTo(value) <= 0 && value.compareTo(max) <= 0;
    }

    @LiteralParameters("x")
    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType("bit(x)") Slice value)
    {
        return XxHash64.hash(value);
    }

    @LiteralParameters({"x", "y"})
    @ScalarOperator(IS_DISTINCT_FROM)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @SqlType("bit(x)") Slice left,
            @IsNull boolean leftNull,
            @SqlType("bit(y)") Slice right,
            @IsNull boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }

        if (left.length() == right.length()) {
            return notEqual(left, right);
        }

        if (left.length() > right.length()) {
            Slice temp = left;
            left = right;
            right = temp;
        }

        int start = right.length() - left.length();
        for (int i = 0; i < start; i++) {
            if (right.getByte(i) != 0) {
                return true;
            }
        }

        return !left.equals(0, left.length(), right, start, left.length());
    }

    @ScalarOperator(CAST)
    @SqlType("bit(y)")
    @LiteralParameters({"x", "y"})
    public static Slice bitToBitCast(
            @LiteralParameter("x") long x,
            @LiteralParameter("y") long y,
            @SqlType("bit(x)") Slice slice)
    {
        if (x < y) {
            Slice value = Slices.allocate(byteCount(toIntExact(y)));
            value.setBytes(value.length() - slice.length(), slice);
            return value;
        }

        if (x > y) {
            int bits = toIntExact(y);
            int count = byteCount(bits);
            int prefix = bits % 8;

            if (prefix == 0) {
                return slice.slice(slice.length() - count, count);
            }

            Slice value = Slices.copyOf(slice, slice.length() - count, count);
            value.setByte(0, value.getUnsignedByte(0) & ((1 << prefix) - 1));
            return value;
        }

        return slice;
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castToBinary(@SqlType("bit(x)") Slice slice)
    {
        return slice;
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType("bit(x)")
    public static Slice castFromBinary(@LiteralParameter("x") long x, @SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        int bits = toIntExact(x);
        int count = byteCount(bits);

        checkCondition((bits % 8) == 0, NOT_SUPPORTED, "Bit length must be a multiple of 8");
        checkCondition(slice.length() <= count, INVALID_CAST_ARGUMENT, "Binary data is longer than bit length");

        if (slice.length() == count) {
            return slice;
        }

        Slice value = Slices.allocate(count);
        value.setBytes(count - slice.length(), slice);
        return value;
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("x") long x, @SqlType("bit(x)") Slice slice)
    {
        int bits = toIntExact(x);
        int prefix = bits % 8;
        SliceOutput output = Slices.allocate(bits).getOutput();

        int start = 0;
        if (prefix > 0) {
            start = 1;
            output.writeBytes(BINARY_SLICES[slice.getUnsignedByte(0)], 8 - prefix, prefix);
        }

        for (int i = start; i < slice.length(); i++) {
            output.writeLong(BINARY_LONGS[slice.getUnsignedByte(i)]);
        }
        return output.getUnderlyingSlice();
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType("bit(x)")
    public static Slice castFromVarchar(@LiteralParameter("x") long x, @SqlType("varchar(x)") Slice slice)
    {
        int bits = toIntExact(x);
        checkCondition(slice.length() <= bits, INVALID_CAST_ARGUMENT, "String data is longer than bit length");

        Slice output = Slices.allocate(byteCount(bits));

        int start = output.length() - (slice.length() / 8) - 1;
        int prefix = slice.length() % 8;

        if (prefix > 0) {
            output.setByte(start, readBinary(slice, 0, prefix));
            start++;
        }

        for (int i = prefix; i < slice.length(); i += 8) {
            output.setByte(start + (i / 8), readBinary(slice, i, 8));
        }

        return output;
    }

    private static int readBinary(Slice slice, int offset, int length)
    {
        int value = 0;
        for (int i = 0; i < length; i++) {
            value <<= 1;
            byte b = slice.getByte(offset + i);
            if (b == '1') {
                value |= 1;
            }
            else if (b != '0') {
                throw new PrestoException(INVALID_CAST_ARGUMENT, "Invalid bit value in string (expected '0' or '1')");
            }
        }
        return value;
    }

    private static long[] binaryLongs()
    {
        return Arrays.stream(binaryStrings())
                .map(Slices::utf8Slice)
                .mapToLong(slice -> slice.getLong(0))
                .toArray();
    }

    private static Slice[] binarySlices()
    {
        return Arrays.stream(binaryStrings())
                .map(Slices::utf8Slice)
                .toArray(Slice[]::new);
    }
}
