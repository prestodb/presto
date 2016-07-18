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

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

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
        long lowBitsMask = (1 << (bits - 1)) - 1; // set the least (bits - 1) bits
        if (num > lowBitsMask || num < ~lowBitsMask) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Number must be representable with the bits specified. " + num + " can not be represented with " + bits + " bits");
        }
        long mask = lowBitsMask | 0x8000_0000_0000_0000L; // set the least (bits - 1) bits and the sign bit
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
}
