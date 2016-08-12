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
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.DIVIDE;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.MODULUS;
import static com.facebook.presto.spi.function.OperatorType.MULTIPLY;
import static com.facebook.presto.spi.function.OperatorType.NEGATION;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.lang.String.valueOf;

public final class SmallintOperators
{
    private SmallintOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.SMALLINT)
    public static long add(@SqlType(StandardTypes.SMALLINT) long left, @SqlType(StandardTypes.SMALLINT) long right)
    {
        try {
            return Shorts.checkedCast(left + right);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("smallint addition overflow: %s + %s", left, right), e);
        }
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.SMALLINT)
    public static long subtract(@SqlType(StandardTypes.SMALLINT) long left, @SqlType(StandardTypes.SMALLINT) long right)
    {
        try {
            return Shorts.checkedCast(left - right);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("smallint subtraction overflow: %s - %s", left, right), e);
        }
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.SMALLINT)
    public static long multiply(@SqlType(StandardTypes.SMALLINT) long left, @SqlType(StandardTypes.SMALLINT) long right)
    {
        try {
            return Shorts.checkedCast(left * right);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("smallint multiplication overflow: %s * %s", left, right), e);
        }
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.SMALLINT)
    public static long divide(@SqlType(StandardTypes.SMALLINT) long left, @SqlType(StandardTypes.SMALLINT) long right)
    {
        try {
            return left / right;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(DIVISION_BY_ZERO, e);
        }
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.SMALLINT)
    public static long modulus(@SqlType(StandardTypes.SMALLINT) long left, @SqlType(StandardTypes.SMALLINT) long right)
    {
        try {
            return left % right;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(DIVISION_BY_ZERO, e);
        }
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.SMALLINT)
    public static long negate(@SqlType(StandardTypes.SMALLINT) long value)
    {
        try {
            return Shorts.checkedCast(-value);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "smallint negation overflow: " + value, e);
        }
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType(StandardTypes.SMALLINT) long left, @SqlType(StandardTypes.SMALLINT) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(StandardTypes.SMALLINT) long left, @SqlType(StandardTypes.SMALLINT) long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.SMALLINT) long left, @SqlType(StandardTypes.SMALLINT) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.SMALLINT) long left, @SqlType(StandardTypes.SMALLINT) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.SMALLINT) long left, @SqlType(StandardTypes.SMALLINT) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.SMALLINT) long left, @SqlType(StandardTypes.SMALLINT) long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.SMALLINT) long value, @SqlType(StandardTypes.SMALLINT) long min, @SqlType(StandardTypes.SMALLINT) long max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType(StandardTypes.SMALLINT) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.SMALLINT) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.SMALLINT) long value)
    {
        try {
            return SignedBytes.checkedCast(value);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for tinyint: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType(StandardTypes.SMALLINT) long value)
    {
        return value != 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType(StandardTypes.SMALLINT) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static long castToReal(@SqlType(StandardTypes.SMALLINT) long value)
    {
        return (long) floatToRawIntBits((float) value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castToVarchar(@SqlType(StandardTypes.SMALLINT) long value)
    {
        // todo optimize me
        return utf8Slice(valueOf(value));
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.SMALLINT) long value)
    {
        return (short) value;
    }
}
