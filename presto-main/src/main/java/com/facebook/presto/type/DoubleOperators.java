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

import com.facebook.presto.operator.scalar.MathFunctions;
import com.facebook.presto.operator.scalar.annotations.ScalarOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;

import static com.facebook.presto.metadata.OperatorType.ADD;
import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorType.DIVIDE;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.MODULUS;
import static com.facebook.presto.metadata.OperatorType.MULTIPLY;
import static com.facebook.presto.metadata.OperatorType.NEGATION;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.OperatorType.SATURATED_FLOOR_CAST;
import static com.facebook.presto.metadata.OperatorType.SUBTRACT;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.valueOf;
import static java.math.RoundingMode.FLOOR;

public final class DoubleOperators
{
    private static final double MIN_LONG_AS_DOUBLE = -0x1p63;
    private static final double MAX_LONG_AS_DOUBLE_PLUS_ONE = 0x1p63;
    private static final double MIN_INT_AS_DOUBLE = -0x1p31;
    private static final double MAX_INT_AS_DOUBLE = 0x1p31 - 1.0;

    private DoubleOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DOUBLE)
    public static double add(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left + right;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.DOUBLE)
    public static double subtract(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left - right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.DOUBLE)
    public static double multiply(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left * right;
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.DOUBLE)
    public static double divide(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        try {
            return left / right;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(DIVISION_BY_ZERO, e);
        }
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.DOUBLE)
    public static double modulus(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        try {
            return left % right;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(DIVISION_BY_ZERO, e);
        }
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.DOUBLE)
    public static double negate(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return -value;
    }

    @ScalarOperator(EQUAL)
    @SuppressWarnings("FloatingPointEquality")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SuppressWarnings("FloatingPointEquality")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double min, @SqlType(StandardTypes.DOUBLE) double max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return value != 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.DOUBLE) double value)
    {
        try {
            return Ints.checkedCast((long) MathFunctions.round(value));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for integer: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.DOUBLE) double value)
    {
        try {
            return Shorts.checkedCast((long) MathFunctions.round(value));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for smallint: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.DOUBLE) double value)
    {
        try {
            return SignedBytes.checkedCast((long) MathFunctions.round(value));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for tinyint: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToLong(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return (long) MathFunctions.round(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.FLOAT)
    public static long castToFloat(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return floatToRawIntBits((float) value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castToVarchar(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return utf8Slice(valueOf(value));
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return doubleToLongBits(value);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long saturatedFloorCastToBigint(@SqlType(StandardTypes.DOUBLE) double value)
    {
        if (value <= MIN_LONG_AS_DOUBLE) {
            return Long.MIN_VALUE;
        }
        if (MAX_LONG_AS_DOUBLE_PLUS_ONE - value <= 1) {
            return Long.MAX_VALUE;
        }
        return DoubleMath.roundToLong(value, FLOOR);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long saturatedFloorCastToInteger(@SqlType(StandardTypes.DOUBLE) double value)
    {
        if (value <= MIN_INT_AS_DOUBLE) {
            return Integer.MIN_VALUE;
        }
        if (MAX_INT_AS_DOUBLE - value <= 1) {
            return Integer.MAX_VALUE;
        }
        return DoubleMath.roundToInt(value, FLOOR);
    }
}
