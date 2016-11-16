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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.AbstractLongType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Ints;
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
import static com.facebook.presto.spi.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.MODULUS;
import static com.facebook.presto.spi.function.OperatorType.MULTIPLY;
import static com.facebook.presto.spi.function.OperatorType.NEGATION;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.SATURATED_FLOOR_CAST;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.valueOf;
import static java.math.RoundingMode.FLOOR;

public final class DoubleOperators
{
    private static final double MIN_LONG_AS_DOUBLE = -0x1p63;
    private static final double MAX_LONG_PLUS_ONE_AS_DOUBLE = 0x1p63;
    private static final double MIN_INTEGER_AS_DOUBLE = -0x1p31;
    private static final double MAX_INTEGER_PLUS_ONE_AS_DOUBLE = 0x1p31;
    private static final double MIN_SHORT_AS_DOUBLE = -0x1p15;
    private static final double MAX_SHORT_PLUS_ONE_AS_DOUBLE = 0x1p15;
    private static final double MIN_BYTE_AS_DOUBLE = -0x1p7;
    private static final double MAX_BYTE_PLUS_ONE_AS_DOUBLE = 0x1p7;

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
    @SqlType(StandardTypes.REAL)
    public static long castToReal(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return floatToRawIntBits((float) value);
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return utf8Slice(valueOf(value));
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return AbstractLongType.hash(doubleToLongBits(value));
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.REAL)
    public static strictfp long saturatedFloorCastToFloat(@SqlType(StandardTypes.DOUBLE) double value)
    {
        float result;
        float minFloat = -1.0f * Float.MAX_VALUE;
        if (value <= minFloat) {
            result = minFloat;
        }
        else if (value >= Float.MAX_VALUE) {
            result = Float.MAX_VALUE;
        }
        else {
            result = (float) value;
            if (result > value) {
                result = Math.nextDown(result);
            }
            checkState(result <= value);
        }
        return floatToRawIntBits(result);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long saturatedFloorCastToBigint(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return saturatedFloorCastToLong(value, Long.MIN_VALUE, MIN_LONG_AS_DOUBLE, Long.MAX_VALUE, MAX_LONG_PLUS_ONE_AS_DOUBLE);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long saturatedFloorCastToInteger(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return saturatedFloorCastToLong(value, Integer.MIN_VALUE, MIN_INTEGER_AS_DOUBLE, Integer.MAX_VALUE, MAX_INTEGER_PLUS_ONE_AS_DOUBLE);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long saturatedFloorCastToSmallint(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return saturatedFloorCastToLong(value, Short.MIN_VALUE, MIN_SHORT_AS_DOUBLE, Short.MAX_VALUE, MAX_SHORT_PLUS_ONE_AS_DOUBLE);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long saturatedFloorCastToTinyint(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return saturatedFloorCastToLong(value, Byte.MIN_VALUE, MIN_BYTE_AS_DOUBLE, Byte.MAX_VALUE, MAX_BYTE_PLUS_ONE_AS_DOUBLE);
    }

    private static long saturatedFloorCastToLong(double value, long minValue, double minValueAsDouble, long maxValue, double maxValuePlusOneAsDouble)
    {
        if (value <= minValueAsDouble) {
            return minValue;
        }
        if (value + 1 >= maxValuePlusOneAsDouble) {
            return maxValue;
        }
        return DoubleMath.roundToLong(value, FLOOR);
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @SqlType(StandardTypes.DOUBLE) double left,
            @IsNull boolean leftNull,
            @SqlType(StandardTypes.DOUBLE) double right,
            @IsNull boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        return notEqual(left, right);
    }
}
