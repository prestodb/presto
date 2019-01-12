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
package io.prestosql.type;

import com.google.common.math.DoubleMath;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import io.prestosql.operator.scalar.MathFunctions;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.BlockIndex;
import io.prestosql.spi.function.BlockPosition;
import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.AbstractIntType;
import io.prestosql.spi.type.StandardTypes;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.prestosql.spi.function.OperatorType.ADD;
import static io.prestosql.spi.function.OperatorType.BETWEEN;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.function.OperatorType.DIVIDE;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.MODULUS;
import static io.prestosql.spi.function.OperatorType.MULTIPLY;
import static io.prestosql.spi.function.OperatorType.NEGATION;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.function.OperatorType.SATURATED_FLOOR_CAST;
import static io.prestosql.spi.function.OperatorType.SUBTRACT;
import static io.prestosql.spi.function.OperatorType.XX_HASH_64;
import static io.prestosql.spi.type.RealType.REAL;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.FLOOR;

public final class RealOperators
{
    private static final float MIN_LONG_AS_FLOAT = -0x1p63f;
    private static final float MAX_LONG_PLUS_ONE_AS_FLOAT = 0x1p63f;
    private static final float MIN_INTEGER_AS_FLOAT = -0x1p31f;
    private static final float MAX_INTEGER_PLUS_ONE_AS_FLOAT = 0x1p31f;
    private static final float MIN_SHORT_AS_FLOAT = -0x1p15f;
    private static final float MAX_SHORT_PLUS_ONE_AS_FLOAT = 0x1p15f;
    private static final float MIN_BYTE_AS_FLOAT = -0x1p7f;
    private static final float MAX_BYTE_PLUS_ONE_AS_FLOAT = 0x1p7f;

    private RealOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.REAL)
    public static long add(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) + intBitsToFloat((int) right));
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.REAL)
    public static long subtract(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) - intBitsToFloat((int) right));
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.REAL)
    public static long multiply(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) * intBitsToFloat((int) right));
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.REAL)
    public static long divide(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) / intBitsToFloat((int) right));
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.REAL)
    public static long modulus(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) % intBitsToFloat((int) right));
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.REAL)
    public static long negate(@SqlType(StandardTypes.REAL) long value)
    {
        return floatToRawIntBits(-intBitsToFloat((int) value));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return intBitsToFloat((int) left) == intBitsToFloat((int) right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return intBitsToFloat((int) left) != intBitsToFloat((int) right);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return intBitsToFloat((int) left) < intBitsToFloat((int) right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return intBitsToFloat((int) left) <= intBitsToFloat((int) right);
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return intBitsToFloat((int) left) > intBitsToFloat((int) right);
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return intBitsToFloat((int) left) >= intBitsToFloat((int) right);
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.REAL) long min, @SqlType(StandardTypes.REAL) long max)
    {
        return intBitsToFloat((int) min) <= intBitsToFloat((int) value) &&
                intBitsToFloat((int) value) <= intBitsToFloat((int) max);
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.REAL) long value)
    {
        return AbstractIntType.hash(floatToIntBits(intBitsToFloat((int) value)));
    }

    @ScalarOperator(XX_HASH_64)
    @SqlType(StandardTypes.BIGINT)
    public static long xxHash64(@SqlType(StandardTypes.REAL) long value)
    {
        return XxHash64.hash(floatToIntBits(intBitsToFloat((int) value)));
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@SqlType(StandardTypes.REAL) long value)
    {
        return utf8Slice(String.valueOf(intBitsToFloat((int) value)));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToLong(@SqlType(StandardTypes.REAL) long value)
    {
        return (long) MathFunctions.round((double) intBitsToFloat((int) value));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.REAL) long value)
    {
        try {
            return toIntExact((long) MathFunctions.round((double) intBitsToFloat((int) value)));
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for integer: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.REAL) long value)
    {
        try {
            return Shorts.checkedCast((long) MathFunctions.round((double) intBitsToFloat((int) value)));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for smallint: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.REAL) long value)
    {
        try {
            return SignedBytes.checkedCast((long) MathFunctions.round((double) intBitsToFloat((int) value)));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for tinyint: " + value, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType(StandardTypes.REAL) long value)
    {
        return (double) intBitsToFloat((int) value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType(StandardTypes.REAL) long value)
    {
        return intBitsToFloat((int) value) != 0.0f;
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class RealDistinctFromOperator
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @SqlType(StandardTypes.REAL) long left,
                @IsNull boolean leftNull,
                @SqlType(StandardTypes.REAL) long right,
                @IsNull boolean rightNull)
        {
            if (leftNull != rightNull) {
                return true;
            }
            if (leftNull) {
                return false;
            }
            float leftFloat = intBitsToFloat((int) left);
            float rightFloat = intBitsToFloat((int) right);
            if (Float.isNaN(leftFloat) && Float.isNaN(rightFloat)) {
                return false;
            }
            return notEqual(left, right);
        }

        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = StandardTypes.REAL, nativeContainerType = long.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = StandardTypes.REAL, nativeContainerType = long.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
                return true;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }
            return notEqual(REAL.getLong(left, leftPosition), REAL.getLong(right, rightPosition));
        }
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long saturatedFloorCastToSmallint(@SqlType(StandardTypes.REAL) long value)
    {
        return saturatedFloorCastToLong(value, Short.MIN_VALUE, MIN_SHORT_AS_FLOAT, Short.MAX_VALUE, MAX_SHORT_PLUS_ONE_AS_FLOAT);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long saturatedFloorCastToTinyint(@SqlType(StandardTypes.REAL) long value)
    {
        return saturatedFloorCastToLong(value, Byte.MIN_VALUE, MIN_BYTE_AS_FLOAT, Byte.MAX_VALUE, MAX_BYTE_PLUS_ONE_AS_FLOAT);
    }

    private static long saturatedFloorCastToLong(long valueBits, long minValue, float minValueAsDouble, long maxValue, float maxValuePlusOneAsDouble)
    {
        float value = intBitsToFloat((int) valueBits);
        if (value <= minValueAsDouble) {
            return minValue;
        }
        if (value + 1 >= maxValuePlusOneAsDouble) {
            return maxValue;
        }
        return DoubleMath.roundToLong(value, FLOOR);
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType(StandardTypes.REAL) long value, @IsNull boolean isNull)
    {
        return isNull;
    }
}
