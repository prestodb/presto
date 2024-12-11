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

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.function.OperatorType.DIVIDE;
import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.function.OperatorType.MODULUS;
import static com.facebook.presto.common.function.OperatorType.MULTIPLY;
import static com.facebook.presto.common.function.OperatorType.NEGATION;
import static com.facebook.presto.common.function.OperatorType.SATURATED_FLOOR_CAST;
import static com.facebook.presto.common.function.OperatorType.SUBTRACT;
import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.math.RoundingMode.FLOOR;
import static java.math.RoundingMode.HALF_UP;

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
        try {
            return DoubleMath.roundToLong(intBitsToFloat((int) value), HALF_UP);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Unable to cast %s to bigint", intBitsToFloat((int) value)), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.REAL) long value)
    {
        try {
            return DoubleMath.roundToInt(intBitsToFloat((int) value), HALF_UP);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Unable to cast %s to integer", intBitsToFloat((int) value)), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.REAL) long value)
    {
        try {
            return Shorts.checkedCast(DoubleMath.roundToInt(intBitsToFloat((int) value), HALF_UP));
        }
        catch (ArithmeticException | IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Unable to cast %s to smallint", intBitsToFloat((int) value)), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.REAL) long value)
    {
        try {
            return SignedBytes.checkedCast(DoubleMath.roundToInt(intBitsToFloat((int) value), HALF_UP));
        }
        catch (ArithmeticException | IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Unable to cast %s to tinyint", intBitsToFloat((int) value)), e);
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
