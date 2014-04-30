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
import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.metadata.OperatorInfo.OperatorType.ADD;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.DIVIDE;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.MODULUS;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.MULTIPLY;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.NEGATION;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.SUBTRACT;
import static java.lang.Double.doubleToLongBits;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DoubleOperators
{
    private DoubleOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(DoubleType.class)
    public static double add(@SqlType(DoubleType.class) double left, @SqlType(DoubleType.class) double right)
    {
        return left + right;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(DoubleType.class)
    public static double subtract(@SqlType(DoubleType.class) double left, @SqlType(DoubleType.class) double right)
    {
        return left - right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(DoubleType.class)
    public static double multiply(@SqlType(DoubleType.class) double left, @SqlType(DoubleType.class) double right)
    {
        return left * right;
    }

    @ScalarOperator(DIVIDE)
    @SqlType(DoubleType.class)
    public static double divide(@SqlType(DoubleType.class) double left, @SqlType(DoubleType.class) double right)
    {
        try {
            return left / right;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(StandardErrorCode.DIVISION_BY_ZERO.toErrorCode(), e);
        }
    }

    @ScalarOperator(MODULUS)
    @SqlType(DoubleType.class)
    public static double modulus(@SqlType(DoubleType.class) double left, @SqlType(DoubleType.class) double right)
    {
        try {
            return left % right;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(StandardErrorCode.DIVISION_BY_ZERO.toErrorCode(), e);
        }
    }

    @ScalarOperator(NEGATION)
    @SqlType(DoubleType.class)
    public static double negate(@SqlType(DoubleType.class) double value)
    {
        return -value;
    }

    @ScalarOperator(EQUAL)
    @SuppressWarnings("FloatingPointEquality")
    @SqlType(BooleanType.class)
    public static boolean equal(@SqlType(DoubleType.class) double left, @SqlType(DoubleType.class) double right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SuppressWarnings("FloatingPointEquality")
    @SqlType(BooleanType.class)
    public static boolean notEqual(@SqlType(DoubleType.class) double left, @SqlType(DoubleType.class) double right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(BooleanType.class)
    public static boolean lessThan(@SqlType(DoubleType.class) double left, @SqlType(DoubleType.class) double right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean lessThanOrEqual(@SqlType(DoubleType.class) double left, @SqlType(DoubleType.class) double right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(BooleanType.class)
    public static boolean greaterThan(@SqlType(DoubleType.class) double left, @SqlType(DoubleType.class) double right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean greaterThanOrEqual(@SqlType(DoubleType.class) double left, @SqlType(DoubleType.class) double right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(BooleanType.class)
    public static boolean between(@SqlType(DoubleType.class) double value, @SqlType(DoubleType.class) double min, @SqlType(DoubleType.class) double max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    @SqlType(BooleanType.class)
    public static boolean castToBoolean(@SqlType(DoubleType.class) double value)
    {
        return value != 0;
    }

    @ScalarOperator(CAST)
    @SqlType(BigintType.class)
    public static long castToLong(@SqlType(DoubleType.class) double value)
    {
        return (long) MathFunctions.round(value);
    }

    @ScalarOperator(CAST)
    @SqlType(VarcharType.class)
    public static Slice castToVarchar(@SqlType(DoubleType.class) double value)
    {
        return Slices.copiedBuffer(String.valueOf(value), UTF_8);
    }

    @ScalarOperator(HASH_CODE)
    public static int hashCode(@SqlType(DoubleType.class) double value)
    {
        long bits = doubleToLongBits(value);
        return (int) (bits ^ (bits >>> 32));
    }
}
