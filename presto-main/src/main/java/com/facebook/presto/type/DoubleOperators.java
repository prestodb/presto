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
    public static double add(double left, double right)
    {
        return left + right;
    }

    @ScalarOperator(SUBTRACT)
    public static double subtract(double left, double right)
    {
        return left - right;
    }

    @ScalarOperator(MULTIPLY)
    public static double multiply(double left, double right)
    {
        return left * right;
    }

    @ScalarOperator(DIVIDE)
    public static double divide(double left, double right)
    {
        try {
            return left / right;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(StandardErrorCode.DIVISION_BY_ZERO.toErrorCode(), e);
        }
    }

    @ScalarOperator(MODULUS)
    public static double modulus(double left, double right)
    {
        try {
            return left % right;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(StandardErrorCode.DIVISION_BY_ZERO.toErrorCode(), e);
        }
    }

    @ScalarOperator(NEGATION)
    public static double negate(double value)
    {
        return -value;
    }

    @ScalarOperator(EQUAL)
    @SuppressWarnings("FloatingPointEquality")
    public static boolean equal(double left, double right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SuppressWarnings("FloatingPointEquality")
    public static boolean notEqual(double left, double right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    public static boolean lessThan(double left, double right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    public static boolean lessThanOrEqual(double left, double right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    public static boolean greaterThan(double left, double right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    public static boolean greaterThanOrEqual(double left, double right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    public static boolean between(double value, double min, double max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    public static boolean castToBoolean(double value)
    {
        return value != 0;
    }

    @ScalarOperator(CAST)
    public static long castToLong(double value)
    {
        return (long) MathFunctions.round(value);
    }

    @ScalarOperator(CAST)
    public static Slice castToVarchar(double value)
    {
        return Slices.copiedBuffer(String.valueOf(value), UTF_8);
    }

    @ScalarOperator(HASH_CODE)
    public static int hashCode(double value)
    {
        long bits = doubleToLongBits(value);
        return (int) (bits ^ (bits >>> 32));
    }
}
