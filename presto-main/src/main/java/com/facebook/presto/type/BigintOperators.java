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

import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

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
import static com.facebook.presto.metadata.OperatorType.SUBTRACT;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class BigintOperators
{
    private BigintOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(BigintType.class)
    public static long add(@SqlType(BigintType.class) long left, @SqlType(BigintType.class) long right)
    {
        return left + right;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(BigintType.class)
    public static long subtract(@SqlType(BigintType.class) long left, @SqlType(BigintType.class) long right)
    {
        return left - right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(BigintType.class)
    public static long multiply(@SqlType(BigintType.class) long left, @SqlType(BigintType.class) long right)
    {
        return left * right;
    }

    @ScalarOperator(DIVIDE)
    @SqlType(BigintType.class)
    public static long divide(@SqlType(BigintType.class) long left, @SqlType(BigintType.class) long right)
    {
        try {
            return left / right;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(StandardErrorCode.DIVISION_BY_ZERO.toErrorCode(), e);
        }
    }

    @ScalarOperator(MODULUS)
    @SqlType(BigintType.class)
    public static long modulus(@SqlType(BigintType.class) long left, @SqlType(BigintType.class) long right)
    {
        try {
            return left % right;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(StandardErrorCode.DIVISION_BY_ZERO.toErrorCode(), e);
        }
    }

    @ScalarOperator(NEGATION)
    @SqlType(BigintType.class)
    public static long negate(@SqlType(BigintType.class) long value)
    {
        return -value;
    }

    @ScalarOperator(EQUAL)
    @SqlType(BooleanType.class)
    public static boolean equal(@SqlType(BigintType.class) long left, @SqlType(BigintType.class) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean notEqual(@SqlType(BigintType.class) long left, @SqlType(BigintType.class) long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(BooleanType.class)
    public static boolean lessThan(@SqlType(BigintType.class) long left, @SqlType(BigintType.class) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean lessThanOrEqual(@SqlType(BigintType.class) long left, @SqlType(BigintType.class) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(BooleanType.class)
    public static boolean greaterThan(@SqlType(BigintType.class) long left, @SqlType(BigintType.class) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean greaterThanOrEqual(@SqlType(BigintType.class) long left, @SqlType(BigintType.class) long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(BooleanType.class)
    public static boolean between(@SqlType(BigintType.class) long value, @SqlType(BigintType.class) long min, @SqlType(BigintType.class) long max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    @SqlType(BooleanType.class)
    public static boolean castToBoolean(@SqlType(BigintType.class) long value)
    {
        return value != 0;
    }

    @ScalarOperator(CAST)
    @SqlType(DoubleType.class)
    public static double castToDouble(@SqlType(BigintType.class) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(VarcharType.class)
    public static Slice castToVarchar(@SqlType(BigintType.class) long value)
    {
        // todo optimize me
        return Slices.copiedBuffer(String.valueOf(value), UTF_8);
    }

    @ScalarOperator(HASH_CODE)
    public static int hashCode(@SqlType(BigintType.class) long value)
    {
        return (int) (value ^ (value >>> 32));
    }
}
