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

import com.facebook.presto.client.IntervalYearMonth;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.AbstractIntType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.function.OperatorType.DIVIDE;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.MULTIPLY;
import static com.facebook.presto.common.function.OperatorType.NEGATION;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.function.OperatorType.SUBTRACT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.toIntExact;

public final class IntervalYearMonthOperators
{
    private IntervalYearMonthOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long add(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        try {
            return Math.addExact((int) left, (int) right);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Overflow adding interval year-month values: " + left + " + " + right);
        }
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long subtract(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        try {
            return Math.subtractExact((int) left, (int) right);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Overflow subtracting interval year-month values: " + left + " - " + right);
        }
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long multiplyByInteger(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        try {
            return Math.multiplyExact((int) left, (int) right);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Overflow multiplying interval year-month value by integer: " + left + " * " + right);
        }
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long multiplyByDouble(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        long result = (long) (left * right);
        if (result < Integer.MIN_VALUE || result > Integer.MAX_VALUE) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Overflow multiplying interval year-month value by double: " + left + " * " + right);
        }
        return result;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long integerMultiply(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        try {
            return Math.multiplyExact((int) left, (int) right);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Overflow multiplying integer by interval year-month value: " + left + " * " + right);
        }
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long doubleMultiply(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        long result = (long) (left * right);
        if (result < Integer.MIN_VALUE || result > Integer.MAX_VALUE) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Overflow multiplying double by interval year-month value: " + left + " * " + right);
        }
        return result;
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long divideByDouble(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        long result = (long) (left / right);
        if (result < Integer.MIN_VALUE || result > Integer.MAX_VALUE) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Overflow dividing interval year-month value by double: " + left + " / " + right);
        }
        return result;
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long negate(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long value)
    {
        return -value;
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long value,
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long min,
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToSlice(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long value)
    {
        return utf8Slice(IntervalYearMonth.formatMonths(toIntExact(value)));
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long value)
    {
        return AbstractIntType.hash((int) value);
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class IntervalYearMonthDistinctFromOperator
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left,
                @IsNull boolean leftNull,
                @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right,
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

        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = StandardTypes.INTERVAL_YEAR_TO_MONTH, nativeContainerType = long.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = StandardTypes.INTERVAL_YEAR_TO_MONTH, nativeContainerType = long.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
                return true;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }
            return notEqual(INTERVAL_YEAR_MONTH.getLong(left, leftPosition), INTERVAL_YEAR_MONTH.getLong(right, rightPosition));
        }
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long value, @IsNull boolean isNull)
    {
        return isNull;
    }
}
