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

import io.airlift.slice.Slice;
import io.prestosql.client.IntervalYearMonth;
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
import static io.prestosql.spi.function.OperatorType.MULTIPLY;
import static io.prestosql.spi.function.OperatorType.NEGATION;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.function.OperatorType.SUBTRACT;
import static io.prestosql.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
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
        return left + right;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long subtract(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left - right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long multiplyByBigint(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left * right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long multiplyByDouble(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return (long) (left * right);
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long bigintMultiply(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left * right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long doubleMultiply(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return (long) (left * right);
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long divideByDouble(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return (long) (left / right);
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
