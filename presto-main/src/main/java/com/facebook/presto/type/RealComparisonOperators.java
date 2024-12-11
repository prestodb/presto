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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TypeUtils.realCompare;
import static com.facebook.presto.common.type.TypeUtils.realEquals;
import static com.facebook.presto.common.type.TypeUtils.realHashCode;
import static java.lang.Float.intBitsToFloat;

public final class RealComparisonOperators
{
    private RealComparisonOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return realEquals(intBitsToFloat((int) left), intBitsToFloat((int) right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return !realEquals(intBitsToFloat((int) left), intBitsToFloat((int) right));
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return realCompare(intBitsToFloat((int) left), intBitsToFloat((int) right)) == -1;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return realCompare(intBitsToFloat((int) left), intBitsToFloat((int) right)) <= 0;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return realCompare(intBitsToFloat((int) left), intBitsToFloat((int) right)) == 1;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.REAL) long left, @SqlType(StandardTypes.REAL) long right)
    {
        return realCompare(intBitsToFloat((int) left), intBitsToFloat((int) right)) >= 0;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.REAL) long min, @SqlType(StandardTypes.REAL) long max)
    {
        return lessThanOrEqual(min, value) && lessThanOrEqual(value, max);
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.REAL) long value)
    {
        return realHashCode(intBitsToFloat((int) value));
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
}
