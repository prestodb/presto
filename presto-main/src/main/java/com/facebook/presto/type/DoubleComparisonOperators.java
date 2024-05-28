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
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TypeUtils.doubleCompare;
import static com.facebook.presto.common.type.TypeUtils.doubleEquals;
import static com.facebook.presto.common.type.TypeUtils.doubleHashCode;

public class DoubleComparisonOperators
{
    private DoubleComparisonOperators() {}

    @ScalarOperator(EQUAL)
    @SuppressWarnings("FloatingPointEquality")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return doubleEquals(left, right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SuppressWarnings("FloatingPointEquality")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return !equal(left, right);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return doubleCompare(left, right) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return doubleCompare(left, right) <= 0;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return doubleCompare(left, right) == 1;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return doubleCompare(left, right) >= 0;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double min, @SqlType(StandardTypes.DOUBLE) double max)
    {
        return lessThanOrEqual(min, value) && lessThanOrEqual(value, max);
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return doubleHashCode(value);
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class DoubleDistinctFromOperator
    {
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

        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = StandardTypes.DOUBLE, nativeContainerType = double.class) Block leftBlock,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = StandardTypes.DOUBLE, nativeContainerType = double.class) Block rightBlock,
                @BlockIndex int rightPosition)
        {
            if (leftBlock.isNull(leftPosition) != rightBlock.isNull(rightPosition)) {
                return true;
            }
            if (leftBlock.isNull(leftPosition)) {
                return false;
            }
            double left = DOUBLE.getDouble(leftBlock, leftPosition);
            double right = DOUBLE.getDouble(rightBlock, rightPosition);
            return notEqual(left, right);
        }
    }
}
