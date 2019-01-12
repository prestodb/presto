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
import io.airlift.slice.XxHash64;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.BlockIndex;
import io.prestosql.spi.function.BlockPosition;
import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.function.OperatorType.BETWEEN;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.function.OperatorType.XX_HASH_64;

public final class VarbinaryOperators
{
    private VarbinaryOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        return !left.equals(right);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        return left.compareTo(right) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        return left.compareTo(right) <= 0;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        return left.compareTo(right) > 0;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        return left.compareTo(right) >= 0;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.VARBINARY) Slice value, @SqlType(StandardTypes.VARBINARY) Slice min, @SqlType(StandardTypes.VARBINARY) Slice max)
    {
        return min.compareTo(value) <= 0 && value.compareTo(max) <= 0;
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.VARBINARY) Slice value)
    {
        // This needs to match the hash function for VARBINARY blocks
        // (i.e. AstractVariableWidthBlock.hash(...))
        // TODO: we need to get rid of hash from Block and rely on HASH_CODE operators only
        return xxHash64(value);
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class VarbinaryDistinctFromOperator
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @SqlType(StandardTypes.VARBINARY) Slice left,
                @IsNull boolean leftNull,
                @SqlType(StandardTypes.VARBINARY) Slice right,
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
                @BlockPosition @SqlType(value = StandardTypes.VARBINARY, nativeContainerType = Slice.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = StandardTypes.VARBINARY, nativeContainerType = Slice.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
                return true;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }

            int leftLength = left.getSliceLength(leftPosition);
            int rightLength = right.getSliceLength(rightPosition);
            if (leftLength != rightLength) {
                return true;
            }
            return !left.equals(leftPosition, 0, right, rightPosition, 0, leftLength);
        }
    }

    @ScalarOperator(XX_HASH_64)
    @SqlType(StandardTypes.BIGINT)
    public static long xxHash64(@SqlType(StandardTypes.VARBINARY) Slice value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType(StandardTypes.VARBINARY) Slice value, @IsNull boolean isNull)
    {
        return isNull;
    }
}
