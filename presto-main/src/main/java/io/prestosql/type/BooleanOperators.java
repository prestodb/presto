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
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.BlockIndex;
import io.prestosql.spi.function.BlockPosition;
import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.function.OperatorType.BETWEEN;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static java.lang.Float.floatToRawIntBits;
import static java.nio.charset.StandardCharsets.US_ASCII;

public final class BooleanOperators
{
    private static final Slice TRUE = Slices.copiedBuffer("true", US_ASCII);
    private static final Slice FALSE = Slices.copiedBuffer("false", US_ASCII);

    private BooleanOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return !left && right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return !left || right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return left && !right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return left || !right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.BOOLEAN) boolean value, @SqlType(StandardTypes.BOOLEAN) boolean min, @SqlType(StandardTypes.BOOLEAN) boolean max)
    {
        return (value && max) || (!value && !min);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static long castToReal(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? floatToRawIntBits(1.0f) : floatToRawIntBits(0.0f);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? TRUE : FALSE;
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1231 : 1237;
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType(StandardTypes.BOOLEAN) boolean value, @IsNull boolean isNull)
    {
        return isNull;
    }

    @SqlType(StandardTypes.BOOLEAN)
    @ScalarFunction(hidden = true) // TODO: this should not be callable from SQL
    public static boolean not(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return !value;
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class BooleanDistinctFromOperator
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @SqlType(StandardTypes.BOOLEAN) boolean left,
                @IsNull boolean leftNull,
                @SqlType(StandardTypes.BOOLEAN) boolean right,
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
                @BlockPosition @SqlType(value = StandardTypes.BOOLEAN, nativeContainerType = boolean.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = StandardTypes.BOOLEAN, nativeContainerType = boolean.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
                return true;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }
            return notEqual(BOOLEAN.getBoolean(left, leftPosition), BOOLEAN.getBoolean(right, rightPosition));
        }
    }
}
