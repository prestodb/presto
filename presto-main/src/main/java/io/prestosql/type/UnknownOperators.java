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

public final class UnknownOperators
{
    private UnknownOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType("unknown") boolean left, @SqlType("unknown") boolean right)
    {
        throw new AssertionError("value of unknown type should all be NULL");
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType("unknown") boolean left, @SqlType("unknown") boolean right)
    {
        throw new AssertionError("value of unknown type should all be NULL");
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType("unknown") boolean left, @SqlType("unknown") boolean right)
    {
        throw new AssertionError("value of unknown type should all be NULL");
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType("unknown") boolean left, @SqlType("unknown") boolean right)
    {
        throw new AssertionError("value of unknown type should all be NULL");
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType("unknown") boolean left, @SqlType("unknown") boolean right)
    {
        throw new AssertionError("value of unknown type should all be NULL");
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType("unknown") boolean left, @SqlType("unknown") boolean right)
    {
        throw new AssertionError("value of unknown type should all be NULL");
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType("unknown") boolean value, @SqlType("unknown") boolean min, @SqlType("unknown") boolean max)
    {
        throw new AssertionError("value of unknown type should all be NULL");
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType("unknown") boolean value)
    {
        throw new AssertionError("value of unknown type should all be NULL");
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class UnknownDistinctFromOperator
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @SqlType("unknown") boolean left,
                @IsNull boolean leftNull,
                @SqlType("unknown") boolean right,
                @IsNull boolean rightNull)
        {
            return false;
        }

        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = "unknown", nativeContainerType = boolean.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = "unknown", nativeContainerType = boolean.class) Block right,
                @BlockIndex int rightNull)
        {
            return false;
        }
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType("unknown") @SqlNullable Boolean value)
    {
        return true;
    }
}
