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
package io.prestosql.plugin.geospatial;

import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.AbstractLongType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;

public final class BingTileOperators
{
    private BingTileOperators() {}

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(BingTileType.NAME) long left, @SqlType(BingTileType.NAME) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(BingTileType.NAME) long left, @SqlType(BingTileType.NAME) long right)
    {
        return left != right;
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @SqlType(BingTileType.NAME) long left,
            @IsNull boolean leftNull,
            @SqlType(BingTileType.NAME) long right,
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

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(BingTileType.NAME) long value)
    {
        return AbstractLongType.hash(value);
    }
}
