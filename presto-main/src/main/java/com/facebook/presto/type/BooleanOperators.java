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

import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
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
    public static boolean equal(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
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

    @SqlType(StandardTypes.BOOLEAN)
    @ScalarFunction(hidden = true) // TODO: this should not be callable from SQL
    public static boolean not(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return !value;
    }
}
