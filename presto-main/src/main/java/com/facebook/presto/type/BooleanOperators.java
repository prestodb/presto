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
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.metadata.OperatorInfo.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.NOT_EQUAL;
import static java.nio.charset.StandardCharsets.US_ASCII;

public final class BooleanOperators
{
    private static final Slice TRUE = Slices.copiedBuffer("true", US_ASCII);
    private static final Slice FALSE = Slices.copiedBuffer("false", US_ASCII);

    private BooleanOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(BooleanType.class)
    public static boolean equal(@SqlType(BooleanType.class) boolean left, @SqlType(BooleanType.class) boolean right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean notEqual(@SqlType(BooleanType.class) boolean left, @SqlType(BooleanType.class) boolean right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(BooleanType.class)
    public static boolean lessThan(@SqlType(BooleanType.class) boolean left, @SqlType(BooleanType.class) boolean right)
    {
        return !left && right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean lessThanOrEqual(@SqlType(BooleanType.class) boolean left, @SqlType(BooleanType.class) boolean right)
    {
        return !left || right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(BooleanType.class)
    public static boolean greaterThan(@SqlType(BooleanType.class) boolean left, @SqlType(BooleanType.class) boolean right)
    {
        return left && !right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean greaterThanOrEqual(@SqlType(BooleanType.class) boolean left, @SqlType(BooleanType.class) boolean right)
    {
        return left || !right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(BooleanType.class)
    public static boolean between(@SqlType(BooleanType.class) boolean value, @SqlType(BooleanType.class) boolean min, @SqlType(BooleanType.class) boolean max)
    {
        return (value && max) || (!value && !min);
    }

    @ScalarOperator(CAST)
    @SqlType(DoubleType.class)
    public static double castToDouble(@SqlType(BooleanType.class) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(BigintType.class)
    public static long castToBigint(@SqlType(BooleanType.class) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(VarcharType.class)
    public static Slice castToVarchar(@SqlType(BooleanType.class) boolean value)
    {
        return value ? TRUE : FALSE;
    }

    @ScalarOperator(HASH_CODE)
    public static int hashCode(@SqlType(BooleanType.class) boolean value)
    {
        return value ? 1231 : 1237;
    }
}
