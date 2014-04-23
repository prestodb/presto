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
    public static boolean equal(boolean left, boolean right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    public static boolean notEqual(boolean left, boolean right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    public static boolean lessThan(boolean left, boolean right)
    {
        return !left && right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    public static boolean lessThanOrEqual(boolean left, boolean right)
    {
        return !left || right;
    }

    @ScalarOperator(GREATER_THAN)
    public static boolean greaterThan(boolean left, boolean right)
    {
        return left && !right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    public static boolean greaterThanOrEqual(boolean left, boolean right)
    {
        return left || !right;
    }

    @ScalarOperator(BETWEEN)
    public static boolean between(boolean value, boolean min, boolean max)
    {
        return (value && max) || (!value && !min);
    }

    @ScalarOperator(CAST)
    public static double castToDouble(boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    public static long castToBigint(boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    public static Slice castToVarchar(boolean value)
    {
        return value ? TRUE : FALSE;
    }

    @ScalarOperator(HASH_CODE)
    public static int hashCode(boolean value)
    {
        return value ? 1231 : 1237;
    }
}
