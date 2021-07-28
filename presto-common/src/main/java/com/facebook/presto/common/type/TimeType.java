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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.ScalarOperator;
import com.facebook.presto.common.function.SqlFunctionProperties;
import io.airlift.slice.XxHash64;

import static com.facebook.presto.common.function.OperatorType.COMPARISON;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.common.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.lang.invoke.MethodHandles.lookup;

//
// A time is stored as milliseconds from midnight on 1970-01-01T00:00:00 in the time zone of the session.
// When performing calculations on a time the client's time zone must be taken into account.
//
public final class TimeType
        extends AbstractLongType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(TimeType.class, lookup(), long.class);

    public static final TimeType TIME = new TimeType();

    private TimeType()
    {
        super(parseTypeSignature(StandardTypes.TIME));
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (properties.isLegacyTimestamp()) {
            return new SqlTime(block.getLong(position), properties.getTimeZoneKey());
        }
        else {
            return new SqlTime(block.getLong(position));
        }
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == TIME;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @ScalarOperator(EQUAL)
    public static boolean equalOperator(long left, long right)
    {
        return left == right;
    }

    @ScalarOperator(HASH_CODE)
    public static long hashCodeOperator(long value)
    {
        return AbstractLongType.hash(value);
    }

    @ScalarOperator(XX_HASH_64)
    public static long xxHash64Operator(long value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(COMPARISON)
    private static long comparisonOperator(long left, long right)
    {
        return Long.compare(left, right);
    }

    @ScalarOperator(LESS_THAN)
    public static boolean lessThan(long left, long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    public static boolean lessThanOrEqual(long left, long right)
    {
        return left <= right;
    }
}
