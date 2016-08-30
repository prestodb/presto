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

import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;

public final class UnknownOperators
{
    private UnknownOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean equal(@SqlType("unknown") @SqlNullable Void left, @SqlType("unknown") @SqlNullable Void right)
    {
        return null;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean notEqual(@SqlType("unknown") @SqlNullable Void left, @SqlType("unknown") @SqlNullable Void right)
    {
        return null;
    }

    @ScalarOperator(LESS_THAN)
    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean lessThan(@SqlType("unknown") @SqlNullable Void left, @SqlType("unknown") @SqlNullable Void right)
    {
        return null;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean lessThanOrEqual(@SqlType("unknown") @SqlNullable Void left, @SqlType("unknown") @SqlNullable Void right)
    {
        return null;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean greaterThan(@SqlType("unknown") @SqlNullable Void left, @SqlType("unknown") @SqlNullable Void right)
    {
        return null;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean greaterThanOrEqual(@SqlType("unknown") @SqlNullable Void left, @SqlType("unknown") @SqlNullable Void right)
    {
        return null;
    }

    @ScalarOperator(BETWEEN)
    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean between(@SqlType("unknown") @SqlNullable Void value, @SqlType("unknown") @SqlNullable Void min, @SqlType("unknown") @SqlNullable Void max)
    {
        return null;
    }

    @ScalarOperator(HASH_CODE)
    @SqlNullable
    @SqlType(StandardTypes.BIGINT)
    public static Long hashCode(@SqlType("unknown") @SqlNullable Void value)
    {
        return null;
    }
}
