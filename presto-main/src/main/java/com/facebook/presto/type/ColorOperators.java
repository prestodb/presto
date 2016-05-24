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

import com.facebook.presto.operator.scalar.annotations.ScalarOperator;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;

public final class ColorOperators
{
    private ColorOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType(ColorType.NAME) long left, @SqlType(ColorType.NAME) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(ColorType.NAME) long left, @SqlType(ColorType.NAME) long right)
    {
        return left != right;
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(ColorType.NAME) long value)
    {
        return (int) value;
    }
}
