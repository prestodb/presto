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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.function.LiteralParameters;
import com.facebook.presto.common.function.ScalarFunction;
import com.facebook.presto.common.function.SqlNullable;
import com.facebook.presto.common.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import io.airlift.slice.Slice;

public final class CustomFunctions
{
    private CustomFunctions() {}

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long customAdd(@SqlType(StandardTypes.BIGINT) long x, @SqlType(StandardTypes.BIGINT) long y)
    {
        return x + y;
    }

    @ScalarFunction(value = "custom_is_null", calledOnNullInput = true)
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean customIsNullVarchar(@SqlNullable @SqlType("varchar(x)") Slice slice)
    {
        return slice == null;
    }

    @ScalarFunction(value = "custom_is_null", calledOnNullInput = true)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean customIsNullBigint(@SqlNullable @SqlType(StandardTypes.BIGINT) Long value)
    {
        return value == null;
    }
}
