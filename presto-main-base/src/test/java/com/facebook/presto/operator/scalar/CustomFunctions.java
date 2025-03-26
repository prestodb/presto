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

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlParameter;
import com.facebook.presto.spi.function.SqlParameters;
import com.facebook.presto.spi.function.SqlType;
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

    @SqlInvokedScalarFunction(value = "custom_square", deterministic = true, calledOnNullInput = false)
    @Description("Custom SQL to test NULLIF in Functions")
    @SqlParameters({@SqlParameter(name = "x", type = "integer"), @SqlParameter(name = "y", type = "integer")})
    @SqlType("integer")
    public static String customSquare()
    {
        return "RETURN IF(NULLIF(x, y) IS NOT NULL, x * x, y * y)";
    }
}
