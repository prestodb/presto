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
package com.facebook.presto.operator.scalar.sql;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.SqlInvokedScalarFunction;
import com.facebook.presto.spi.function.SqlParameter;
import com.facebook.presto.spi.function.SqlType;

public class ArrayArithmeticFunctions
{
    private ArrayArithmeticFunctions() {}

    @SqlInvokedScalarFunction(value = "array_sum", deterministic = true, calledOnNullInput = false)
    @Description("Returns the sum of all array elements, or 0 if the array is empty. Ignores null elements.")
    @SqlParameter(name = "input", type = "array<bigint>")
    @SqlType("bigint")
    public static String arraySumBigint()
    {
        return "RETURN reduce(input, BIGINT '0', (s, x) -> s + coalesce(x, BIGINT '0'), s -> s)";
    }

    @SqlInvokedScalarFunction(value = "array_sum", deterministic = true, calledOnNullInput = false)
    @Description("Returns the sum of all array elements, or 0 if the array is empty. Ignores null elements.")
    @SqlParameter(name = "input", type = "array<double>")
    @SqlType("double")
    public static String arraySumDouble()
    {
        return "RETURN reduce(input, DOUBLE '0', (s, x) -> s + coalesce(x, DOUBLE '0'), s -> s)";
    }

    @SqlInvokedScalarFunction(value = "array_average", deterministic = true, calledOnNullInput = false)
    @Description("Returns the average of all array elements, or null if the array is empty. Ignores null elements.")
    @SqlParameter(name = "input", type = "array<double>")
    @SqlType("double")
    public static String arrayAverage()
    {
        return "RETURN reduce(" +
                "input, " +
                "(double '0.0', 0), " +
                "(s, x) -> IF(x IS NOT NULL, (s[1] + x, s[2] + 1), s), " +
                "s -> if(s[2] = 0, cast(null as double), s[1] / cast(s[2] as double)))";
    }
}
