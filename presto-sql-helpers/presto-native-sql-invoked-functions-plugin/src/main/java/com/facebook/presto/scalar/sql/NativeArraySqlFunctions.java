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
package com.facebook.presto.scalar.sql;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.SqlInvokedScalarFunction;
import com.facebook.presto.spi.function.SqlParameter;
import com.facebook.presto.spi.function.SqlParameters;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

public class NativeArraySqlFunctions
{
    private NativeArraySqlFunctions() {}

    @SqlInvokedScalarFunction(value = "array_top_n", deterministic = true, calledOnNullInput = true)
    @Description("Returns the top N values of the given map sorted using the provided lambda comparator.")
    @TypeParameter("T")
    @SqlParameters({@SqlParameter(name = "input", type = "array(T)"), @SqlParameter(name = "n", type = "int"), @SqlParameter(name = "f", type = "function(T, T, int)")})
    @SqlType("array<T>")
    public static String arrayTopNComparator()
    {
        return "RETURN IF(n < 0, fail('Parameter n: ' || cast(n as varchar) || ' to ARRAY_TOP_N is negative'), SLICE(REVERSE(ARRAY_SORT(input, f)), 1, n))";
    }
}
