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
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

public class ArrayIntersectFunction
{
    private ArrayIntersectFunction() {}

    @SqlInvokedScalarFunction(value = "array_intersect", deterministic = true, calledOnNullInput = false)
    @Description("Intersects elements of all arrays in the given array")
    @TypeParameter("T")
    @SqlParameter(name = "input", type = "array<array<T>>")
    @SqlType("array<T>")
    public static String arrayIntersectArray()
    {
        return "RETURN reduce(input, IF((cardinality(input) = 0), ARRAY[], input[1]), (s, x) -> array_intersect(s, x), (s) -> s)";
    }
}
