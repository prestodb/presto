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

public class NativeMapSqlFunctions
{
    private NativeMapSqlFunctions() {}

    @SqlInvokedScalarFunction(value = "map_top_n_keys", deterministic = true, calledOnNullInput = true)
    @Description("Returns the top N keys of the given map sorting its keys using the provided lambda comparator.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "n", type = "bigint"), @SqlParameter(name = "f", type = "function(K, K, bigint)")})
    @SqlType("array<K>")
    public static String mapTopNKeysComparator()
    {
        return "RETURN IF(n < 0, fail('n must be greater than or equal to 0'), slice(reverse(array_sort(map_keys(input), f)), 1, n))";
    }

    @SqlInvokedScalarFunction(value = "map_top_n_values", deterministic = true, calledOnNullInput = true)
    @Description("Returns the top N values of the given map sorted using the provided lambda comparator.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "n", type = "bigint"), @SqlParameter(name = "f", type = "function(V, V, bigint)")})
    @SqlType("array<V>")
    public static String mapTopNValuesComparator()
    {
        return "RETURN IF(n < 0, fail('n must be greater than or equal to 0'), slice(reverse(array_sort(remove_nulls(map_values(input)), f)) || filter(map_values(input), x -> x is null), 1, n))";
    }
}
