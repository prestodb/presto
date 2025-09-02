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

public class MapSqlFunctions
{
    private MapSqlFunctions() {}

    @SqlInvokedScalarFunction(value = "map_keys_by_top_n_values", deterministic = true, calledOnNullInput = false)
    @Description("Returns the top N keys of the given map in descending order according to the natural ordering of its values.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "n", type = "bigint")})
    @SqlType("array<K>")
    public static String mapKeysByTopNValues()
    {
        return "RETURN IF(n < 0, fail('n must be greater than or equal to 0'), map_keys(map_top_n(input, n)))";
    }

    @SqlInvokedScalarFunction(value = "map_key_exists", deterministic = true, calledOnNullInput = false)
    @Description("Returns whether a given key exists in a map.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "k", type = "K")})
    @SqlType("boolean")
    public static String mapKeysExists()
    {
        return "RETURN CONTAINS(MAP_KEYS(input), k)";
    }

    @SqlInvokedScalarFunction(value = "map_top_n", deterministic = true, calledOnNullInput = true)
    @Description("Truncates map items. Keeps only the top N elements by value.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "n", type = "bigint")})
    @SqlType("map(K, V)")
    public static String mapTopN()
    {
        return "RETURN IF(n < 0, fail('n must be greater than or equal to 0'), map_from_entries(slice(array_sort(map_entries(map_filter(input, (k, v) -> v is not null)), (x, y) -> IF(x[2] < y[2], 1, IF(x[2] = y[2], IF(x[1] < y[1], 1, -1), -1))) "
                + "|| ARRAY_SORT(MAP_ENTRIES(MAP_FILTER(input, (k, v) -> v IS NULL)), (x, y) -> IF( x[1] < y[1],  1, -1)),  1, n)))";
    }

    @SqlInvokedScalarFunction(value = "map_top_n_keys", deterministic = true, calledOnNullInput = false)
    @Description("Returns the top N keys of the given map by sorting the keys in descending order according to the natural ordering of its keys.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "n", type = "bigint")})
    @SqlType("array<K>")
    public static String mapTopNKeys()
    {
        return "RETURN IF(n < 0, fail('n must be greater than or equal to 0'), slice(reverse(array_sort(map_keys(input))), 1, n))";
    }

    @SqlInvokedScalarFunction(value = "map_top_n_keys", deterministic = true, calledOnNullInput = true)
    @Description("Returns the top N keys of the given map sorting its keys using the provided lambda comparator.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "n", type = "bigint"), @SqlParameter(name = "f", type = "function(K, K, int)")})
    @SqlType("array<K>")
    public static String mapTopNKeysComparator()
    {
        return "RETURN IF(n < 0, fail('n must be greater than or equal to 0'), slice(reverse(array_sort(map_keys(input), f)), 1, n))";
    }

    @SqlInvokedScalarFunction(value = "map_top_n_values", deterministic = true, calledOnNullInput = false)
    @Description("Returns the top N values of the given map in descending order according to the natural ordering of its values.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "n", type = "bigint")})
    @SqlType("array<V>")
    public static String mapTopNValues()
    {
        return "RETURN IF(n < 0, fail('n must be greater than or equal to 0'), slice(array_sort_desc(map_values(input)), 1, n))";
    }

    @SqlInvokedScalarFunction(value = "map_top_n_values", deterministic = true, calledOnNullInput = true)
    @Description("Returns the top N values of the given map sorted using the provided lambda comparator.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "n", type = "bigint"), @SqlParameter(name = "f", type = "function(V, V, int)")})
    @SqlType("array<V>")
    public static String mapTopNValuesComparator()
    {
        return "RETURN IF(n < 0, fail('n must be greater than or equal to 0'), slice(reverse(array_sort(remove_nulls(map_values(input)), f)) || filter(map_values(input), x -> x is null), 1, n))";
    }

    @SqlInvokedScalarFunction(value = "map_remove_null_values", deterministic = true, calledOnNullInput = true)
    @Description("Constructs a map by removing all the keys with null values.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameter(name = "input", type = "map(K, V)")
    @SqlType("map(K, V)")
    public static String mapRemoveNulls()
    {
        return "RETURN map_filter(input, (k, v) -> v is not null)";
    }

    @SqlInvokedScalarFunction(value = "all_keys_match", deterministic = true, calledOnNullInput = true)
    @Description("Returns whether all keys of a map match the given predicate.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "f", type = "function(K, boolean)")})
    @SqlType("boolean")
    public static String allKeysMatch()
    {
        return "RETURN ALL_MATCH(MAP_KEYS(input), f)";
    }

    @SqlInvokedScalarFunction(value = "any_keys_match", deterministic = true, calledOnNullInput = true)
    @Description("Returns whether any key of a map matches the given predicate.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "f", type = "function(K, boolean)")})
    @SqlType("boolean")
    public static String anyKeysMatch()
    {
        return "RETURN ANY_MATCH(MAP_KEYS(input), f)";
    }

    @SqlInvokedScalarFunction(value = "any_values_match", deterministic = true, calledOnNullInput = true)
    @Description("Returns whether any values of a map match the given predicate.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "f", type = "function(V, boolean)")})
    @SqlType("boolean")
    public static String anyValuesMatch()
    {
        return "RETURN ANY_MATCH(MAP_VALUES(input), f)";
    }

    @SqlInvokedScalarFunction(value = "no_keys_match", deterministic = true, calledOnNullInput = true)
    @Description("Returns whether no keys of a map match the given predicate.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "f", type = "function(K, boolean)")})
    @SqlType("boolean")
    public static String noKeysMatch()
    {
        return "RETURN NONE_MATCH(MAP_KEYS(input), f)";
    }

    @SqlInvokedScalarFunction(value = "no_values_match", deterministic = true, calledOnNullInput = true)
    @Description("Returns whether no values of a map match the given predicate.")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlParameters({@SqlParameter(name = "input", type = "map(K, V)"), @SqlParameter(name = "f", type = "function(V, boolean)")})
    @SqlType("boolean")
    public static String noValuesMatch()
    {
        return "RETURN NONE_MATCH(MAP_VALUES(input), f)";
    }
}
