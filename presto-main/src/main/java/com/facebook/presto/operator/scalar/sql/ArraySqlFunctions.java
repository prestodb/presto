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
import com.facebook.presto.spi.function.SqlParameters;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

public class ArraySqlFunctions
{
    private ArraySqlFunctions() {}

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

    @SqlInvokedScalarFunction(value = "array_frequency", deterministic = true, calledOnNullInput = false)
    @Description("Returns the frequency of all array elements as a map.")
    @TypeParameter("T")
    @SqlParameter(name = "input", type = "array(T)")
    @SqlType("map(T, int)")
    public static String arrayFrequency()
    {
        return "RETURN reduce(" +
                "input," +
                "MAP()," +
                "(m, x) -> IF (x IS NOT NULL, MAP_CONCAT(m,MAP_FROM_ENTRIES(ARRAY[ROW(x, COALESCE(ELEMENT_AT(m,x) + 1, 1))])), m)," +
                "m -> m)";
    }

    @SqlInvokedScalarFunction(value = "array_duplicates", alias = {"array_dupes"}, deterministic = true, calledOnNullInput = false)
    @Description("Returns set of elements that have duplicates")
    @SqlParameter(name = "input", type = "array(T)")
    @TypeParameter("T")
    @SqlType("array(T)")
    public static String arrayDuplicates()
    {
        return "RETURN CONCAT(" +
                "IF (cardinality(filter(input, x -> x is NULL)) > 1, array[element_at(input, find_first_index(input, x -> x IS NULL))], array[])," +
                "map_keys(map_filter(array_frequency(input), (k, v) -> v > 1)))";
    }

    @SqlInvokedScalarFunction(value = "array_has_duplicates", alias = {"array_has_dupes"}, deterministic = true, calledOnNullInput = false)
    @Description("Returns whether array has any duplicate element")
    @TypeParameter("T")
    @SqlParameter(name = "input", type = "array(T)")
    @SqlType("boolean")
    public static String arrayHasDuplicatesVarchar()
    {
        return "RETURN cardinality(array_duplicates(input)) > 0";
    }

    @SqlInvokedScalarFunction(value = "array_max_by", deterministic = true, calledOnNullInput = true)
    @Description("Get the maximum value of array, by using a specific transformation function")
    @TypeParameter("T")
    @TypeParameter("U")
    @SqlParameters({@SqlParameter(name = "input", type = "array(T)"), @SqlParameter(name = "f", type = "function(T, U)")})
    @SqlType("T")
    public static String arrayMaxBy()
    {
        return "RETURN input[" +
                "array_max(zip_with(transform(input, f), sequence(1, cardinality(input)), (x, y)->IF(x IS NULL, NULL, (x, y))))[2]" +
                "]";
    }

    @SqlInvokedScalarFunction(value = "array_min_by", deterministic = true, calledOnNullInput = true)
    @Description("Get the minimum value of array, by using a specific transformation function")
    @TypeParameter("T")
    @TypeParameter("U")
    @SqlParameters({@SqlParameter(name = "input", type = "array(T)"), @SqlParameter(name = "f", type = "function(T, U)")})
    @SqlType("T")
    public static String arrayMinBy()
    {
        return "RETURN input[" +
                "array_min(zip_with(transform(input, f), sequence(1, cardinality(input)), (x, y)->IF(x IS NULL, NULL, (x, y))))[2]" +
                "]";
    }

    @SqlInvokedScalarFunction(value = "array_sort_desc", deterministic = true, calledOnNullInput = true)
    @Description("Sorts the given array in descending order according to the natural ordering of its elements.")
    @TypeParameter("T")
    @SqlParameter(name = "input", type = "array(T)")
    @SqlType("array<T>")
    public static String arraySortDesc()
    {
        return "RETURN reverse(array_sort(remove_nulls(input))) || filter(input, x -> x is null)";
    }

    @SqlInvokedScalarFunction(value = "remove_nulls", deterministic = true, calledOnNullInput = true)
    @Description("Removes null values from an array.")
    @TypeParameter("T")
    @SqlParameter(name = "input", type = "array(T)")
    @SqlType("array<T>")
    public static String removeNulls()
    {
        return "RETURN IF(none_match(input, x -> x is null), input, filter(input, x -> x is not null))";
    }
}
