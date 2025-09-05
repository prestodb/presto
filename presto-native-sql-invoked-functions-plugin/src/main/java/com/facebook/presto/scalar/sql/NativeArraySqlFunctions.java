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

    @SqlInvokedScalarFunction(value = "array_split_into_chunks", deterministic = true, calledOnNullInput = false)
    @Description("Returns an array of arrays splitting input array into chunks of given length. " +
            "If array is not evenly divisible it will split into as many possible chunks and " +
            "return the left over elements for the last array. Returns null for null inputs, but not elements.")
    @TypeParameter("T")
    @SqlParameters({@SqlParameter(name = "input", type = "array(T)"), @SqlParameter(name = "sz", type = "int")})
    @SqlType("array(array(T))")
    public static String arraySplitIntoChunks()
    {
        return "RETURN IF(sz <= 0, " +
                "fail('Invalid slice size: ' || cast(sz as varchar) || '. Size must be greater than zero.'), " +
                "IF(cardinality(input) / sz > 10000, " +
                "fail('Cannot split array of size: ' || cast(cardinality(input) as varchar) || ' into more than 10000 parts.'), " +
                "transform(" +
                "sequence(1, cardinality(input), sz), " +
                "x -> slice(input, x, sz))))";
    }

    @SqlInvokedScalarFunction(value = "array_least_frequent", deterministic = true, calledOnNullInput = true)
    @Description("Determines the least frequent element in the array. If there are multiple elements, the function returns the smallest element")
    @TypeParameter("T")
    @SqlParameter(name = "input", type = "array(T)")
    @SqlType("array<T>")
    public static String arrayLeastFrequent()
    {
        return "RETURN IF(COALESCE(CARDINALITY(REMOVE_NULLS(input)), 0) = 0, NULL, TRANSFORM(SLICE(ARRAY_SORT(TRANSFORM(MAP_ENTRIES(ARRAY_FREQUENCY(REMOVE_NULLS(input))), x -> ROW(x[2], x[1]))), 1, 1), x -> x[2]))";
    }

    @SqlInvokedScalarFunction(value = "array_least_frequent", deterministic = true, calledOnNullInput = true)
    @Description("Determines the n least frequent element in the array in the ascending order of the elements.")
    @TypeParameter("T")
    @SqlParameters({@SqlParameter(name = "input", type = "array(T)"), @SqlParameter(name = "n", type = "bigint")})
    @SqlType("array<T>")
    public static String arrayNLeastFrequent()
    {
        return "RETURN IF(n < 0, fail('n must be greater than or equal to 0'), IF(COALESCE(CARDINALITY(REMOVE_NULLS(input)), 0) = 0, NULL, TRANSFORM(SLICE(ARRAY_SORT(TRANSFORM(MAP_ENTRIES(ARRAY_FREQUENCY(REMOVE_NULLS(input))), x -> ROW(x[2], x[1]))), 1, n), x -> x[2])))";
    }

    @SqlInvokedScalarFunction(value = "array_top_n", deterministic = true, calledOnNullInput = true)
    @Description("Returns the top N values of the given map sorted using the provided lambda comparator.")
    @TypeParameter("T")
    @SqlParameters({@SqlParameter(name = "input", type = "array(T)"), @SqlParameter(name = "n", type = "int"), @SqlParameter(name = "f", type = "function(T, T, bigint)")})
    @SqlType("array<T>")
    public static String arrayTopNComparator()
    {
        return "RETURN IF(n < 0, fail('Parameter n: ' || cast(n as varchar) || ' to ARRAY_TOP_N is negative'), SLICE(REVERSE(ARRAY_SORT(input, f)), 1, n))";
    }
}
