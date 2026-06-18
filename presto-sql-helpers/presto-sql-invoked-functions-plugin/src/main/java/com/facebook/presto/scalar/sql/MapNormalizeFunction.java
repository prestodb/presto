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

@SqlInvokedScalarFunction(value = "map_normalize", deterministic = true, calledOnNullInput = false)
@Description("Returns the map with the same keys but all non-null values are scaled proportionally so that the sum of values becomes 1.")
public class MapNormalizeFunction
{
    private MapNormalizeFunction() {}

    @SqlParameter(name = "input", type = "map<varchar, double>")
    @SqlType("map<varchar, double>")
    public static String arraySumDouble()
    {
        return "RETURN " +
                "transform(array[ROW(input, cast(array_sum(map_values(input)) as double))], " +
                "x->transform_values(x[1], (k, v) -> (v / x[2])))[1]";
    }
}
