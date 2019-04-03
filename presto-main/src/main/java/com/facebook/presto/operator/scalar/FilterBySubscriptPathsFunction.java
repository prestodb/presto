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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

public class FilterBySubscriptPathsFunction
{
    public static final String FILTER_BY_SUBSCRIPT_PATHS = "filter_by_subscript_paths";

    private FilterBySubscriptPathsFunction() {}

    @ScalarFunction(value = FILTER_BY_SUBSCRIPT_PATHS, hidden = true)
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType("map(K,V)")
    public static Block filterMap(@SqlType("map(K,V)") Block block, @SqlType("array(varchar)") Block paths)
    {
        return block;
    }

    @ScalarFunction(value = FILTER_BY_SUBSCRIPT_PATHS, hidden = true)
    @TypeParameter("T")
    @SqlType("array(T)")
    public static Block filterArray(@SqlType("array(T)") Block block, @SqlType("array(varchar)") Block paths)
    {
        return block;
    }
}
