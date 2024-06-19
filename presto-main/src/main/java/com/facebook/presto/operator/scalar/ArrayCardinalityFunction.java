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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarFunctionConstantStats;
import com.facebook.presto.spi.function.ScalarPropagateSourceStats;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import static com.facebook.presto.spi.function.PropagateSourceStats.SOURCE_STATS;

@Description("Returns the cardinality (length) of the array")
@ScalarFunction("cardinality")
public final class ArrayCardinalityFunction
{
    private ArrayCardinalityFunction() {}

    @TypeParameter("E")
    @SqlType(StandardTypes.BIGINT)
    @ScalarFunctionConstantStats(avgRowSize = 8.0, minValue = 0)
    public static long arrayCardinality(
            @ScalarPropagateSourceStats(nullFraction = SOURCE_STATS) @SqlType("array(E)") Block block)
    {
        return block.getPositionCount();
    }
}
