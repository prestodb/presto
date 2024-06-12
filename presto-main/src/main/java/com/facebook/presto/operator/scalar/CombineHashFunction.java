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

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarFunctionConstantStats;
import com.facebook.presto.spi.function.ScalarPropagateSourceStats;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.spi.function.PropagateSourceStats.MAX;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;

public final class CombineHashFunction
{
    private CombineHashFunction() {}

    @ScalarFunction(value = "combine_hash", visibility = HIDDEN)
    @SqlType(StandardTypes.BIGINT)
    @ScalarFunctionConstantStats(avgRowSize = 8, distinctValuesCount = -1) // distinctValuesCount of -1 implies ROW_COUNT
    public static long getHash(
            @ScalarPropagateSourceStats(nullFraction = MAX) @SqlType(StandardTypes.BIGINT) long previousHashValue,
            @SqlType(StandardTypes.BIGINT) long value)
    {
        return (31 * previousHashValue + value);
    }
}
