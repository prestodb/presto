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
package io.prestosql.operator.scalar;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;

@ScalarFunction("cardinality")
@Description("Returns the cardinality (the number of key-value pairs) of the map")
public final class MapCardinalityFunction
{
    private MapCardinalityFunction() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.BIGINT)
    public static long mapCardinality(@SqlType("map(K,V)") Block block)
    {
        return block.getPositionCount() / 2;
    }
}
