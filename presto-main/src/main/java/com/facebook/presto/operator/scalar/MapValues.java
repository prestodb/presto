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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

@ScalarFunction("map_values")
@Description("Returns the values of the given map(K,V) as an array")
public final class MapValues
{
    private MapValues() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType("array(V)")
    public static Block getValues(
            @TypeParameter("V") Type valueType,
            @SqlType("map(K,V)") Block block)
    {
        BlockBuilder blockBuilder = valueType.createBlockBuilder(null, block.getPositionCount() / 2);
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            valueType.appendTo(block, i + 1, blockBuilder);
        }
        return blockBuilder.build();
    }
}
