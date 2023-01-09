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
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
@ScalarFunction("map_subset")
@Description("returns a map where the keys are a subset of the given array of keys")
public final class MapSubsetFunction
{
    private MapSubsetFunction() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType("MAP(K,V)")
    public static Block mapSubset(
            @TypeParameter("K") Type keyType,
            @TypeParameter("V") Type valueType,
            @TypeParameter("MAP(K,V)") Type mapType,
            @SqlType("MAP(K,V)") Block mapBlock, @SqlType("ARRAY(K)") Block keySubset)
    {
        if (mapBlock.getPositionCount() == 0 || keySubset.getPositionCount() == 0) {
            return mapType.createBlockBuilder(null, 0).build();
        }

        TypedSet typedSet = new TypedSet(keyType, keySubset.getPositionCount(), "map_subset");
        for (int i = 0; i < keySubset.getPositionCount(); i++) {
            if (!keySubset.isNull(i)) {
                typedSet.add(keySubset, i);
            }
        }

        int toFind = typedSet.size();
        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, toFind);
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();

        for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
            if (typedSet.contains(mapBlock, i)) {
                keyType.appendTo(mapBlock, i, blockBuilder);
                valueType.appendTo(mapBlock, i + 1, blockBuilder);
                toFind--;
                if (toFind == 0) {
                    break;
                }
            }
        }

        mapBlockBuilder.closeEntry();
        return (Block) mapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }
}
