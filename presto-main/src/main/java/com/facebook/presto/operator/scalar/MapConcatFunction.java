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

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.operator.scalar.annotations.TypeParameter;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;

@ScalarFunction("map_concat")
@Description("Concatenates given maps")
public final class MapConcatFunction
{
    private MapConcatFunction() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType("map(K,V)")
    public static Block mapConcat(
            @TypeParameter("K") Type keyType,
            @TypeParameter("V") Type valueType,
            @SqlType("map(K,V)") Block leftMap,
            @SqlType("map(K,V)") Block rightMap)
    {
        TypedSet typedSet = new TypedSet(keyType, rightMap.getPositionCount());
        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(keyType, valueType), new BlockBuilderStatus(), leftMap.getPositionCount() + rightMap.getPositionCount());
        for (int i = 0; i < rightMap.getPositionCount(); i += 2) {
            typedSet.add(rightMap, i);
            keyType.appendTo(rightMap, i, blockBuilder);
            valueType.appendTo(rightMap, i + 1, blockBuilder);
        }
        for (int i = 0; i < leftMap.getPositionCount(); i += 2) {
            if (!typedSet.contains(leftMap, i)) {
                keyType.appendTo(leftMap, i, blockBuilder);
                valueType.appendTo(leftMap, i + 1, blockBuilder);
            }
        }
        return blockBuilder.build();
    }
}
