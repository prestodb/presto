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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.scalar.ArraySortComparatorFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import com.facebook.presto.sql.gen.lambda.LambdaFunctionInterface;
import io.airlift.slice.Slice;

import java.util.Arrays;

@Description("Returns top N keys of a map based on the comparator")
@ScalarFunction(value = "map_top_n_keys")
public final class MapTopNKeys
{
    private MapTopNKeys() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @TypeParameterSpecialization(name = "K", nativeContainerType = Block.class)
    @SqlType("array(K)")
    @SqlNullable
    public Block topNKeysBlock(
            @TypeParameter("K") Type keyType,
            @SqlType("map(K,V)") Block mapBlock,
            @SqlType(StandardTypes.INTEGER) int n,
            @SqlType("function(K, K, int)") ComparatorBlockLambda function)
    {
        BlockBuilder blockBuilder = keyType.createBlockBuilder(null, mapBlock.getPositionCount() / 2);
        for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
            keyType.appendTo(mapBlock, i, blockBuilder);
        }

        ArraySortComparatorFunction obj = new ArraySortComparatorFunction(keyType);
        Block block = obj.sortObject(keyType, blockBuilder.build(), (ArraySortComparatorFunction.ComparatorBlockLambda) function);

        return (Block) Arrays.asList(block).subList(0, n);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @TypeParameterSpecialization(name = "K", nativeContainerType = Slice.class)
    @SqlType("array(K)")
    @SqlNullable
    public Block topNKeysSlice(
            @TypeParameter("K") Type keyType,
            @SqlType("map(K,V)") Block mapBlock,
            @SqlType(StandardTypes.INTEGER) int n,
            @SqlType("function(K, K, int)") ComparatorSliceLambda function)
    {
        BlockBuilder blockBuilder = keyType.createBlockBuilder(null, mapBlock.getPositionCount() / 2);
        for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
            keyType.appendTo(mapBlock, i, blockBuilder);
        }

        ArraySortComparatorFunction obj = new ArraySortComparatorFunction(keyType);
        Block block = obj.sortSlice(keyType, blockBuilder.build(), (ArraySortComparatorFunction.ComparatorSliceLambda) function);

        return (Block) Arrays.asList(block).subList(0, n);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @TypeParameterSpecialization(name = "K", nativeContainerType = long.class)
    @SqlType("array(K)")
    @SqlNullable
    public Block topNKeysLong(
            @TypeParameter("K") Type keyType,
            @SqlType("map(K,V)") Block mapBlock,
            @SqlType(StandardTypes.INTEGER) int n,
            @SqlType("function(K, K, int)") ComparatorLongLambda function)
    {
        BlockBuilder blockBuilder = keyType.createBlockBuilder(null, mapBlock.getPositionCount() / 2);
        for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
            keyType.appendTo(mapBlock, i, blockBuilder);
        }

        ArraySortComparatorFunction obj = new ArraySortComparatorFunction(keyType);
        Block block = obj.sortLong(keyType, blockBuilder.build(), (ArraySortComparatorFunction.ComparatorLongLambda) function);

        return (Block) Arrays.asList(block).subList(0, n);
    }

    @FunctionalInterface
    public interface ComparatorLongLambda
            extends LambdaFunctionInterface
    {
        Long apply(Long x, Long y);
    }

    @FunctionalInterface
    public interface ComparatorDoubleLambda
            extends LambdaFunctionInterface
    {
        Long apply(Double x, Double y);
    }

    @FunctionalInterface
    public interface ComparatorBooleanLambda
            extends LambdaFunctionInterface
    {
        Long apply(Boolean x, Boolean y);
    }

    @FunctionalInterface
    public interface ComparatorSliceLambda
            extends LambdaFunctionInterface
    {
        Long apply(Slice x, Slice y);
    }

    @FunctionalInterface
    public interface ComparatorBlockLambda
            extends LambdaFunctionInterface
    {
        Long apply(Block x, Block y);
    }
}
