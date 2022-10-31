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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import io.airlift.slice.Slice;

@ScalarFunction("map_top_n_keys")
@Description("Get the top N keys of the given map using a lambda comparator.")
public final class MapTopNKeysComparatorFunction
{
    @TypeParameter("K")
    @TypeParameter("V")
    public MapTopNKeysComparatorFunction(@TypeParameter("K") Type keyType, @TypeParameter("V") Type valueType) {}

    @TypeParameter("K")
    @TypeParameterSpecialization(name = "K", nativeContainerType = long.class)
    @TypeParameter("V")
    @SqlType("array(K)")
    public Block topNKeysLong(
            @TypeParameter("K") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(K, K, int)") ArraySortComparatorFunction.ComparatorLongLambda function)
    {
        if (n == 0) {
            return type.createBlockBuilder(null, 0).build();
        }

        ArraySortComparatorFunction instance = new ArraySortComparatorFunction(type);
        Block block = instance.sortLong(type, MapKeys.getKeys(type, mapBlock), function);

        return MapTopNKeysFunction.computeTopNBlock(type, block, n);
    }

    @TypeParameter("K")
    @TypeParameterSpecialization(name = "K", nativeContainerType = double.class)
    @TypeParameter("V")
    @SqlType("array(K)")
    public Block topNKeysDouble(
            @TypeParameter("K") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(K, K, int)") ArraySortComparatorFunction.ComparatorDoubleLambda function)
    {
        if (n == 0) {
            return type.createBlockBuilder(null, 0).build();
        }

        ArraySortComparatorFunction instance = new ArraySortComparatorFunction(type);
        Block block = instance.sortDouble(type, MapKeys.getKeys(type, mapBlock), function);

        return MapTopNKeysFunction.computeTopNBlock(type, block, n);
    }

    @TypeParameter("K")
    @TypeParameterSpecialization(name = "K", nativeContainerType = boolean.class)
    @TypeParameter("V")
    @SqlType("array(K)")
    public Block topNKeysBoolean(
            @TypeParameter("K") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(K, K, int)") ArraySortComparatorFunction.ComparatorBooleanLambda function)
    {
        if (n == 0) {
            return type.createBlockBuilder(null, 0).build();
        }

        ArraySortComparatorFunction instance = new ArraySortComparatorFunction(type);
        Block block = instance.sortBoolean(type, MapKeys.getKeys(type, mapBlock), function);

        return MapTopNKeysFunction.computeTopNBlock(type, block, n);
    }

    @TypeParameter("K")
    @TypeParameterSpecialization(name = "K", nativeContainerType = Slice.class)
    @TypeParameter("V")
    @SqlType("array(K)")
    public Block topNKeysSlice(
            @TypeParameter("K") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(K, K, int)") ArraySortComparatorFunction.ComparatorSliceLambda function)
    {
        if (n == 0) {
            return type.createBlockBuilder(null, 0).build();
        }

        ArraySortComparatorFunction instance = new ArraySortComparatorFunction(type);
        Block block = instance.sortSlice(type, MapKeys.getKeys(type, mapBlock), function);

        return MapTopNKeysFunction.computeTopNBlock(type, block, n);
    }

    @TypeParameter("K")
    @TypeParameterSpecialization(name = "K", nativeContainerType = Block.class)
    @TypeParameter("V")
    @SqlType("array(K)")
    public Block topNKeysObject(
            @TypeParameter("K") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(K, K, int)") ArraySortComparatorFunction.ComparatorBlockLambda function)
    {
        if (n == 0) {
            return type.createBlockBuilder(null, 0).build();
        }

        ArraySortComparatorFunction instance = new ArraySortComparatorFunction(type);
        Block block = instance.sortObject(type, MapKeys.getKeys(type, mapBlock), function);

        return MapTopNKeysFunction.computeTopNBlock(type, block, n);
    }
}
