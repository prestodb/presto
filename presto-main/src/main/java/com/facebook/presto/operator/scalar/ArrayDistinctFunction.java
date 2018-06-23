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

import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

@ScalarFunction("array_distinct")
@Description("Remove duplicate values from the given array")
public final class ArrayDistinctFunction
{
    private final PageBuilder pageBuilder;

    @TypeParameter("E")
    public ArrayDistinctFunction(@TypeParameter("E") Type elementType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(elementType));
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block distinct(@TypeParameter("E") Type type, @SqlType("array(E)") Block array)
    {
        if (array.getPositionCount() < 2) {
            return array;
        }

        if (array.getPositionCount() == 2) {
            if (type.equalTo(array, 0, array, 1)) {
                return array.getSingleValueBlock(0);
            }
            else {
                return array;
            }
        }

        TypedSet typedSet = new TypedSet(type, array.getPositionCount(), "array_distinct");
        int distinctCount = 0;

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        BlockBuilder distinctElementBlockBuilder = pageBuilder.getBlockBuilder(0);
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (!typedSet.contains(array, i)) {
                typedSet.add(array, i);
                distinctCount++;
                type.appendTo(array, i, distinctElementBlockBuilder);
            }
        }

        pageBuilder.declarePositions(distinctCount);

        return distinctElementBlockBuilder.getRegion(distinctElementBlockBuilder.getPositionCount() - distinctCount, distinctCount);
    }

    @SqlType("array(bigint)")
    public Block bigintDistinct(@SqlType("array(bigint)") Block array)
    {
        if (array.getPositionCount() == 0) {
            return array;
        }

        boolean containsNull = false;
        LongSet set = new LongOpenHashSet(array.getPositionCount());
        int distinctCount = 0;

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        BlockBuilder distinctElementBlockBuilder = pageBuilder.getBlockBuilder(0);
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (array.isNull(i)) {
                if (!containsNull) {
                    containsNull = true;
                    distinctElementBlockBuilder.appendNull();
                    distinctCount++;
                }
                continue;
            }
            long value = BIGINT.getLong(array, i);
            if (!set.contains(value)) {
                set.add(value);
                distinctCount++;
                BIGINT.appendTo(array, i, distinctElementBlockBuilder);
            }
        }

        pageBuilder.declarePositions(distinctCount);

        return distinctElementBlockBuilder.getRegion(distinctElementBlockBuilder.getPositionCount() - distinctCount, distinctCount);
    }
}
