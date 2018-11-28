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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.Type;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;

@ScalarFunction("array_union")
@Description("Union elements of the two given arrays")
public final class ArrayUnionFunction
{
    private ArrayUnionFunction() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block union(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        int leftArrayCount = leftArray.getPositionCount();
        int rightArrayCount = rightArray.getPositionCount();
        TypedSet typedSet = new TypedSet(type, leftArrayCount + rightArrayCount, "array_union");
        appendTypedArray(leftArray, typedSet);
        appendTypedArray(rightArray, typedSet);
        return typedSet.getBlock();
    }

    @SqlType("array(bigint)")
    public static Block bigintUnion(@SqlType("array(bigint)") Block leftArray, @SqlType("array(bigint)") Block rightArray)
    {
        int leftArrayCount = leftArray.getPositionCount();
        int rightArrayCount = rightArray.getPositionCount();
        LongSet set = new LongOpenHashSet(leftArrayCount + rightArrayCount);
        BlockBuilder distinctElementBlockBuilder = BIGINT.createBlockBuilder(null, leftArrayCount + rightArrayCount);
        AtomicBoolean containsNull = new AtomicBoolean(false);
        appendBigintArray(leftArray, containsNull, set, distinctElementBlockBuilder);
        appendBigintArray(rightArray, containsNull, set, distinctElementBlockBuilder);

        return distinctElementBlockBuilder.build();
    }

    @SqlType("array(boolean)")
    public static Block booleanUnion(@SqlType("array(boolean)") Block leftArray, @SqlType("array(boolean)") Block rightArray)
    {
        boolean[] leftDistinctBooleanValues = getDistinctBooleanValues(leftArray);
        boolean[] rightDistinctBooleanValues = getDistinctBooleanValues(rightArray);

        BlockBuilder distinctElementBlockBuilder = BOOLEAN.createBlockBuilder(null, 3);

        if (leftDistinctBooleanValues[0] || rightDistinctBooleanValues[0]) {
            distinctElementBlockBuilder.appendNull();
        }

        if (leftDistinctBooleanValues[1] || rightDistinctBooleanValues[1]) {
            BOOLEAN.writeBoolean(distinctElementBlockBuilder, true);
        }

        if (leftDistinctBooleanValues[2] || rightDistinctBooleanValues[2]) {
            BOOLEAN.writeBoolean(distinctElementBlockBuilder, false);
        }

        return distinctElementBlockBuilder.build();
    }

    private static void appendBigintArray(Block array, AtomicBoolean containsNull, LongSet set, BlockBuilder blockBuilder)
    {
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (array.isNull(i)) {
                if (!containsNull.get()) {
                    containsNull.set(true);
                    blockBuilder.appendNull();
                }
                continue;
            }
            long value = BIGINT.getLong(array, i);
            if (set.add(value)) {
                BIGINT.writeLong(blockBuilder, value);
            }
        }
    }

    private static void appendTypedArray(Block array, TypedSet typedSet)
    {
        for (int i = 0; i < array.getPositionCount(); i++) {
            typedSet.add(array, i);
        }
    }

    private static boolean[] getDistinctBooleanValues(Block block)
    {
        boolean[] results = new boolean[3];

        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                results[0] = true;
            }
            else {
                boolean value = BOOLEAN.getBoolean(block, i);
                results[1] |= value;
                results[2] |= !value;
            }
        }
        return results;
    }
}
