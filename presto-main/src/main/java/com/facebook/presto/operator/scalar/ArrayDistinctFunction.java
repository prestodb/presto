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
import com.facebook.presto.type.TypeUtils;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

@ScalarFunction(value = "array_distinct", deterministic = true)
@Description("Remove duplicate values from the given array")
public final class ArrayDistinctFunction
{
    private ArrayDistinctFunction() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block distinct(@TypeParameter("E") Type type, @SqlType("array(E)") Block array)
    {
        //if (true) newDistinct(type, array);
        int arrayLength = array.getPositionCount();
        if (arrayLength < 2) {
            return array;
        }

        if (arrayLength == 2) {
            if (TypeUtils.positionEqualsPosition(type, array, 0, array, 1)) {
                return array.getSingleValueBlock(0);
            }
            else {
                return array;
            }
        }

        TypedSet typedSet = new TypedSet(type, arrayLength, "array_distinct");
        BlockBuilder distinctElementBlockBuilder;
        int position = 0;

        if (array.mayHaveNull()) {
            while (position < arrayLength && typedSet.add(array, position)) { position++; }
            if (position == arrayLength) {
                // All elements are distinct, so just return the original.
                return array;
            }

            distinctElementBlockBuilder = type.createBlockBuilder(null, arrayLength);
            for (int i = 0; i < position; i++) {
                type.appendTo(array, i, distinctElementBlockBuilder);
            }
            for (position++; position < arrayLength; position++) {
                if (typedSet.add(array, position)) {
                    type.appendTo(array, position, distinctElementBlockBuilder);
                }
            }
        }
        else {
            while (position < arrayLength && typedSet.addNonNull(array, position)) { position++; }
            if (position == arrayLength) {
                // All elements are distinct, so just return the original.
                return array;
            }

            distinctElementBlockBuilder = type.createBlockBuilder(null, arrayLength);
            for (int i = 0; i < position; i++) {
                type.appendTo(array, i, distinctElementBlockBuilder);
            }
            for (position++; position < arrayLength; position++) {
                if (typedSet.addNonNull(array, position)) {
                    type.appendTo(array, position, distinctElementBlockBuilder);
                }
            }
        }

        return distinctElementBlockBuilder.build();
    }

    @SqlType("array(bigint)")
    public static Block bigintDistinct(@SqlType("array(bigint)") Block array)
    {
        final int arrayLength = array.getPositionCount();
        if (arrayLength < 2) {
            return array;
        }

        BlockBuilder distinctElementBlockBuilder;
        LongSet set = new LongOpenHashSet(arrayLength);

        if (array.mayHaveNull()) {
            int position = 0;
            boolean notSeenNull = true;

            while (position < arrayLength) {
                if (array.isNull(position)) {
                    if (notSeenNull) {
                        notSeenNull = false;
                    }
                    else {
                        // Second null.
                        break;
                    }
                }
                else if (!set.add(BIGINT.getLong(array, position))) {
                    // Dupe found.
                    break;
                }
                position++;
            }

            if (position == arrayLength) {
                // All elements are distinct, so just return the original.
                return array;
            }

            distinctElementBlockBuilder = BIGINT.createBlockBuilder(null, arrayLength);
            for (int i = 0; i < position; i++) {
                BIGINT.appendTo(array, i, distinctElementBlockBuilder);
            }

            for (position++; position < arrayLength; position++) {
                if (array.isNull(position)) {
                    if (notSeenNull) {
                        notSeenNull = false;
                    }
                }
                else if (set.add(BIGINT.getLong(array, position))) {
                    BIGINT.appendTo(array, position, distinctElementBlockBuilder);
                }
            }

            if (!notSeenNull) {
                distinctElementBlockBuilder.appendNull();
            }
        }
        else {
            int position = 0;
            while (position < arrayLength && set.add(BIGINT.getLong(array, position))) {
                position++;
            }
            if (position == arrayLength) {
                // All elements are distinct, so just return the original.
                return array;
            }

            distinctElementBlockBuilder = BIGINT.createBlockBuilder(null, arrayLength);
            for (int i = 0; i < position; i++) {
                BIGINT.appendTo(array, i, distinctElementBlockBuilder);
            }

            for (position++; position < arrayLength; position++) {
                if (set.add(BIGINT.getLong(array, position))) {
                    BIGINT.appendTo(array, position, distinctElementBlockBuilder);
                }
            }
        }

        return distinctElementBlockBuilder.build();
    }
}
