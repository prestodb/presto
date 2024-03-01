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
import com.facebook.presto.type.TypeUtils;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import static com.facebook.presto.common.type.BigintType.BIGINT;

@ScalarFunction("array_distinct")
@Description("Remove duplicate values from the given array")
public final class ArrayDistinctFunction
{
    private ArrayDistinctFunction() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block distinct(@TypeParameter("E") Type type, @SqlType("array(E)") Block array)
    {
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

        if (array.mayHaveNull()) {
            int firstDuplicatePosition = 0;
            // Keep adding the element to the set as long as there are no dupes.
            while (firstDuplicatePosition < arrayLength && typedSet.add(array, firstDuplicatePosition)) {
                firstDuplicatePosition++;
            }

            if (firstDuplicatePosition == arrayLength) {
                // All elements are distinct, so just return the original.
                return array;
            }

            int position = 0;
            distinctElementBlockBuilder = type.createBlockBuilder(null, arrayLength);
            while (position < firstDuplicatePosition) {
                type.appendTo(array, position, distinctElementBlockBuilder);
                position++;
            }
            while (position < arrayLength) {
                if (typedSet.add(array, position)) {
                    type.appendTo(array, position, distinctElementBlockBuilder);
                }
                position++;
            }
        }
        else {
            int firstDuplicatePosition = 0;
            while (firstDuplicatePosition < arrayLength && typedSet.addNonNull(array, firstDuplicatePosition)) {
                firstDuplicatePosition++;
            }

            if (firstDuplicatePosition == arrayLength) {
                // All elements are distinct, so just return the original.
                return array;
            }

            int position = 0;
            distinctElementBlockBuilder = type.createBlockBuilder(null, arrayLength);
            while (position < firstDuplicatePosition) {
                type.appendTo(array, position, distinctElementBlockBuilder);
                position++;
            }
            while (position < arrayLength) {
                if (typedSet.addNonNull(array, position)) {
                    type.appendTo(array, position, distinctElementBlockBuilder);
                }
                position++;
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
            boolean containsNull = false;

            // Keep adding the element to the set as long as there are no dupes.
            while (position < arrayLength) {
                if (array.isNull(position)) {
                    if (!containsNull) {
                        containsNull = true;
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
                    if (!containsNull) {
                        BIGINT.appendTo(array, position, distinctElementBlockBuilder);
                    }
                }
                else if (set.add(BIGINT.getLong(array, position))) {
                    BIGINT.appendTo(array, position, distinctElementBlockBuilder);
                }
            }
        }
        else {
            int position = 0;
            // Keep adding the element to the set as long as there are no dupes.
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
