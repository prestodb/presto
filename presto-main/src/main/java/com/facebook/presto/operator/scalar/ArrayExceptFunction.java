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
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.Type;

@ScalarFunction("array_except")
@Description("Returns an array of elements that are in the first array but not the second, without duplicates.")
public final class ArrayExceptFunction
{
    private ArrayExceptFunction() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block except(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        int leftPositionCount = leftArray.getPositionCount();
        int rightPositionCount = rightArray.getPositionCount();

        if (leftPositionCount == 0) {
            return leftArray;
        }
        TypedSet typedSet = new TypedSet(type, leftPositionCount + rightPositionCount);
        BlockBuilder distinctElementBlockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), leftPositionCount);
        for (int i = 0; i < rightPositionCount; i++) {
            typedSet.add(rightArray, i);
        }
        for (int i = 0; i < leftPositionCount; i++) {
            if (!typedSet.contains(leftArray, i)) {
                typedSet.add(leftArray, i);
                type.appendTo(leftArray, i, distinctElementBlockBuilder);
            }
        }
        return distinctElementBlockBuilder.build();
    }
}
