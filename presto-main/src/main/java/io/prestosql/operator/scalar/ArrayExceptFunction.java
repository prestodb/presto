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
package io.prestosql.operator.scalar;

import io.prestosql.operator.aggregation.TypedSet;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.Type;

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
        TypedSet typedSet = new TypedSet(type, leftPositionCount + rightPositionCount, "array_except");
        BlockBuilder distinctElementBlockBuilder = type.createBlockBuilder(null, leftPositionCount);
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
