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

@ScalarFunction("array_intersect")
@Description("Intersects elements of the two given arrays")
public final class ArrayIntersectFunction
{
    private ArrayIntersectFunction() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block intersect(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        int leftPositionCount = leftArray.getPositionCount();
        int rightPositionCount = rightArray.getPositionCount();

        if (rightPositionCount == 0) {
            return rightArray;
        }

        TypedSet leftTypedSet = new TypedSet(type, leftPositionCount, "array_intersect");
        for (int i = 0; i < leftPositionCount; i++) {
            leftTypedSet.add(leftArray, i);
        }

        TypedSet rightTypedSet = new TypedSet(type, rightPositionCount, "array_intersect");
        BlockBuilder blockBuilder = type.createBlockBuilder(null, rightPositionCount);
        for (int i = 0; i < rightPositionCount; i++) {
            if (leftTypedSet.contains(rightArray, i) && rightTypedSet.add(rightArray, i)) {
                type.appendTo(rightArray, i, blockBuilder);
            }
        }

        return blockBuilder.build();
    }
}
