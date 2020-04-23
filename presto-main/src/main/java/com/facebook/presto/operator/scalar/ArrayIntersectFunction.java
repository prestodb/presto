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
        if (leftArray.getPositionCount() < rightArray.getPositionCount()) {
            Block tempArray = leftArray;
            leftArray = rightArray;
            rightArray = tempArray;
        }

        int leftPositionCount = leftArray.getPositionCount();
        int rightPositionCount = rightArray.getPositionCount();

        if (rightPositionCount == 0) {
            return rightArray;
        }

        TypedSet rightTypedSet = new TypedSet(type, rightPositionCount, "array_intersect");
        for (int i = 0; i < rightPositionCount; i++) {
            rightTypedSet.add(rightArray, i);
        }

        BlockBuilder blockBuilder = type.createBlockBuilder(null, Math.min(leftPositionCount, rightPositionCount));

        // The intersected set can have at most rightPositionCount elements
        TypedSet intersectTypedSet = new TypedSet(type, blockBuilder, rightPositionCount, "array_intersect");
        for (int i = 0; i < leftPositionCount; i++) {
            if (rightTypedSet.contains(leftArray, i)) {
                intersectTypedSet.add(leftArray, i);
            }
        }

        return blockBuilder.build();
    }
}
