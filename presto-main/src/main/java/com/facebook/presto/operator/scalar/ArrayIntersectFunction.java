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

import com.facebook.presto.operator.aggregation.HashTable;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

@ScalarFunction("array_intersect")
@Description("Intersects elements of the two given arrays")
public final class ArrayIntersectFunction
{
    private final PageBuilder pageBuilder;

    @TypeParameter("E")
    public ArrayIntersectFunction(@TypeParameter("E") Type elementType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(elementType));
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block intersect(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        if (leftArray.getPositionCount() < rightArray.getPositionCount()) {
            Block tempArray = leftArray;
            leftArray = rightArray;
            rightArray = tempArray;
        }

        int rightPositionCount = rightArray.getPositionCount();

        if (rightPositionCount == 0) {
            return rightArray;
        }

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        HashTable rightHashTable = new HashTable(type, rightArray, rightPositionCount);
        for (int i = 0; i < rightPositionCount; i++) {
            rightHashTable.addIfAbsent(rightArray, i);
        }

        BlockBuilder leftBlockBuilder = pageBuilder.getBlockBuilder(0);
        int positionCountBefore = leftBlockBuilder.getPositionCount();

        // The intersected set can have at most rightPositionCount elements
        HashTable leftHashTable = new HashTable(type, leftBlockBuilder, rightPositionCount);
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            if (rightHashTable.contains(leftArray, i)) {
                leftHashTable.addIfAbsent(leftArray, i);
            }
        }

        int positionCountAfter = leftBlockBuilder.getPositionCount();
        pageBuilder.declarePositions(positionCountAfter - positionCountBefore);

        return leftHashTable.getBlock();
    }
}
