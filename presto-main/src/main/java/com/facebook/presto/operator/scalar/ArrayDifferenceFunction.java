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

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;

@Description("Set difference of two arrays")
@ScalarFunction("array_diff")
public final class ArrayDifferenceFunction
{
    private static final int INITIAL_SIZE = 128;

    private int[] leftPositions = new int[INITIAL_SIZE];
    private int[] rightPositions = new int[INITIAL_SIZE];
    private final PageBuilder pageBuilder;

    @TypeParameter("E")
    public ArrayDifferenceFunction(@TypeParameter("E") Type elementType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(elementType));
    }

    private static IntComparator IntBlockCompare(Type type, Block block)
    {
        return new AbstractIntComparator()
        {
            @Override
            public int compare(int left, int right)
            {
                if (block.isNull(left) && block.isNull(right)) {
                    return 0;
                }
                if (block.isNull(left)) {
                    return -1;
                }
                if (block.isNull(right)) {
                    return 1;
                }
                return type.compareTo(block, left, block, right);
            }
        };
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block difference(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block left,
            @SqlType("array(E)") Block right)
    {
        int leftSize = left.getPositionCount();
        int rightSize = right.getPositionCount();
        if (leftSize == 0 || rightSize == 0) {
            return left;
        }

        if (leftPositions.length < leftSize) {
            leftPositions = new int[leftSize];
        }

        if (rightPositions.length < rightSize) {
            rightPositions = new int[rightSize];
        }

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        for (int i = 0; i < leftSize; i++) {
            leftPositions[i] = i;
        }
        for (int i = 0; i < rightSize; i++) {
            rightPositions[i] = i;
        }
        IntArrays.quickSort(leftPositions, 0, leftSize, IntBlockCompare(type, left));
        IntArrays.quickSort(rightPositions, 0, rightSize, IntBlockCompare(type, right));

        BlockBuilder resultBlockBuilder = pageBuilder.getBlockBuilder(0);
        int totalCount = 0;

        int leftPositionIndex = 0;
        while (leftPositionIndex < leftSize && left.isNull(leftPositions[leftPositionIndex])) {
            resultBlockBuilder.appendNull();
            leftPositionIndex++;
            totalCount++;
        }

        boolean insertNull = right.isNull(rightPositions[0]);
        int rightPositionIndex = 0;
        while (rightPositionIndex < rightSize && right.isNull(rightPositions[rightPositionIndex])) {
            rightPositionIndex++;
        }

        while (leftPositionIndex < leftSize && rightPositionIndex < rightSize) {
            int compareValue = type.compareTo(left, leftPositions[leftPositionIndex], right, rightPositions[rightPositionIndex]);
            if (compareValue < 0) {
                if (insertNull) {
                    resultBlockBuilder.appendNull();
                }
                else {
                    type.appendTo(left, leftPositions[leftPositionIndex], resultBlockBuilder);
                }
                leftPositionIndex++;
                totalCount++;
            }
            else if (compareValue > 0) {
                rightPositionIndex++;
            }
            else {
                int equalityPosition = leftPositions[leftPositionIndex];
                leftPositionIndex++;
                rightPositionIndex++;
                while (leftPositionIndex < leftSize && type.equalTo(left, equalityPosition, left, leftPositions[leftPositionIndex])) {
                    leftPositionIndex++;
                }
                while (rightPositionIndex < rightSize && type.equalTo(left, equalityPosition, right, rightPositions[rightPositionIndex])) {
                    rightPositionIndex++;
                }
            }
        }
        while (leftPositionIndex < leftSize) {
            if (insertNull) {
                resultBlockBuilder.appendNull();
            }
            else {
                type.appendTo(left, leftPositions[leftPositionIndex], resultBlockBuilder);
            }
            leftPositionIndex++;
            totalCount++;
        }
        pageBuilder.declarePositions(totalCount);
        return resultBlockBuilder.getRegion(resultBlockBuilder.getPositionCount() - totalCount, totalCount);
    }
}
