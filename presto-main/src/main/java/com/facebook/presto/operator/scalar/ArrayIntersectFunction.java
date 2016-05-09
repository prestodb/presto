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

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.OperatorType.LESS_THAN;

@ScalarFunction("array_intersect")
@Description("Intersects elements of the two given arrays")
public final class ArrayIntersectFunction
{
    private static final int INITIAL_SIZE = 128;

    private int[] leftPositions = new int[INITIAL_SIZE];
    private int[] rightPositions = new int[INITIAL_SIZE];
    private final PageBuilder pageBuilder;

    @TypeParameter("E")
    public ArrayIntersectFunction(@TypeParameter("E") Type elementType)
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
    public Block intersect(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle lessThanFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        int leftPositionCount = leftArray.getPositionCount();
        int rightPositionCount = rightArray.getPositionCount();

        if (leftPositionCount == 0) {
            return leftArray;
        }
        if (rightPositionCount == 0) {
            return rightArray;
        }

        if (leftPositions.length < leftPositionCount) {
            leftPositions = new int[leftPositionCount];
        }

        if (rightPositions.length < rightPositionCount) {
            rightPositions = new int[rightPositionCount];
        }

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        for (int i = 0; i < leftPositionCount; i++) {
            leftPositions[i] = i;
        }
        for (int i = 0; i < rightPositionCount; i++) {
            rightPositions[i] = i;
        }
        IntArrays.quickSort(leftPositions, 0, leftPositionCount, IntBlockCompare(type, leftArray));
        IntArrays.quickSort(rightPositions, 0, rightPositionCount, IntBlockCompare(type, rightArray));

        BlockBuilder resultBlockBuilder = pageBuilder.getBlockBuilder(0);

        int leftCurrentPosition = 0;
        int rightCurrentPosition = 0;
        int leftBasePosition;
        int rightBasePosition;
        int totalCount = 0;

        while (leftCurrentPosition < leftPositionCount && rightCurrentPosition < rightPositionCount) {
            leftBasePosition = leftCurrentPosition;
            rightBasePosition = rightCurrentPosition;
            int compareValue = type.compareTo(leftArray, leftPositions[leftCurrentPosition], rightArray, rightPositions[rightCurrentPosition]);
            if (compareValue > 0) {
                rightCurrentPosition++;
            }
            else if (compareValue < 0) {
                leftCurrentPosition++;
            }
            else {
                type.appendTo(leftArray, leftPositions[leftCurrentPosition], resultBlockBuilder);
                leftCurrentPosition++;
                rightCurrentPosition++;
                totalCount++;
                while (leftCurrentPosition < leftPositionCount && type.equalTo(leftArray, leftPositions[leftBasePosition], leftArray, leftPositions[leftCurrentPosition])) {
                    leftCurrentPosition++;
                }
                while (rightCurrentPosition < rightPositionCount && type.equalTo(rightArray, rightPositions[rightBasePosition], rightArray, rightPositions[rightCurrentPosition])) {
                    rightCurrentPosition++;
                }
            }
        }
        pageBuilder.declarePositions(totalCount);
        return resultBlockBuilder.getRegion(resultBlockBuilder.getPositionCount() - totalCount, totalCount);
    }
}
