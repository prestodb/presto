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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.AbstractType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;

import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;

@ScalarFunction("arrays_overlap")
@Description("Returns true if arrays have common elements")
public final class ArraysOverlapFunction
{
    private int[] leftPositions;
    private int[] rightPositions;

    private long[] leftLongArray;
    private long[] rightLongArray;

    @TypeParameter("E")
    public ArraysOverlapFunction(@TypeParameter("E") Type elementType) {}

    private static IntComparator intBlockCompare(Type type, Block block)
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
                    return 1;
                }
                if (block.isNull(right)) {
                    return -1;
                }
                return type.compareTo(block, left, block, right);
            }
        };
    }

    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public Boolean arraysOverlapInt(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"integer", "integer"}) MethodHandle lessThanFunction,
            @SqlType("array(integer)") Block leftArray,
            @SqlType("array(integer)") Block rightArray)
    {
        return genericArraysOverlap(leftArray, rightArray, INTEGER);
    }

    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public Boolean arraysOverlapBigInt(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"bigint", "bigint"}) MethodHandle lessThanFunction,
            @SqlType("array(bigint)") Block leftArray,
            @SqlType("array(bigint)") Block rightArray)
    {
        return genericArraysOverlap(leftArray, rightArray, BIGINT);
    }

    @SqlNullable
    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    public Boolean arraysOverlap(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle lessThanFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        int leftPositionCount = leftArray.getPositionCount();
        int rightPositionCount = rightArray.getPositionCount();

        if (leftPositionCount == 0 || rightPositionCount == 0) {
            return false;
        }

        if (leftPositions == null || leftPositions.length < leftPositionCount) {
            leftPositions = new int[leftPositionCount * 2];
        }

        if (rightPositions == null || rightPositions.length < rightPositionCount) {
            rightPositions = new int[rightPositionCount * 2];
        }

        for (int i = 0; i < leftPositionCount; i++) {
            leftPositions[i] = i;
        }
        for (int i = 0; i < rightPositionCount; i++) {
            rightPositions[i] = i;
        }
        IntArrays.quickSort(leftPositions, 0, leftPositionCount, intBlockCompare(type, leftArray));
        IntArrays.quickSort(rightPositions, 0, rightPositionCount, intBlockCompare(type, rightArray));

        int leftCurrentPosition = 0;
        int rightCurrentPosition = 0;
        while (leftCurrentPosition < leftPositionCount && rightCurrentPosition < rightPositionCount) {
            if (leftArray.isNull(leftPositions[leftCurrentPosition]) || rightArray.isNull(rightPositions[rightCurrentPosition])) {
                // Nulls are in the end of the array. Non-null elements do not overlap.
                return null;
            }
            int compareValue = type.compareTo(leftArray, leftPositions[leftCurrentPosition], rightArray, rightPositions[rightCurrentPosition]);
            if (compareValue > 0) {
                rightCurrentPosition++;
            }
            else if (compareValue < 0) {
                leftCurrentPosition++;
            }
            else {
                return true;
            }
        }
        return leftArray.isNull(leftPositions[leftPositionCount - 1]) || rightArray.isNull(rightPositions[rightPositionCount - 1]) ? null : false;
    }

    public Boolean genericArraysOverlap(Block leftArray, Block rightArray, AbstractType type)
    {
        int leftSize = leftArray.getPositionCount();
        int rightSize = rightArray.getPositionCount();

        if (leftSize == 0 || rightSize == 0) {
            return false;
        }

        if (leftLongArray == null || leftLongArray.length < leftSize) {
            leftLongArray = new long[leftSize * 2];
        }
        if (rightLongArray == null || rightLongArray.length < rightSize) {
            rightLongArray = new long[rightSize * 2];
        }

        int leftNonNullSize = sortAbstractLongArray(leftArray, leftLongArray, type);
        int rightNonNullSize = sortAbstractLongArray(rightArray, rightLongArray, type);

        int leftPosition = 0;
        int rightPosition = 0;
        while (leftPosition < leftNonNullSize && rightPosition < rightNonNullSize) {
            if (leftLongArray[leftPosition] < rightLongArray[rightPosition]) {
                leftPosition++;
            }
            else if (rightLongArray[rightPosition] < leftLongArray[leftPosition]) {
                rightPosition++;
            }
            else {
                return true;
            }
        }
        return (leftNonNullSize < leftSize) || (rightNonNullSize < rightSize) ? null : false;
    }

    // Assumes buffer is long enough, returns count of non-null elements.
    private static int sortAbstractLongArray(Block array, long[] buffer, AbstractType type)
    {
        int arraySize = array.getPositionCount();
        int nonNullSize = 0;
        for (int i = 0; i < arraySize; i++) {
            if (!array.isNull(i)) {
                buffer[nonNullSize++] = type.getLong(array, i);
            }
        }
        for (int i = 1; i < nonNullSize; i++) {
            if (buffer[i - 1] > buffer[i]) {
                Arrays.sort(buffer, 0, nonNullSize);
                break;
            }
        }
        return nonNullSize;
    }
}
