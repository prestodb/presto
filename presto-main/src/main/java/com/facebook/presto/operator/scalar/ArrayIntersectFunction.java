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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class ArrayIntersectFunction
        extends SqlScalarFunction
{
    public static final ArrayIntersectFunction ARRAY_INTERSECT_FUNCTION = new ArrayIntersectFunction();
    private static final String FUNCTION_NAME = "array_intersect";
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayIntersectFunction.class, "intersect", Type.class, Block.class, Block.class);

    public ArrayIntersectFunction()
    {
        super(FUNCTION_NAME, ImmutableList.of(orderableTypeParameter("E")), "array<E>", ImmutableList.of("array<E>", "array<E>"));
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Intersects elements of the two given arrays";
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, format("%s expects only one argument", FUNCTION_NAME));
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(types.get("E"));
        return new ScalarFunctionImplementation(false, ImmutableList.of(false, false), methodHandle, isDeterministic());
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

    public static Block intersect(Type type, Block leftArray, Block rightArray)
    {
        int leftPositionCount = leftArray.getPositionCount();
        int rightPositionCount = rightArray.getPositionCount();

        if (leftPositionCount == 0) {
            return leftArray;
        }
        if (rightPositionCount == 0) {
            return rightArray;
        }

        int[] leftPositions = new int[leftPositionCount];
        int[] rightPositions = new int[rightPositionCount];

        for (int i = 0; i < leftPositionCount; i++) {
            leftPositions[i] = i;
        }
        for (int i = 0; i < rightPositionCount; i++) {
            rightPositions[i] = i;
        }
        IntArrays.quickSort(leftPositions, IntBlockCompare(type, leftArray));
        IntArrays.quickSort(rightPositions, IntBlockCompare(type, rightArray));

        int entrySize;
        if (leftPositionCount < rightPositionCount) {
            entrySize = (int) Math.ceil(leftArray.getSizeInBytes() / (double) leftPositionCount);
        }
        else {
            entrySize = (int) Math.ceil(rightArray.getSizeInBytes() / (double) rightPositionCount);
        }
        BlockBuilder resultBlockBuilder = type.createBlockBuilder(
                new BlockBuilderStatus(),
                Math.min(leftArray.getPositionCount(), rightArray.getPositionCount()),
                entrySize);

        int leftCurrentPosition = 0;
        int rightCurrentPosition = 0;
        int leftBasePosition;
        int rightBasePosition;

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
                while (leftCurrentPosition < leftPositionCount && type.equalTo(leftArray, leftPositions[leftBasePosition], leftArray, leftPositions[leftCurrentPosition])) {
                    leftCurrentPosition++;
                }
                while (rightCurrentPosition < rightPositionCount && type.equalTo(rightArray, rightPositions[rightBasePosition], rightArray, rightPositions[rightCurrentPosition])) {
                    rightCurrentPosition++;
                }
            }
        }

        return resultBlockBuilder.build();
    }
}
