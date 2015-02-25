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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class ArrayIntersectFunction
        extends ParametricScalar
{
    public static final ArrayIntersectFunction ARRAY_INTERSECT_FUNCTION = new ArrayIntersectFunction();
    private static final String FUNCTION_NAME = "array_intersect";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(orderableTypeParameter("E")), "array<E>", ImmutableList.of("array<E>", "array<E>"), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayIntersectFunction.class, "intersect", Type.class, Slice.class, Slice.class);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
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
        return "Intersects elements of the two given arrays.";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, format("%s expects only one argument", FUNCTION_NAME));
        TypeSignature typeSignature = parameterizedTypeName("array", types.get("E").getTypeSignature());
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(types.get("E"));
        Signature signature = new Signature(FUNCTION_NAME, typeSignature, typeSignature, typeSignature);
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false, false));
    }

    private static IntComparator IntBlockCompare(Type type, Block inBlock)
    {
        return new IntComparator() {
            @Override
            public int compare(int left, int right)
            {
                if (inBlock.isNull(left) && inBlock.isNull(right)) {
                    return 0;
                }
                if (inBlock.isNull(left)) {
                    return -1;
                }
                if (inBlock.isNull(right)) {
                    return 1;
                }
                return type.compareTo(inBlock, left, inBlock, right);
            }

            @Override
            public int compare(Integer left, Integer right)
            {
                if (inBlock.isNull(left) && inBlock.isNull(right)) {
                return 0;
            }
                if (inBlock.isNull(left)) {
                    return -1;
                }
                if (inBlock.isNull(right)) {
                    return 1;
                }
                return type.compareTo(inBlock, left, inBlock, right);
            }
        };
    }

    public static Slice intersect(Type type, Slice leftArray, Slice rightArray)
    {
        Block leftBlock = readStructuralBlock(leftArray);
        Block rightBlock = readStructuralBlock(rightArray);

        int leftPositionCount = leftBlock.getPositionCount();
        int rightPositionCount = rightBlock.getPositionCount();

        int[] leftPositions = new int[leftPositionCount];
        int[] rightPositions = new int[rightPositionCount];

        for (int i = 0; i < leftPositionCount; i++) {
            leftPositions[i] = i;
        }
        for (int i = 0; i < rightPositionCount; i++) {
            rightPositions[i] = i;
        }
        IntArrays.quickSort(leftPositions, IntBlockCompare(type, leftBlock));
        IntArrays.quickSort(rightPositions, IntBlockCompare(type, rightBlock));

        BlockBuilder resultBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), leftBlock.getSizeInBytes());

        int leftCurPos = 0;
        int rightCurPos = 0;
        int leftBasePos;
        int rightBasePos;

        while (leftCurPos < leftPositionCount && rightCurPos < rightPositionCount) {
            leftBasePos = leftCurPos;
            rightBasePos = rightCurPos;
            int compareValue = type.compareTo(leftBlock, leftPositions[leftCurPos], rightBlock, rightPositions[rightCurPos]);
            if (compareValue > 0) {
                rightCurPos++;
            }
            else if (compareValue < 0) {
                leftCurPos++;
            }
            else {
                type.appendTo(leftBlock, leftPositions[leftCurPos], resultBlockBuilder);
                leftCurPos++;
                rightCurPos++;
                while (leftCurPos < leftPositionCount && type.equalTo(leftBlock, leftPositions[leftBasePos], leftBlock, leftPositions[leftCurPos])) {
                    leftCurPos++;
                }
                while (rightCurPos < rightPositionCount && type.equalTo(rightBlock, rightPositions[rightBasePos], rightBlock, rightPositions[rightCurPos])) {
                    rightCurPos++;
                }
            }
        }

        return buildStructuralSlice(resultBlockBuilder);
    }
}
