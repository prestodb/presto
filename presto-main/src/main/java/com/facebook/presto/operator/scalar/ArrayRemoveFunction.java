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
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.createBlock;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class ArrayRemoveFunction
        extends ParametricScalar
{
    public static final ArrayRemoveFunction ARRAY_REMOVE_FUNCTION = new ArrayRemoveFunction();
    private static final String FUNCTION_NAME = "array_remove";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(comparableTypeParameter("E")), "array<E>", ImmutableList.of("array<E>", "E"), false, false);

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
        return "Remove specified values from the given array";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, format("%s expects only one argument", FUNCTION_NAME));
        Type type = types.get("E");
        TypeSignature valueType = type.getTypeSignature();
        TypeSignature arrayType = parameterizedTypeName(StandardTypes.ARRAY, valueType);
        MethodHandle methodHandle = methodHandle(ArrayRemoveFunction.class, "remove", Type.class, Slice.class, type.getJavaType());
        Signature signature = new Signature(FUNCTION_NAME, arrayType, arrayType, valueType);
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle.bindTo(type), isDeterministic(), true, ImmutableList.of(false, false));
    }

    public static Slice remove(Type type, Slice array, Slice value)
    {
        return remove(type, array, createBlock(type, value));
    }

    public static Slice remove(Type type, Slice array, long value)
    {
        return remove(type, array, createBlock(type, value));
    }

    public static Slice remove(Type type, Slice array, double value)
    {
        return remove(type, array, createBlock(type, value));
    }

    public static Slice remove(Type type, Slice array, boolean value)
    {
        return remove(type, array, createBlock(type, value));
    }

    private static Slice remove(Type type, Slice array, Block valueBlock)
    {
        Block elementsBlock = readStructuralBlock(array);

        int sizeAfterRemove = 0;
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < elementsBlock.getPositionCount(); i++) {
            if (!type.equalTo(elementsBlock, i, valueBlock, 0)) {
                positions.add(i);
                sizeAfterRemove += type.getSlice(elementsBlock, i).length();
            }
        }
        if (elementsBlock.getPositionCount() == positions.size()) {
            return array;
        }

        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), sizeAfterRemove);

        for (int position : positions) {
            type.appendTo(elementsBlock, position, blockBuilder);
        }

        return buildStructuralSlice(blockBuilder);
    }
}
