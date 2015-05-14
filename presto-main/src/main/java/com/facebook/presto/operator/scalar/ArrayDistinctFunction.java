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
import com.facebook.presto.operator.aggregation.SimpleTypedSet;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class ArrayDistinctFunction
        extends ParametricScalar
{
    public static final ArrayDistinctFunction ARRAY_DISTINCT_FUNCTION = new ArrayDistinctFunction();
    private static final String FUNCTION_NAME = "array_distinct";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(comparableTypeParameter("E")), "array<E>", ImmutableList.of("array<E>"), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayDistinctFunction.class, "distinct", Type.class, Slice.class);

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
        return "Remove duplicate values from the given array";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, format("%s expects only one argument", FUNCTION_NAME));
        Type type = types.get("E");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(type);
        Signature signature = new Signature(FUNCTION_NAME,
                parameterizedTypeName("array", type.getTypeSignature()),
                parameterizedTypeName("array", type.getTypeSignature()));
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false));
    }

    public static Slice distinct(Type type, Slice array)
    {
        Block elementsBlock = readStructuralBlock(array);
        if (elementsBlock.getPositionCount() == 0) {
            return array;
        }

        TypedSet typedSet = new SimpleTypedSet(type, elementsBlock.getPositionCount());
        BlockBuilder distinctElementBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), elementsBlock.getPositionCount());
        for (int i = 0; i < elementsBlock.getPositionCount(); i++) {
            if (!typedSet.contains(elementsBlock, i)) {
                typedSet.add(elementsBlock, i);
                type.appendTo(elementsBlock, i, distinctElementBlockBuilder);
            }
        }

        return buildStructuralSlice(distinctElementBlockBuilder);
    }
}
