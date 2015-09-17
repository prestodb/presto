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
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionType.SCALAR;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class ArrayDistinctFunction
        implements ParametricFunction
{
    public static final ArrayDistinctFunction ARRAY_DISTINCT_FUNCTION = new ArrayDistinctFunction();
    private static final String FUNCTION_NAME = "array_distinct";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, SCALAR, ImmutableList.of(comparableTypeParameter("E")), "array<E>", ImmutableList.of("array<E>"), false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayDistinctFunction.class, "distinct", Type.class, Block.class);

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
                SCALAR,
                parameterizedTypeName("array", type.getTypeSignature()),
                parameterizedTypeName("array", type.getTypeSignature()));
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false));
    }

    public static Block distinct(Type type, Block array)
    {
        if (array.getPositionCount() == 0) {
            return array;
        }

        TypedSet typedSet = new TypedSet(type, array.getPositionCount());
        BlockBuilder distinctElementBlockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), array.getPositionCount());
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (!typedSet.contains(array, i)) {
                typedSet.add(array, i);
                type.appendTo(array, i, distinctElementBlockBuilder);
            }
        }

        return distinctElementBlockBuilder.build();
    }
}
