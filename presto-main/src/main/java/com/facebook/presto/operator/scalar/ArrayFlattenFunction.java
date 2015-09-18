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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ArrayFlattenFunction
        extends ParametricScalar
{
    public static final ArrayFlattenFunction ARRAY_FLATTEN_FUNCTION = new ArrayFlattenFunction();
    private static final String FUNCTION_NAME = "flatten";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(typeParameter("E")), "array<E>", ImmutableList.of("array<array<E>>"), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayFlattenFunction.class, FUNCTION_NAME, Type.class, Block.class);

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
        return "Flattens a given array";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type elementType = types.get("E");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(elementType);
        TypeSignature typeSignature = parameterizedTypeName("array", types.get("E").getTypeSignature());
        Signature signature = new Signature(FUNCTION_NAME, typeSignature, parameterizedTypeName("array", typeSignature));
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false));
    }

    public static Block flatten(Type type, Block array)
    {
        BlockBuilder resultBlockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), array.getSizeInBytes());
        for (int i = 0; i < array.getPositionCount(); ++i) {
            Block subBlock = array.getObject(i, Block.class);
            for (int j = 0; j < subBlock.getPositionCount(); ++j) {
                type.appendTo(subBlock, j, resultBlockBuilder);
            }
        }
        return resultBlockBuilder.build();
    }
}
