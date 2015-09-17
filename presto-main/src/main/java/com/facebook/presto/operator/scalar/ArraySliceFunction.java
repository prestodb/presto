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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionType.SCALAR;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class ArraySliceFunction
        implements ParametricFunction
{
    public static final ArraySliceFunction ARRAY_SLICE_FUNCTION = new ArraySliceFunction();
    private static final String FUNCTION_NAME = "slice";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, SCALAR, ImmutableList.of(typeParameter("E")), "array<E>", ImmutableList.of("array<E>", "bigint", "bigint"), false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArraySliceFunction.class, "slice", Type.class, Block.class, long.class, long.class);

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
        return "Subsets an array given an offset (1-indexed) and length";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("E");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(type);
        Signature signature = new Signature(FUNCTION_NAME,
                SCALAR,
                parameterizedTypeName("array", type.getTypeSignature()),
                parameterizedTypeName("array", type.getTypeSignature()), parseTypeSignature("bigint"), parseTypeSignature("bigint"));
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false, false, false));
    }

    public static Block slice(Type type, Block array, long fromIndex, long length)
    {
        checkCondition(length >= 0, INVALID_FUNCTION_ARGUMENT, "Invalid array length");
        checkCondition(fromIndex != 0, INVALID_FUNCTION_ARGUMENT, "Invalid start index");

        int size = array.getPositionCount();
        if (size == 0) {
            return array;
        }

        if (fromIndex < 0) {
            fromIndex = size + fromIndex + 1;
        }

        long toIndex = Math.min(fromIndex + length, size + 1);

        if (fromIndex >= toIndex || fromIndex < 0 || toIndex < 0) {
            return type.createBlockBuilder(new BlockBuilderStatus(), 0).build();
        }

        return array.getRegion((int) (fromIndex - 1), (int) (toIndex - fromIndex));
    }
}
