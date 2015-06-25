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
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class ArraySliceFunction
        extends ParametricScalar
{
    public static final ArraySliceFunction ARRAY_SLICE_FUNCTION = new ArraySliceFunction();
    private static final String FUNCTION_NAME = "slice";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(typeParameter("E")), "array<E>", ImmutableList.of("array<E>", "bigint", "bigint"), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArraySliceFunction.class, "slice", Type.class, Slice.class, long.class, long.class);

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
        return "Subsets an array given an offset (zero-indexed) and length";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("E");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(type);
        Signature signature = new Signature(FUNCTION_NAME,
                parameterizedTypeName("array", type.getTypeSignature()),
                parameterizedTypeName("array", type.getTypeSignature()), parseTypeSignature("bigint"), parseTypeSignature("bigint"));
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false, false, false));
    }

    public static Slice slice(Type type, Slice array, long offset, long length)
    {
        Block elementsBlock = readStructuralBlock(array);
        int size = elementsBlock.getPositionCount();
        if (size == 0) {
            return array;
        }

        checkCondition(length >= 0, INVALID_FUNCTION_ARGUMENT, "Invalid array length");

        if (offset < 0) {
            offset = size + offset;
        }

        long toIndex = Math.min(offset + length, size);

        if (offset >= toIndex || offset < 0 || toIndex < 0) {
            return buildStructuralSlice(type.createBlockBuilder(new BlockBuilderStatus(), 0).build());
        }

        return buildStructuralSlice(elementsBlock.getRegion((int) offset, (int) (toIndex - offset)));
    }
}
