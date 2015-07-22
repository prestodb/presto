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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ArrayElementAtFunction
        extends ParametricScalar
{
    public static final ArrayElementAtFunction ARRAY_ELEMENT_AT_FUNCTION = new ArrayElementAtFunction();
    private static final String FUNCTION_NAME = "element_at";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(typeParameter("E")), "E", ImmutableList.of("array<E>", "bigint"), false, false);
    private static final Map<Class<?>, MethodHandle> METHOD_HANDLES = ImmutableMap.<Class<?>, MethodHandle>builder()
            .put(boolean.class, methodHandle(ArrayElementAtFunction.class, "booleanElementAt", Type.class, Slice.class, long.class))
            .put(long.class, methodHandle(ArrayElementAtFunction.class, "longElementAt", Type.class, Slice.class, long.class))
            .put(double.class, methodHandle(ArrayElementAtFunction.class, "doubleElementAt", Type.class, Slice.class, long.class))
            .put(Slice.class, methodHandle(ArrayElementAtFunction.class, "sliceElementAt", Type.class, Slice.class, long.class))
            .put(void.class, methodHandle(ArrayElementAtFunction.class, "arrayWithUnknownType", Type.class, Slice.class, long.class))
            .build();

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
        return "Get element of array at given index";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, "Expected one type, got %s", types);
        final Type elementType = types.get("E");
        final MethodHandle methodHandle = METHOD_HANDLES.get(elementType.getJavaType()).bindTo(elementType);
        checkNotNull(methodHandle, "methodHandle is null");
        final Signature signature = new Signature(FUNCTION_NAME, elementType.getTypeSignature(), parameterizedTypeName("array", elementType.getTypeSignature()), parseTypeSignature("bigint"));
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), true, ImmutableList.of(false, false));
    }

    public static void arrayWithUnknownType(Type elementType, Slice array, long index)
    {
        final Block block = readStructuralBlock(array);
        checkedIndexToBlockPosition(block, index);
    }

    public static Long longElementAt(Type elementType, Slice array, long index)
    {
        final Block block = readStructuralBlock(array);
        final int position = checkedIndexToBlockPosition(block, index);
        if (block.isNull(position)) {
            return null;
        }

        return elementType.getLong(block, position);
    }

    public static Boolean booleanElementAt(Type elementType, Slice array, long index)
    {
        final Block block = readStructuralBlock(array);
        final int position = checkedIndexToBlockPosition(block, index);
        if (block.isNull(position)) {
            return null;
        }

        return elementType.getBoolean(block, position);
    }

    public static Double doubleElementAt(Type elementType, Slice array, long index)
    {
        final Block block = readStructuralBlock(array);
        final int position = checkedIndexToBlockPosition(block, index);
        if (block.isNull(position)) {
            return null;
        }

        return elementType.getDouble(block, position);
    }

    public static Slice sliceElementAt(Type elementType, Slice array, long index)
    {
        final Block block = readStructuralBlock(array);
        final int position = checkedIndexToBlockPosition(block, index);
        if (block.isNull(position)) {
            return null;
        }

        return elementType.getSlice(block, position);
    }

    private static int checkedIndexToBlockPosition(final Block block, final long index)
    {
        final int arrayLength = block.getPositionCount();
        if (index == 0 || Math.abs(index) > arrayLength) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Index out of bounds");
        }

        if (index > 0) {
            return Ints.checkedCast(index - 1);
        }
        else {
            return Ints.checkedCast(arrayLength + index);
        }
    }
}
