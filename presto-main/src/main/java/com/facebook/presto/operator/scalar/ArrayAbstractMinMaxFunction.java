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
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public abstract class ArrayAbstractMinMaxFunction
        extends ParametricScalar
{
    private final OperatorType operatorType;
    private final String functionName;
    private final Signature signature;
    private final String description;

    private static final Map<Class<?>, MethodHandle> METHOD_HANDLES = ImmutableMap.<Class<?>, MethodHandle>builder()
            .put(boolean.class, methodHandle(ArrayAbstractMinMaxFunction.class, "booleanArrayMinMax", MethodHandle.class, Type.class, Slice.class))
            .put(long.class, methodHandle(ArrayAbstractMinMaxFunction.class, "longArrayMinMax", MethodHandle.class, Type.class, Slice.class))
            .put(double.class, methodHandle(ArrayAbstractMinMaxFunction.class, "doubleArrayMinMax", MethodHandle.class, Type.class, Slice.class))
            .put(Slice.class, methodHandle(ArrayAbstractMinMaxFunction.class, "sliceArrayMinMax", MethodHandle.class, Type.class, Slice.class))
            .put(void.class, methodHandle(ArrayAbstractMinMaxFunction.class, "arrayWithUnknownType", MethodHandle.class, Type.class, Slice.class))
            .build();

    public ArrayAbstractMinMaxFunction(OperatorType operatorType, String functionName, String description)
    {
        this.operatorType = operatorType;
        this.functionName = functionName;
        this.signature = new Signature(functionName, ImmutableList.of(orderableTypeParameter("E")), "E", ImmutableList.of("array<E>"), false, false);
        this.description = description;
    }

    @Override
    public Signature getSignature()
    {
        return signature;
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
        return description;
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, "Expected one type, got %s", types);
        Type elementType = types.get("E");
        checkArgument(elementType.isOrderable(), "Type must be orderable");

        MethodHandle compareMethodHandle = functionRegistry.resolveOperator(operatorType, ImmutableList.of(elementType, elementType)).getMethodHandle();
        MethodHandle methodHandle = METHOD_HANDLES.get(elementType.getJavaType());
        if (methodHandle == null) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Argument type to array_min/max unsupported");
        }
        methodHandle = methodHandle.bindTo(compareMethodHandle).bindTo(elementType);

        Signature signature = new Signature(functionName, elementType.getTypeSignature(), parameterizedTypeName(StandardTypes.ARRAY, elementType.getTypeSignature()));

        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false));
    }

    public static void arrayWithUnknownType(MethodHandle compareMethodHandle, Type elementType, Slice array)
    {
    }

    public static long longArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Slice array)
    {
        try {
            Block block = readStructuralBlock(array);

            int currentPosition = firstNonNullPosition(block);
            for (int i = currentPosition + 1; i < block.getPositionCount(); i++) {
                if (!block.isNull(i) && (boolean) compareMethodHandle.invokeExact(elementType.getLong(block, i), elementType.getLong(block, currentPosition))) {
                    currentPosition = i;
                }
            }

            return elementType.getLong(block, currentPosition);
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(INTERNAL_ERROR, t);
        }
    }

    public static boolean booleanArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Slice array)
    {
        try {
            Block block = readStructuralBlock(array);
            int currentPosition = firstNonNullPosition(block);
            for (int i = currentPosition + 1; i < block.getPositionCount(); i++) {
                if (!block.isNull(i) && (boolean) compareMethodHandle.invokeExact(elementType.getBoolean(block, i), elementType.getBoolean(block, currentPosition))) {
                    currentPosition = i;
                }
            }

            return elementType.getBoolean(block, currentPosition);
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(INTERNAL_ERROR, t);
        }
    }

    public static double doubleArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Slice array)
    {
        try {
            Block block = readStructuralBlock(array);
            int currentPosition = firstNonNullPosition(block);
            for (int i = currentPosition + 1; i < block.getPositionCount(); i++) {
                if (!block.isNull(i) && (boolean) compareMethodHandle.invokeExact(elementType.getDouble(block, i), elementType.getDouble(block, currentPosition))) {
                    currentPosition = i;
                }
            }

            return elementType.getDouble(block, currentPosition);
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(INTERNAL_ERROR, t);
        }
    }

    public static Slice sliceArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Slice array)
    {
        try {
            Block block = readStructuralBlock(array);
            int currentPosition = firstNonNullPosition(block);
            for (int i = currentPosition + 1; i < block.getPositionCount(); i++) {
                if (!block.isNull(i) && (boolean) compareMethodHandle.invokeExact(elementType.getSlice(block, i), elementType.getSlice(block, currentPosition))) {
                    currentPosition = i;
                }
            }

            return elementType.getSlice(block, currentPosition);
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(INTERNAL_ERROR, t);
        }
    }

    private static int firstNonNullPosition(Block block)
    {
        // Block should have at least one non-null element
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!block.isNull(i)) {
                return i;
            }
        }
        throw new PrestoException(INTERNAL_ERROR, "All elements in block is null.");
    }
}
