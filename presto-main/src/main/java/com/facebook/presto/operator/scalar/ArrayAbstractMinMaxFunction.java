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

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class ArrayAbstractMinMaxFunction
        extends ParametricScalar
{
    private final OperatorType operatorType;
    private final String functionName;
    private final Signature signature;

    private static final Map<Class<?>, MethodHandle> METHOD_HANDLES = ImmutableMap.<Class<?>, MethodHandle>builder()
            .put(boolean.class, methodHandle(ArrayAbstractMinMaxFunction.class, "booleanArrayMinMax", MethodHandle.class, Type.class, Slice.class))
            .put(long.class, methodHandle(ArrayAbstractMinMaxFunction.class, "longArrayMinMax", MethodHandle.class, Type.class, Slice.class))
            .put(double.class, methodHandle(ArrayAbstractMinMaxFunction.class, "doubleArrayMinMax", MethodHandle.class, Type.class, Slice.class))
            .put(Slice.class, methodHandle(ArrayAbstractMinMaxFunction.class, "sliceArrayMinMax", MethodHandle.class, Type.class, Slice.class))
            .put(void.class, methodHandle(ArrayAbstractMinMaxFunction.class, "arrayWithUnknownType", MethodHandle.class, Type.class, Slice.class))
            .build();

    public ArrayAbstractMinMaxFunction(OperatorType operatorType, String functionName)
    {
        this.operatorType = operatorType;
        this.functionName = functionName;
        this.signature = new Signature(functionName, ImmutableList.of(comparableTypeParameter("E")), "E", ImmutableList.of("array<E>"), false, false);
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
        return "Get min value of array";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, "Expected one type, got %s", types);
        Type elementType = types.get("E");
        checkArgument(elementType.isOrderable(), "Type must be orderable");

        MethodHandle compareMethodHandle = functionRegistry.resolveOperator(operatorType, ImmutableList.of(elementType, elementType)).getMethodHandle();
        MethodHandle methodHandle = METHOD_HANDLES.get(elementType.getJavaType()).bindTo(compareMethodHandle).bindTo(elementType);
        requireNonNull(methodHandle, "methodHandle is null");

        Signature signature = new Signature(functionName, elementType.getTypeSignature(), parameterizedTypeName(StandardTypes.ARRAY, elementType.getTypeSignature()));

        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false));
    }

    public static void arrayWithUnknownType(MethodHandle compareMethodHandle, Type elementType, Slice array)
    {
    }

    public static Long longArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Slice array)
    {
        try {
            Block block = readStructuralBlock(array);
            int currentPosition = 0;
            for (int i = 1; i < block.getPositionCount(); i++) {
                if ((boolean) compareMethodHandle.invokeExact(elementType.getLong(block, i), elementType.getLong(block, currentPosition))) {
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

    public static Boolean booleanArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Slice array)
    {
        try {
            Block block = readStructuralBlock(array);
            int currentPosition = 0;
            for (int i = 1; i < block.getPositionCount(); i++) {
                if ((boolean) compareMethodHandle.invokeExact(elementType.getBoolean(block, i), elementType.getBoolean(block, currentPosition))) {
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

    public static Double doubleArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Slice array)
    {
        try {
            Block block = readStructuralBlock(array);
            int currentPosition = 0;
            for (int i = 1; i < block.getPositionCount(); i++) {
                if ((boolean) compareMethodHandle.invokeExact(elementType.getDouble(block, i), elementType.getDouble(block, currentPosition))) {
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
            int currentPosition = 0;
            for (int i = 1; i < block.getPositionCount(); i++) {
                if ((boolean) compareMethodHandle.invokeExact(elementType.getSlice(block, i), elementType.getSlice(block, currentPosition))) {
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
}
