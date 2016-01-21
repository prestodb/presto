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

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagateIfInstanceOf;

public abstract class AbstractArrayMinMaxFunction
        extends SqlScalarFunction
{
    private final OperatorType operatorType;
    private final String description;

    private static final Map<Class<?>, MethodHandle> METHOD_HANDLES = ImmutableMap.<Class<?>, MethodHandle>builder()
            .put(boolean.class, methodHandle(AbstractArrayMinMaxFunction.class, "booleanArrayMinMax", MethodHandle.class, Type.class, Block.class))
            .put(long.class, methodHandle(AbstractArrayMinMaxFunction.class, "longArrayMinMax", MethodHandle.class, Type.class, Block.class))
            .put(double.class, methodHandle(AbstractArrayMinMaxFunction.class, "doubleArrayMinMax", MethodHandle.class, Type.class, Block.class))
            .put(Slice.class, methodHandle(AbstractArrayMinMaxFunction.class, "sliceArrayMinMax", MethodHandle.class, Type.class, Block.class))
            .put(void.class, methodHandle(AbstractArrayMinMaxFunction.class, "arrayWithUnknownType", MethodHandle.class, Type.class, Block.class))
            .build();
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(AbstractArrayMinMaxFunction.class, "objectArrayMinMax", MethodHandle.class, Type.class, Block.class);

    protected AbstractArrayMinMaxFunction(OperatorType operatorType, String functionName, String description)
    {
        super(functionName, ImmutableList.of(orderableTypeParameter("E")), "E", ImmutableList.of("array(E)"));
        this.operatorType = operatorType;
        this.description = description;
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
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, "Expected one type, got %s", types);
        Type elementType = types.get("E");
        checkArgument(elementType.isOrderable(), "Type must be orderable");

        MethodHandle compareMethodHandle = functionRegistry.getScalarFunctionImplementation(internalOperator(operatorType, BOOLEAN, ImmutableList.of(elementType, elementType))).getMethodHandle();
        MethodHandle methodHandle = METHOD_HANDLES.get(elementType.getJavaType());
        if (methodHandle == null) {
            methodHandle = METHOD_HANDLE_OBJECT;
            compareMethodHandle = compareMethodHandle.asType(compareMethodHandle.type().changeParameterType(0, Object.class).changeParameterType(1, Object.class));
        }
        methodHandle = methodHandle.bindTo(compareMethodHandle).bindTo(elementType);

        return new ScalarFunctionImplementation(true, ImmutableList.of(false), methodHandle, isDeterministic());
    }

    @UsedByGeneratedCode
    public static void arrayWithUnknownType(MethodHandle compareMethodHandle, Type elementType, Block block)
    {
    }

    @UsedByGeneratedCode
    public static Long longArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Block block)
    {
        try {
            if (block.getPositionCount() == 0) {
                return null;
            }

            long selectedValue = elementType.getLong(block, 0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    return null;
                }
                long value = elementType.getLong(block, i);
                if ((boolean) compareMethodHandle.invokeExact(value, selectedValue)) {
                    selectedValue = value;
                }
            }

            return selectedValue;
        }
        catch (Throwable t) {
            propagateIfInstanceOf(t, Error.class);
            propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(INTERNAL_ERROR, t);
        }
    }

    @UsedByGeneratedCode
    public static Boolean booleanArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Block block)
    {
        try {
            if (block.getPositionCount() == 0) {
                return null;
            }

            boolean selectedValue = elementType.getBoolean(block, 0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    return null;
                }
                boolean value = elementType.getBoolean(block, i);
                if ((boolean) compareMethodHandle.invokeExact(value, selectedValue)) {
                    selectedValue = value;
                }
            }

            return selectedValue;
        }
        catch (Throwable t) {
            propagateIfInstanceOf(t, Error.class);
            propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(INTERNAL_ERROR, t);
        }
    }

    @UsedByGeneratedCode
    public static Double doubleArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Block block)
    {
        try {
            if (block.getPositionCount() == 0) {
                return null;
            }

            double selectedValue = elementType.getDouble(block, 0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    return null;
                }
                double value = elementType.getDouble(block, i);
                if ((boolean) compareMethodHandle.invokeExact(value, selectedValue)) {
                    selectedValue = value;
                }
            }

            return selectedValue;
        }
        catch (Throwable t) {
            propagateIfInstanceOf(t, Error.class);
            propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(INTERNAL_ERROR, t);
        }
    }

    @UsedByGeneratedCode
    public static Slice sliceArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Block block)
    {
        try {
            if (block.getPositionCount() == 0) {
                return null;
            }

            Slice selectedValue = elementType.getSlice(block, 0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    return null;
                }
                Slice value = elementType.getSlice(block, i);
                if ((boolean) compareMethodHandle.invokeExact(value, selectedValue)) {
                    selectedValue = value;
                }
            }

            return selectedValue;
        }
        catch (Throwable t) {
            propagateIfInstanceOf(t, Error.class);
            propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(INTERNAL_ERROR, t);
        }
    }

    @UsedByGeneratedCode
    public static Object objectArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Block block)
    {
        try {
            if (block.getPositionCount() == 0) {
                return null;
            }

            Object selectedValue = elementType.getObject(block, 0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    return null;
                }
                Object value = elementType.getObject(block, i);
                if ((boolean) compareMethodHandle.invokeExact(value, selectedValue)) {
                    selectedValue = value;
                }
            }

            return selectedValue;
        }
        catch (Throwable t) {
            propagateIfInstanceOf(t, Error.class);
            propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(INTERNAL_ERROR, t);
        }
    }
}
