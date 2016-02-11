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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class ArrayContains
        extends SqlScalarFunction
{
    public static final ArrayContains ARRAY_CONTAINS = new ArrayContains();
    private static final String FUNCTION_NAME = "contains";
    private static final MethodHandle METHOD_HANDLE_UNKNOWN = methodHandle(ArrayContains.class, "arrayWithUnknownType", Type.class, MethodHandle.class, Block.class, Void.class);

    public ArrayContains()
    {
        super(FUNCTION_NAME, ImmutableList.of(comparableTypeParameter("T")), ImmutableList.of(), StandardTypes.BOOLEAN, ImmutableList.of("array(T)", "T"));
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
        return "Determines whether given value exists in the array";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = boundVariables.getTypeVariable("T");

        MethodHandle methodHandle;
        MethodHandle equalsHandle = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(type, type))).getMethodHandle();

        List<Boolean> nullableArguments;
        if (type.getJavaType() == void.class) {
            nullableArguments = ImmutableList.of(false, true);
            methodHandle = METHOD_HANDLE_UNKNOWN;
        }
        else {
            nullableArguments = ImmutableList.of(false, false);
            methodHandle = methodHandle(ArrayContains.class, "contains", Type.class, MethodHandle.class, Block.class, type.getJavaType());
        }

        return new ScalarFunctionImplementation(true, nullableArguments, methodHandle.bindTo(type).bindTo(equalsHandle), isDeterministic());
    }

    public static Boolean arrayWithUnknownType(Type elementType, MethodHandle equals, Block arrayBlock, Void value)
    {
        return null;
    }

    public static Boolean contains(Type elementType, MethodHandle equals, Block arrayBlock, Block value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                if ((boolean) equals.invokeExact((Block) elementType.getObject(arrayBlock, i), value)) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    public static Boolean contains(Type elementType, MethodHandle equals, Block arrayBlock, Slice value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                if ((boolean) equals.invokeExact(elementType.getSlice(arrayBlock, i), value)) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    public static Boolean contains(Type elementType, MethodHandle equals, Block arrayBlock, long value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                if ((boolean) equals.invokeExact(elementType.getLong(arrayBlock, i), value)) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    public static Boolean contains(Type elementType, MethodHandle equals, Block arrayBlock, boolean value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                if ((boolean) equals.invokeExact(elementType.getBoolean(arrayBlock, i), value)) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    public static Boolean contains(Type elementType, MethodHandle equals, Block arrayBlock, double value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                if ((boolean) equals.invokeExact(elementType.getDouble(arrayBlock, i), value)) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }
}
