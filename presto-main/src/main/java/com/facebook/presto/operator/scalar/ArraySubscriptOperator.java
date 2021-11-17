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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ArraySubscriptOperator
        extends SqlOperator
{
    public static final ArraySubscriptOperator ARRAY_SUBSCRIPT = new ArraySubscriptOperator();

    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(ArraySubscriptOperator.class, "booleanSubscript", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(ArraySubscriptOperator.class, "longSubscript", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(ArraySubscriptOperator.class, "doubleSubscript", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(ArraySubscriptOperator.class, "sliceSubscript", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(ArraySubscriptOperator.class, "objectSubscript", Type.class, Block.class, long.class);

    protected ArraySubscriptOperator()
    {
        super(SUBSCRIPT,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("E"),
                ImmutableList.of(parseTypeSignature("array(E)"), parseTypeSignature("bigint")));
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        checkArgument(boundVariables.getTypeVariables().size() == 1, "Expected one type, got %s", boundVariables.getTypeVariables());
        Type elementType = boundVariables.getTypeVariable("E");

        MethodHandle methodHandle;
        if (elementType.getJavaType() == boolean.class) {
            methodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else if (elementType.getJavaType() == long.class) {
            methodHandle = METHOD_HANDLE_LONG;
        }
        else if (elementType.getJavaType() == double.class) {
            methodHandle = METHOD_HANDLE_DOUBLE;
        }
        else if (elementType.getJavaType() == Slice.class) {
            methodHandle = METHOD_HANDLE_SLICE;
        }
        else {
            methodHandle = METHOD_HANDLE_OBJECT;
        }
        methodHandle = methodHandle.bindTo(elementType);
        requireNonNull(methodHandle, "methodHandle is null");
        return new BuiltInScalarFunctionImplementation(
                true,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Long longSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = toIntExact(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getLong(array, position);
    }

    @UsedByGeneratedCode
    public static Boolean booleanSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = toIntExact(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getBoolean(array, position);
    }

    @UsedByGeneratedCode
    public static Double doubleSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = toIntExact(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getDouble(array, position);
    }

    @UsedByGeneratedCode
    public static Slice sliceSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = toIntExact(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getSlice(array, position);
    }

    @UsedByGeneratedCode
    public static Object objectSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = toIntExact(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getObject(array, position);
    }

    public static void checkArrayIndex(long index)
    {
        if (index == 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "SQL array indices start at 1");
        }
        if (index < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Array subscript is negative");
        }
    }

    public static void checkIndex(Block array, long index)
    {
        checkArrayIndex(index);
        if (index > array.getPositionCount()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Array subscript out of bounds");
        }
    }
}
