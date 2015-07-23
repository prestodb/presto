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
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.OperatorType.SUBSCRIPT;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ArraySubscriptOperator
        extends ParametricOperator
{
    public static final ArraySubscriptOperator ARRAY_SUBSCRIPT = new ArraySubscriptOperator();

    private static final MethodHandle METHOD_HANDLE_UNKNOWN = methodHandle(ArraySubscriptOperator.class, "arrayWithUnknownType", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(ArraySubscriptOperator.class, "booleanSubscript", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(ArraySubscriptOperator.class, "longSubscript", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(ArraySubscriptOperator.class, "doubleSubscript", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(ArraySubscriptOperator.class, "sliceSubscript", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(ArraySubscriptOperator.class, "objectSubscript", Type.class, Block.class, long.class);

    protected ArraySubscriptOperator()
    {
        super(SUBSCRIPT, ImmutableList.of(typeParameter("E")), "E", ImmutableList.of("array<E>", "bigint"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, "Expected one type, got %s", types);
        Type elementType = types.get("E");

        MethodHandle methodHandle;
        if (elementType.getJavaType() == void.class) {
            methodHandle = METHOD_HANDLE_UNKNOWN;
        }
        else if (elementType.getJavaType() == boolean.class) {
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
        checkNotNull(methodHandle, "methodHandle is null");
        return new FunctionInfo(Signature.internalOperator(SUBSCRIPT.name(), elementType.getTypeSignature(), parameterizedTypeName("array", elementType.getTypeSignature()), parseTypeSignature(StandardTypes.BIGINT)), "Array subscript", true, methodHandle, true, true, ImmutableList.of(false, false));
    }

    public static void arrayWithUnknownType(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
    }

    public static Long longSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = Ints.checkedCast(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getLong(array, position);
    }

    public static Boolean booleanSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = Ints.checkedCast(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getBoolean(array, position);
    }

    public static Double doubleSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = Ints.checkedCast(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getDouble(array, position);
    }

    public static Slice sliceSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = Ints.checkedCast(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getSlice(array, position);
    }

    public static Object objectSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = Ints.checkedCast(index - 1);
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
