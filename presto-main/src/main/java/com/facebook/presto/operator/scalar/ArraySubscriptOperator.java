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
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.OperatorType.SUBSCRIPT;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ArraySubscriptOperator
        extends ParametricOperator
{
    public static final ArraySubscriptOperator ARRAY_SUBSCRIPT = new ArraySubscriptOperator();

    private static final Map<Class<?>, MethodHandle> METHOD_HANDLES = ImmutableMap.<Class<?>, MethodHandle>builder()
            .put(boolean.class, methodHandle(ArraySubscriptOperator.class, "booleanSubscript", Type.class, Slice.class, long.class))
            .put(long.class, methodHandle(ArraySubscriptOperator.class, "longSubscript", Type.class, Slice.class, long.class))
            .put(void.class, methodHandle(ArraySubscriptOperator.class, "arrayWithUnknownType", Type.class, Slice.class, long.class))
            .put(double.class, methodHandle(ArraySubscriptOperator.class, "doubleSubscript", Type.class, Slice.class, long.class))
            .put(Slice.class, methodHandle(ArraySubscriptOperator.class, "sliceSubscript", Type.class, Slice.class, long.class))
            .build();

    protected ArraySubscriptOperator()
    {
        super(SUBSCRIPT, ImmutableList.of(typeParameter("E")), "E", ImmutableList.of("array<E>", "bigint"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, "Expected one type, got %s", types);
        Type elementType = types.get("E");

        MethodHandle methodHandle = METHOD_HANDLES.get(elementType.getJavaType());
        methodHandle = methodHandle.bindTo(elementType);
        checkNotNull(methodHandle, "methodHandle is null");
        return new FunctionInfo(Signature.internalOperator(SUBSCRIPT.name(), elementType.getTypeSignature(), parameterizedTypeName("array", elementType.getTypeSignature()), parseTypeSignature(StandardTypes.BIGINT)), "Array subscript", true, methodHandle, true, true, ImmutableList.of(false, false));
    }

    public static void arrayWithUnknownType(Type elementType, Slice array, long index)
    {
        readBlockAndCheckIndex(array, index);
    }

    public static Long longSubscript(Type elementType, Slice array, long index)
    {
        Block block = readBlockAndCheckIndex(array, index);
        int position = Ints.checkedCast(index - 1);
        if (block.isNull(position)) {
            return null;
        }

        return elementType.getLong(block, position);
    }

    public static Boolean booleanSubscript(Type elementType, Slice array, long index)
    {
        Block block = readBlockAndCheckIndex(array, index);
        int position = Ints.checkedCast(index - 1);
        if (block.isNull(position)) {
            return null;
        }

        return elementType.getBoolean(block, position);
    }

    public static Double doubleSubscript(Type elementType, Slice array, long index)
    {
        Block block = readBlockAndCheckIndex(array, index);
        int position = Ints.checkedCast(index - 1);
        if (block.isNull(position)) {
            return null;
        }

        return elementType.getDouble(block, position);
    }

    public static Slice sliceSubscript(Type elementType, Slice array, long index)
    {
        Block block = readBlockAndCheckIndex(array, index);
        int position = Ints.checkedCast(index - 1);
        if (block.isNull(position)) {
            return null;
        }

        return elementType.getSlice(block, position);
    }

    public static Block readBlockAndCheckIndex(Slice array, long index)
    {
        if (index == 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "SQL array indices start at 1");
        }
        if (index < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Array subscript is negative");
        }
        Block block = readStructuralBlock(array);
        if (index > block.getPositionCount()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Array subscript out of bounds");
        }
        return block;
    }
}
