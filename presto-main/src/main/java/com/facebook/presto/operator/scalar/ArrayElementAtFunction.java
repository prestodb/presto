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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
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
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ArrayElementAtFunction
        extends SqlScalarFunction
{
    public static final ArrayElementAtFunction ARRAY_ELEMENT_AT_FUNCTION = new ArrayElementAtFunction();
    private static final String FUNCTION_NAME = "element_at";
    private static final Map<Class<?>, MethodHandle> METHOD_HANDLES = ImmutableMap.<Class<?>, MethodHandle>builder()
            .put(boolean.class, methodHandle(ArrayElementAtFunction.class, "booleanElementAt", Type.class, Block.class, long.class))
            .put(long.class, methodHandle(ArrayElementAtFunction.class, "longElementAt", Type.class, Block.class, long.class))
            .put(double.class, methodHandle(ArrayElementAtFunction.class, "doubleElementAt", Type.class, Block.class, long.class))
            .put(Slice.class, methodHandle(ArrayElementAtFunction.class, "sliceElementAt", Type.class, Block.class, long.class))
            .put(void.class, methodHandle(ArrayElementAtFunction.class, "voidElementAt", Type.class, Block.class, long.class))
            .build();
    private static final MethodHandle OBJECT_METHOD_HANDLE = methodHandle(ArrayElementAtFunction.class, "objectElementAt", Type.class, Block.class, long.class);

    public ArrayElementAtFunction()
    {
        super(FUNCTION_NAME, ImmutableList.of(typeParameter("E")), "E", ImmutableList.of("array(E)", "bigint"));
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
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, "Expected one type, got %s", types);
        Type elementType = types.get("E");

        MethodHandle methodHandle;
        if (METHOD_HANDLES.containsKey(elementType.getJavaType())) {
            methodHandle = METHOD_HANDLES.get(elementType.getJavaType());
        }
        else {
            checkArgument(!elementType.getJavaType().isPrimitive(), "Unsupported primitive type: " + elementType.getJavaType());
            methodHandle = OBJECT_METHOD_HANDLE;
        }
        requireNonNull(methodHandle, "methodHandle is null");
        methodHandle = methodHandle.bindTo(elementType);
        return new ScalarFunctionImplementation(true, ImmutableList.of(false, false), methodHandle, isDeterministic());
    }

    public static void voidElementAt(Type elementType, Block array, long index)
    {
        checkedIndexToBlockPosition(array, index);
    }

    public static Long longElementAt(Type elementType, Block array, long index)
    {
        int position = checkedIndexToBlockPosition(array, index);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getLong(array, position);
    }

    public static Boolean booleanElementAt(Type elementType, Block array, long index)
    {
        int position = checkedIndexToBlockPosition(array, index);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getBoolean(array, position);
    }

    public static Double doubleElementAt(Type elementType, Block array, long index)
    {
        int position = checkedIndexToBlockPosition(array, index);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getDouble(array, position);
    }

    public static Slice sliceElementAt(Type elementType, Block array, long index)
    {
        int position = checkedIndexToBlockPosition(array, index);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getSlice(array, position);
    }

    public static Object objectElementAt(Type elementType, Block array, long index)
    {
        int position = checkedIndexToBlockPosition(array, index);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getObject(array, position);
    }

    private static int checkedIndexToBlockPosition(Block block, long index)
    {
        int arrayLength = block.getPositionCount();
        if (index == 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "SQL array indices start at 1");
        }
        if (Math.abs(index) > arrayLength) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Array subscript out of bounds");
        }

        if (index > 0) {
            return Ints.checkedCast(index - 1);
        }
        else {
            return Ints.checkedCast(arrayLength + index);
        }
    }
}
