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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.SUBSCRIPT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;
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
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
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
        return new ScalarFunctionImplementation(
                true,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
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
