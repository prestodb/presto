package com.facebook.presto.operator.scalar;
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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ArrayNotEqualOperator
        extends SqlOperator
{
    public static final ArrayNotEqualOperator ARRAY_NOT_EQUAL = new ArrayNotEqualOperator();
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayNotEqualOperator.class, "notEqual", MethodHandle.class, Type.class, Block.class, Block.class);

    private ArrayNotEqualOperator()
    {
        super(NOT_EQUAL, ImmutableList.of(comparableTypeParameter("T")), StandardTypes.BOOLEAN, ImmutableList.of("array<T>", "array<T>"));
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type elementType = types.get("T");
        MethodHandle equalsFunction = functionRegistry.getScalarFunctionImplementation(internalOperator(EQUAL, BOOLEAN, ImmutableList.of(elementType, elementType))).getMethodHandle();
        MethodHandle method = METHOD_HANDLE.bindTo(equalsFunction).bindTo(elementType);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false, false), method, isDeterministic());
    }

    public static boolean notEqual(MethodHandle equalsFunction, Type type, Block left, Block right)
    {
        return !ArrayEqualOperator.equals(equalsFunction, type, left, right);
    }
}
