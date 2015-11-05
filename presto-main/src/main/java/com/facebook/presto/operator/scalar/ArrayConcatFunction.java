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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ArrayConcatFunction
        extends SqlScalarFunction
{
    public static final ArrayConcatFunction ARRAY_CONCAT_FUNCTION = new ArrayConcatFunction();
    private static final String FUNCTION_NAME = "concat";
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayConcatUtils.class, FUNCTION_NAME, Type.class, Block.class, Block.class);

    public ArrayConcatFunction()
    {
        super(FUNCTION_NAME, ImmutableList.of(typeParameter("E")), "array<E>", ImmutableList.of("array<E>", "array<E>"));
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
        return "Concatenates given arrays";
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type elementType = types.get("E");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(elementType);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false, false), methodHandle, isDeterministic());
    }
}
