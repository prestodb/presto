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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MultisetToArrayCast
        extends SqlOperator
{
    public static final MultisetToArrayCast MULTISET_TO_ARRAY_CAST = new MultisetToArrayCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(MultisetToArrayCast.class, "toArray", Type.class, Block.class);

    private MultisetToArrayCast()
    {
        super(CAST, ImmutableList.of(comparableTypeParameter("E")), ImmutableList.of(), "array<E>", ImmutableList.of("multiset<E>"));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type elementType = boundVariables.getTypeVariable("E");
        MethodHandle method = METHOD_HANDLE.bindTo(elementType);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), method, isDeterministic());
    }

    // in the first version because we simply use an array to represent multiset, just return the backing block storage
    public static Block toArray(Type type, Block multiset)
    {
        return multiset;
    }
}
