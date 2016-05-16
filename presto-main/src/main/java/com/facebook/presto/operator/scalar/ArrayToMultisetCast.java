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
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ArrayToMultisetCast
        extends SqlOperator
{
    public static final ArrayToMultisetCast ARRAY_TO_MULTISET_CAST = new ArrayToMultisetCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayToMultisetCast.class, "toMultiset", Type.class, Block.class);

    private ArrayToMultisetCast()
    {
        super(CAST, ImmutableList.of(typeVariable("E")), ImmutableList.of(), "multiset(E)", ImmutableList.of("array(E)"));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type elementType = boundVariables.getTypeVariable("E");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(elementType);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), methodHandle, isDeterministic());
    }

    // in the first version, multiset is implemented using array
    public static Block toMultiset(Type elementType, Block array)
    {
        return array;
    }
}
