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

import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.type.ArrayType.ARRAY_NULL_ELEMENT_MSG;
import static com.facebook.presto.type.TypeUtils.checkElementNotNull;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ArrayHashCodeOperator
        extends SqlOperator
{
    public static final ArrayHashCodeOperator ARRAY_HASH_CODE = new ArrayHashCodeOperator();
    public static final MethodHandle METHOD_HANDLE = methodHandle(ArrayHashCodeOperator.class, "hash", Type.class, Block.class);

    private ArrayHashCodeOperator()
    {
        super(HASH_CODE, ImmutableList.of(comparableTypeParameter("T")), StandardTypes.BIGINT, ImmutableList.of("array(T)"));
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("T");
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), METHOD_HANDLE.bindTo(type), isDeterministic());
    }

    public static long hash(Type type, Block block)
    {
        int hash = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            checkElementNotNull(block.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            // TODO: This should be migrated away from Type.hash and invoke a MethodHandle for the hash code operator instead
            hash = (int) CombineHashFunction.getHash(hash, type.hash(block, i));
        }
        return hash;
    }
}
