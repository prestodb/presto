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
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.hashPosition;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MapHashCodeOperator
        extends SqlOperator
{
    public static final MapHashCodeOperator MAP_HASH_CODE = new MapHashCodeOperator();
    private static final MethodHandle METHOD_HANDLE = methodHandle(MapHashCodeOperator.class, "hash", MethodHandle.class, MethodHandle.class, Type.class, Type.class, Block.class);

    private MapHashCodeOperator()
    {
        super(HASH_CODE,
                ImmutableList.of(comparableTypeParameter("K"), comparableTypeParameter("V")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.BIGINT),
                ImmutableList.of(parseTypeSignature("map(K,V)")));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");

        MethodHandle keyHashCodeFunction = functionRegistry.getScalarFunctionImplementation(internalOperator(HASH_CODE, BIGINT, ImmutableList.of(keyType))).getMethodHandle();
        MethodHandle valueHashCodeFunction = functionRegistry.getScalarFunctionImplementation(internalOperator(HASH_CODE, BIGINT, ImmutableList.of(valueType))).getMethodHandle();

        MethodHandle method = METHOD_HANDLE.bindTo(keyHashCodeFunction).bindTo(valueHashCodeFunction).bindTo(keyType).bindTo(valueType);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), method, isDeterministic());
    }

    public static long hash(MethodHandle keyHashCodeFunction, MethodHandle valueHashCodeFunction, Type keyType, Type valueType, Block block)
    {
        long result = 0;
        for (int position = 0; position < block.getPositionCount(); position += 2) {
            result += hashPosition(keyHashCodeFunction, keyType, block, position);
            result += hashPosition(valueHashCodeFunction, valueType, block, position + 1);
        }
        return result;
    }
}
