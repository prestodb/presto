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
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MapConcatFunction
        extends SqlScalarFunction
{
    public static final MapConcatFunction MAP_CONCAT_FUNCTION = new MapConcatFunction();
    private static final MethodHandle METHOD_HANDLE = methodHandle(MapConcatFunction.class, "mapConcat", Type.class, Type.class, Block.class, Block.class);

    public MapConcatFunction()
    {
        super("map_concat", ImmutableList.of(typeParameter("K"), typeParameter("V")), "map<K,V>", ImmutableList.of("map<K,V>", "map<K,V>"));
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
        return "Concatenates given maps";
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type kType = types.get("K");
        Type vType = types.get("V");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(kType).bindTo(vType);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false, false), methodHandle, isDeterministic());
    }

    public static Block mapConcat(Type keyType, Type valueType, Block leftMap, Block rightMap)
    {
        TypedSet typedSet = new TypedSet(keyType, rightMap.getPositionCount());
        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(keyType, valueType), new BlockBuilderStatus(), leftMap.getPositionCount() + rightMap.getPositionCount());
        for (int i = 0; i < rightMap.getPositionCount(); i += 2) {
            typedSet.add(rightMap, i);
            keyType.appendTo(rightMap, i, blockBuilder);
            valueType.appendTo(rightMap, i + 1, blockBuilder);
        }
        for (int i = 0; i < leftMap.getPositionCount(); i += 2) {
            if (!typedSet.contains(leftMap, i)) {
                keyType.appendTo(leftMap, i, blockBuilder);
                valueType.appendTo(leftMap, i + 1, blockBuilder);
            }
        }
        return blockBuilder.build();
    }
}
