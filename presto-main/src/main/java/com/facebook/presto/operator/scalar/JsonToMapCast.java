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

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeJsonUtils.appendToBlockBuilder;
import static com.facebook.presto.type.TypeJsonUtils.canCastFromJson;
import static com.facebook.presto.type.TypeJsonUtils.stackRepresentationToObject;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class JsonToMapCast
        extends SqlOperator
{
    public static final JsonToMapCast JSON_TO_MAP = new JsonToMapCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonToMapCast.class, "toMap", Type.class, ConnectorSession.class, Slice.class);

    private JsonToMapCast()
    {
        super(OperatorType.CAST,
                ImmutableList.of(comparableTypeParameter("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("map(K,V)"),
                ImmutableList.of(parseTypeSignature(StandardTypes.JSON)));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        Type mapType = typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.of(keyType.getTypeSignature()), TypeSignatureParameter.of(valueType.getTypeSignature())));
        checkCondition(canCastFromJson(mapType), INVALID_CAST_ARGUMENT, "Cannot cast JSON to %s", mapType);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(mapType);
        return new ScalarFunctionImplementation(true, ImmutableList.of(false), methodHandle, isDeterministic());
    }

    @UsedByGeneratedCode
    public static Block toMap(Type mapType, ConnectorSession connectorSession, Slice json)
    {
        try {
            Map<?, ?> map = (Map<?, ?>) stackRepresentationToObject(connectorSession, json, mapType);
            if (map == null) {
                return null;
            }
            Type keyType = ((MapType) mapType).getKeyType();
            Type valueType = ((MapType) mapType).getValueType();
            BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(keyType, valueType), new BlockBuilderStatus(), map.size() * 2);
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                appendToBlockBuilder(keyType, entry.getKey(), blockBuilder);
                appendToBlockBuilder(valueType, entry.getValue(), blockBuilder);
            }
            return blockBuilder.build();
        }
        catch (RuntimeException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to " + mapType, e);
        }
    }
}
