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
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

public final class MapTransformKeyFunction
        extends SqlScalarFunction
{
    public static final MapTransformKeyFunction MAP_TRANSFORM_KEY_FUNCTION = new MapTransformKeyFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(
            MapTransformKeyFunction.class,
            "transform",
            Type.class,
            Type.class,
            Type.class,
            ConnectorSession.class,
            Block.class,
            MethodHandle.class);

    private MapTransformKeyFunction()
    {
        super(new Signature(
                "transform_keys",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("K1"), typeVariable("K2"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("map(K2,V)"),
                ImmutableList.of(parseTypeSignature("map(K1,V)"), parseTypeSignature("function(K1,V,K2)")),
                false));
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    public String getDescription()
    {
        return "apply lambda to each entry of the map and transform the key";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K1");
        Type transformedKeyType = boundVariables.getTypeVariable("K2");
        Type valueType = boundVariables.getTypeVariable("V");
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(false, false),
                METHOD_HANDLE.bindTo(keyType).bindTo(transformedKeyType).bindTo(valueType),
                isDeterministic());
    }

    public static Block transform(Type keyType, Type transformedKeyType, Type valueType, ConnectorSession session, Block block, MethodHandle function)
    {
        int positionCount = block.getPositionCount();
        BlockBuilder resultBuilder = new InterleavedBlockBuilder(ImmutableList.of(transformedKeyType, valueType), new BlockBuilderStatus(), positionCount);
        TypedSet typedSet = new TypedSet(transformedKeyType, positionCount / 2);

        for (int position = 0; position < positionCount; position += 2) {
            Object key = readNativeValue(keyType, block, position);
            Object value = readNativeValue(valueType, block, position + 1);
            Object transformedKey;
            try {
                transformedKey = function.invoke(key, value);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }

            if (transformedKey == null) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be null");
            }

            writeNativeValue(transformedKeyType, resultBuilder, transformedKey);
            valueType.appendTo(block, position + 1, resultBuilder);

            if (typedSet.contains(resultBuilder, position)) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Duplicate keys (%s) are not allowed", transformedKeyType.getObjectValue(session, resultBuilder, position)));
            }
            typedSet.add(resultBuilder, position);
        }
        return resultBuilder.build();
    }
}
