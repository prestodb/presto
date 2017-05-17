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
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.constructorMethodHandle;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class MapConstructor
        extends SqlScalarFunction
{
    public static final MapConstructor MAP_CONSTRUCTOR = new MapConstructor();

    private static final MethodHandle METHOD_HANDLE = methodHandle(MapConstructor.class, "createMap", MapType.class, MethodHandle.class, MethodHandle.class, State.class, Block.class, Block.class);
    private static final String DESCRIPTION = "Constructs a map from the given key/value arrays";

    public MapConstructor()
    {
        super(new Signature(
                "map",
                FunctionKind.SCALAR,
                ImmutableList.of(comparableTypeParameter("K"), typeVariable("V")),
                ImmutableList.of(),
                TypeSignature.parseTypeSignature("map(K,V)"),
                ImmutableList.of(TypeSignature.parseTypeSignature("array(K)"), TypeSignature.parseTypeSignature("array(V)")),
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
        return true;
    }

    @Override
    public String getDescription()
    {
        return DESCRIPTION;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");

        Type mapType = typeManager.getParameterizedType(MAP, ImmutableList.of(TypeSignatureParameter.of(keyType.getTypeSignature()), TypeSignatureParameter.of(valueType.getTypeSignature())));
        MethodHandle keyHashCode = functionRegistry.getScalarFunctionImplementation(functionRegistry.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType))).getMethodHandle();
        MethodHandle keyEqual = functionRegistry.getScalarFunctionImplementation(functionRegistry.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType))).getMethodHandle();
        MethodHandle instanceFactory = constructorMethodHandle(State.class, MapType.class).bindTo(mapType);

        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(false, false),
                ImmutableList.of(false, false),
                ImmutableList.of(Optional.empty(), Optional.empty()),
                METHOD_HANDLE.bindTo(mapType).bindTo(keyEqual).bindTo(keyHashCode),
                Optional.of(instanceFactory),
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Block createMap(MapType mapType, MethodHandle keyEqual, MethodHandle keyHashCode, State state, Block keyBlock, Block valueBlock)
    {
        PageBuilder pageBuilder = state.getPageBuilder();
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        BlockBuilder mapBlockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();
        checkCondition(keyBlock.getPositionCount() == valueBlock.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "Key and value arrays must be the same length");
        for (int i = 0; i < keyBlock.getPositionCount(); i++) {
            if (keyBlock.isNull(i)) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be null");
            }
            mapType.getKeyType().appendTo(keyBlock, i, blockBuilder);
            mapType.getValueType().appendTo(valueBlock, i, blockBuilder);
        }
        mapBlockBuilder.closeEntry();
        pageBuilder.declarePosition();

        return mapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }

    public static final class State
    {
        private final PageBuilder pageBuilder;

        public State(MapType mapType)
        {
            pageBuilder = new PageBuilder(ImmutableList.of(mapType));
        }

        public PageBuilder getPageBuilder()
        {
            return pageBuilder;
        }
    }
}
