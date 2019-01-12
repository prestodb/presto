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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DuplicateMapKeyException;
import io.prestosql.spi.block.MapBlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.prestosql.metadata.Signature.comparableTypeParameter;
import static io.prestosql.metadata.Signature.internalOperator;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.MAP;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.util.Failures.checkCondition;
import static io.prestosql.util.Failures.internalError;
import static io.prestosql.util.Reflection.constructorMethodHandle;
import static io.prestosql.util.Reflection.methodHandle;

public final class MapConstructor
        extends SqlScalarFunction
{
    public static final MapConstructor MAP_CONSTRUCTOR = new MapConstructor();

    private static final MethodHandle METHOD_HANDLE = methodHandle(
            MapConstructor.class,
            "createMap",
            MapType.class,
            MethodHandle.class,
            MethodHandle.class,
            MethodHandle.class,
            State.class,
            ConnectorSession.class,
            Block.class,
            Block.class);

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
        MethodHandle keyIndeterminate = functionRegistry.getScalarFunctionImplementation(internalOperator(INDETERMINATE.name(), BOOLEAN.getTypeSignature(), ImmutableList.of(keyType.getTypeSignature()))).getMethodHandle();
        MethodHandle instanceFactory = constructorMethodHandle(State.class, MapType.class).bindTo(mapType);

        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                METHOD_HANDLE.bindTo(mapType).bindTo(keyEqual).bindTo(keyHashCode).bindTo(keyIndeterminate),
                Optional.of(instanceFactory),
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Block createMap(
            MapType mapType,
            MethodHandle keyEqual,
            MethodHandle keyHashCode,
            MethodHandle keyIndeterminate,
            State state,
            ConnectorSession session,
            Block keyBlock,
            Block valueBlock)
    {
        checkCondition(keyBlock.getPositionCount() == valueBlock.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "Key and value arrays must be the same length");
        PageBuilder pageBuilder = state.getPageBuilder();
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) pageBuilder.getBlockBuilder(0);
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();
        for (int i = 0; i < keyBlock.getPositionCount(); i++) {
            if (keyBlock.isNull(i)) {
                // close block builder before throwing as we may be in a TRY() call
                // so that subsequent calls do not find it in an inconsistent state
                mapBlockBuilder.closeEntry();
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be null");
            }
            Object keyObject = readNativeValue(mapType.getKeyType(), keyBlock, i);
            try {
                if ((boolean) keyIndeterminate.invoke(keyObject, false)) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be indeterminate: " + mapType.getKeyType().getObjectValue(session, keyBlock, i));
                }
            }
            catch (Throwable t) {
                mapBlockBuilder.closeEntry();
                throw internalError(t);
            }
            mapType.getKeyType().appendTo(keyBlock, i, blockBuilder);
            mapType.getValueType().appendTo(valueBlock, i, blockBuilder);
        }
        try {
            mapBlockBuilder.closeEntryStrict();
        }
        catch (DuplicateMapKeyException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getDetailedMessage(mapType.getKeyType(), session), e);
        }
        finally {
            pageBuilder.declarePosition();
        }

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
