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
import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DuplicateMapKeyException;
import com.facebook.presto.common.block.MapBlockBuilder;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Failures.internalError;
import static com.facebook.presto.util.Reflection.constructorMethodHandle;
import static com.facebook.presto.util.Reflection.methodHandle;

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
            SqlFunctionProperties.class,
            Block.class,
            Block.class);

    private static final String DESCRIPTION = "Constructs a map from the given key/value arrays";

    public MapConstructor()
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "map"),
                FunctionKind.SCALAR,
                ImmutableList.of(comparableTypeParameter("K"), typeVariable("V")),
                ImmutableList.of(),
                TypeSignature.parseTypeSignature("map(K,V)"),
                ImmutableList.of(TypeSignature.parseTypeSignature("array(K)"), TypeSignature.parseTypeSignature("array(V)")),
                false));
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return PUBLIC;
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
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");

        Type mapType = functionAndTypeManager.getParameterizedType(MAP, ImmutableList.of(TypeSignatureParameter.of(keyType.getTypeSignature()), TypeSignatureParameter.of(valueType.getTypeSignature())));
        MethodHandle keyNativeHashCode = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperator(OperatorType.HASH_CODE, fromTypes(keyType))).getMethodHandle();
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));
        MethodHandle keyNativeEquals = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperator(OperatorType.EQUAL, fromTypes(keyType, keyType))).getMethodHandle();
        MethodHandle keyBlockEquals = compose(keyNativeEquals, nativeValueGetter(keyType), nativeValueGetter(keyType));
        MethodHandle keyIndeterminate = functionAndTypeManager.getBuiltInScalarFunctionImplementation(
                functionAndTypeManager.resolveOperator(INDETERMINATE, fromTypeSignatures((keyType.getTypeSignature())))).getMethodHandle();
        MethodHandle instanceFactory = constructorMethodHandle(State.class, MapType.class).bindTo(mapType);

        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                METHOD_HANDLE.bindTo(mapType).bindTo(keyBlockEquals).bindTo(keyBlockHashCode).bindTo(keyIndeterminate),
                Optional.of(instanceFactory));
    }

    @UsedByGeneratedCode
    public static Block createMap(
            MapType mapType,
            MethodHandle keyBlockEqual,
            MethodHandle keyBlockHashCode,
            MethodHandle keyIndeterminate,
            State state,
            SqlFunctionProperties properties,
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
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be indeterminate: " + mapType.getKeyType().getObjectValue(properties, keyBlock, i));
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
            mapBlockBuilder.closeEntryStrict(keyBlockEqual, keyBlockHashCode);
        }
        catch (DuplicateMapKeyException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getDetailedMessage(mapType.getKeyType(), properties), e);
        }
        catch (NotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
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
