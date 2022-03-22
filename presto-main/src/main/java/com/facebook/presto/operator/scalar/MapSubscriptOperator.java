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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.SingleMapBlock;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

public class MapSubscriptOperator
        extends SqlOperator
{
    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, MissingKeyExceptionFactory.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, SqlFunctionProperties.class, Block.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, MissingKeyExceptionFactory.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, SqlFunctionProperties.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, MissingKeyExceptionFactory.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, SqlFunctionProperties.class, Block.class, double.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, MissingKeyExceptionFactory.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, SqlFunctionProperties.class, Block.class, Slice.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, MissingKeyExceptionFactory.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, SqlFunctionProperties.class, Block.class, Object.class);

    private final boolean legacyMissingKey;

    public MapSubscriptOperator(boolean legacyMissingKey)
    {
        super(SUBSCRIPT,
                ImmutableList.of(typeVariable("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("V"),
                ImmutableList.of(parseTypeSignature("map(K,V)"), parseTypeSignature("K")));
        this.legacyMissingKey = legacyMissingKey;
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");

        MethodHandle keyNativeHashCode = functionAndTypeManager.getJavaScalarFunctionImplementation(functionAndTypeManager.resolveOperator(OperatorType.HASH_CODE, fromTypes(keyType))).getMethodHandle();
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));
        MethodHandle keyNativeEquals = functionAndTypeManager.getJavaScalarFunctionImplementation(functionAndTypeManager.resolveOperator(OperatorType.EQUAL, fromTypes(keyType, keyType))).getMethodHandle();
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));

        MethodHandle methodHandle;
        if (keyType.getJavaType() == boolean.class) {
            methodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else if (keyType.getJavaType() == long.class) {
            methodHandle = METHOD_HANDLE_LONG;
        }
        else if (keyType.getJavaType() == double.class) {
            methodHandle = METHOD_HANDLE_DOUBLE;
        }
        else if (keyType.getJavaType() == Slice.class) {
            methodHandle = METHOD_HANDLE_SLICE;
        }
        else {
            methodHandle = METHOD_HANDLE_OBJECT;
        }
        methodHandle = MethodHandles.insertArguments(methodHandle, 0, legacyMissingKey);
        MissingKeyExceptionFactory missingKeyExceptionFactory = new MissingKeyExceptionFactory(functionAndTypeManager, keyType);
        methodHandle = methodHandle.bindTo(missingKeyExceptionFactory).bindTo(keyNativeHashCode).bindTo(keyBlockNativeEquals).bindTo(keyBlockHashCode).bindTo(valueType);
        methodHandle = methodHandle.asType(methodHandle.type().changeReturnType(Primitives.wrap(valueType.getJavaType())));

        return new BuiltInScalarFunctionImplementation(
                true,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, MissingKeyExceptionFactory missingKeyExceptionFactory, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, SqlFunctionProperties properties, Block map, boolean key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition;
        try {
            valuePosition = mapBlock.seekKeyExact(key, keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode);
        }
        catch (NotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }

        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw missingKeyExceptionFactory.create(properties, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, MissingKeyExceptionFactory missingKeyExceptionFactory, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, SqlFunctionProperties properties, Block map, long key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition;
        try {
            valuePosition = mapBlock.seekKeyExact(key, keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode);
        }
        catch (NotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }

        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw missingKeyExceptionFactory.create(properties, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, MissingKeyExceptionFactory missingKeyExceptionFactory, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, SqlFunctionProperties properties, Block map, double key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition;
        try {
            valuePosition = mapBlock.seekKeyExact(key, keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode);
        }
        catch (NotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }

        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw missingKeyExceptionFactory.create(properties, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, MissingKeyExceptionFactory missingKeyExceptionFactory, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, SqlFunctionProperties properties, Block map, Slice key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition;
        try {
            valuePosition = mapBlock.seekKeyExact(key, keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode);
        }
        catch (NotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }

        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw missingKeyExceptionFactory.create(properties, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, MissingKeyExceptionFactory missingKeyExceptionFactory, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, SqlFunctionProperties properties, Block map, Object key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition;
        try {
            valuePosition = mapBlock.seekKeyExact((Block) key, keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode);
        }
        catch (NotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }

        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw missingKeyExceptionFactory.create(properties, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    private static class MissingKeyExceptionFactory
    {
        private final InterpretedFunctionInvoker functionInvoker;
        private final FunctionHandle castFunction;

        public MissingKeyExceptionFactory(FunctionAndTypeManager functionAndTypeManager, Type keyType)
        {
            functionInvoker = new InterpretedFunctionInvoker(functionAndTypeManager);

            FunctionHandle castFunction = null;
            try {
                castFunction = functionAndTypeManager.lookupCast(CAST, keyType.getTypeSignature(), VARCHAR.getTypeSignature());
            }
            catch (PrestoException ignored) {
            }
            this.castFunction = castFunction;
        }

        public PrestoException create(SqlFunctionProperties properties, Object value)
        {
            if (castFunction != null) {
                try {
                    Slice varcharValue = (Slice) functionInvoker.invoke(castFunction, properties, value);
                    return new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Key not present in map: %s", varcharValue.toStringUtf8()));
                }
                catch (RuntimeException ignored) {
                }
            }
            return new PrestoException(INVALID_FUNCTION_ARGUMENT, "Key not present in map");
        }
    }
}
