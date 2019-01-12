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
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.SingleMapBlock;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.InterpretedFunctionInvoker;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.SUBSCRIPT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.sql.relational.Signatures.castSignature;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.String.format;

public class MapSubscriptOperator
        extends SqlOperator
{
    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, InterpretedFunctionInvoker.class, Type.class, Type.class, ConnectorSession.class, Block.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, InterpretedFunctionInvoker.class, Type.class, Type.class, ConnectorSession.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, InterpretedFunctionInvoker.class, Type.class, Type.class, ConnectorSession.class, Block.class, double.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, InterpretedFunctionInvoker.class, Type.class, Type.class, ConnectorSession.class, Block.class, Slice.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, InterpretedFunctionInvoker.class, Type.class, Type.class, ConnectorSession.class, Block.class, Object.class);

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
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");

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
        InterpretedFunctionInvoker functionInvoker = new InterpretedFunctionInvoker(functionRegistry);
        methodHandle = methodHandle.bindTo(functionInvoker).bindTo(keyType).bindTo(valueType);
        methodHandle = methodHandle.asType(methodHandle.type().changeReturnType(Primitives.wrap(valueType.getJavaType())));

        return new ScalarFunctionImplementation(
                true,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, InterpretedFunctionInvoker functionInvoker, Type keyType, Type valueType, ConnectorSession session, Block map, boolean key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw throwMissingKeyException(keyType, functionInvoker, key, session);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, InterpretedFunctionInvoker functionInvoker, Type keyType, Type valueType, ConnectorSession session, Block map, long key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw throwMissingKeyException(keyType, functionInvoker, key, session);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, InterpretedFunctionInvoker functionInvoker, Type keyType, Type valueType, ConnectorSession session, Block map, double key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw throwMissingKeyException(keyType, functionInvoker, key, session);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, InterpretedFunctionInvoker functionInvoker, Type keyType, Type valueType, ConnectorSession session, Block map, Slice key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw throwMissingKeyException(keyType, functionInvoker, key, session);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, InterpretedFunctionInvoker functionInvoker, Type keyType, Type valueType, ConnectorSession session, Block map, Object key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact((Block) key);
        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw throwMissingKeyException(keyType, functionInvoker, key, session);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    private static RuntimeException throwMissingKeyException(Type type, InterpretedFunctionInvoker functionInvoker, Object value, ConnectorSession session)
    {
        String stringValue;
        try {
            stringValue = ((Slice) functionInvoker.invoke(castSignature(VarcharType.VARCHAR, type), session, value)).toStringUtf8();
        }
        catch (RuntimeException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Key not present in map");
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Key not present in map: %s", stringValue));
    }
}
