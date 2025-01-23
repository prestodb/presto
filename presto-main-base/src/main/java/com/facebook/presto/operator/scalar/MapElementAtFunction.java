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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.SingleMapBlock;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MapElementAtFunction
        extends SqlScalarFunction
{
    public static final MapElementAtFunction MAP_ELEMENT_AT = new MapElementAtFunction();

    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(MapElementAtFunction.class, "elementAt", MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, Block.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(MapElementAtFunction.class, "elementAt", MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(MapElementAtFunction.class, "elementAt", MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, Block.class, double.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(MapElementAtFunction.class, "elementAt", MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, Block.class, Slice.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(MapElementAtFunction.class, "elementAt", MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, Block.class, Object.class);

    protected MapElementAtFunction()
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "element_at"),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("V"),
                ImmutableList.of(parseTypeSignature("map(K,V)"), parseTypeSignature("K")),
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
        return "Get value for the given key, or null if it does not exist";
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
        methodHandle = methodHandle.bindTo(keyNativeHashCode).bindTo(keyBlockNativeEquals).bindTo(keyBlockHashCode).bindTo(valueType);
        methodHandle = methodHandle.asType(methodHandle.type().changeReturnType(Primitives.wrap(valueType.getJavaType())));

        return new BuiltInScalarFunctionImplementation(
                true,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Object elementAt(MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, Block map, boolean key)
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
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object elementAt(MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, Block map, long key)
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
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object elementAt(MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, Block map, double key)
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
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object elementAt(MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, Block map, Slice key)
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
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object elementAt(MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, Block map, Object key)
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
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }
}
