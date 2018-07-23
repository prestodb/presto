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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SingleMapBlock;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MapElementAtFunction
        extends SqlScalarFunction
{
    public static final MapElementAtFunction MAP_ELEMENT_AT = new MapElementAtFunction();

    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(MapElementAtFunction.class, "elementAt", MethodHandle.class, Type.class, Type.class, Block.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(MapElementAtFunction.class, "elementAt", MethodHandle.class, Type.class, Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(MapElementAtFunction.class, "elementAt", MethodHandle.class, Type.class, Type.class, Block.class, double.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(MapElementAtFunction.class, "elementAt", MethodHandle.class, Type.class, Type.class, Block.class, Slice.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(MapElementAtFunction.class, "elementAt", MethodHandle.class, Type.class, Type.class, Block.class, Object.class);

    protected MapElementAtFunction()
    {
        super(new Signature(
                "element_at",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("V"),
                ImmutableList.of(parseTypeSignature("map(K,V)"), parseTypeSignature("K")),
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
        return "Get value for the given key, or null if it does not exist";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");

        MethodHandle keyEqualsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(keyType, keyType))).getMethodHandle();

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
        methodHandle = methodHandle.bindTo(keyEqualsMethod).bindTo(keyType).bindTo(valueType);
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
    public static Object elementAt(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Block map, boolean key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object elementAt(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Block map, long key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object elementAt(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Block map, double key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object elementAt(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Block map, Slice key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object elementAt(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Block map, Object key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact((Block) key);
        if (valuePosition == -1) {
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }
}
