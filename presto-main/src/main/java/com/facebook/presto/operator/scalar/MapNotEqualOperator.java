package com.facebook.presto.operator.scalar;
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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MapNotEqualOperator
        extends ParametricOperator
{
    public static final MapNotEqualOperator MAP_NOT_EQUAL = new MapNotEqualOperator();
    private static final TypeSignature RETURN_TYPE = parseTypeSignature(StandardTypes.BOOLEAN);
    private static final MethodHandle METHOD_HANDLE = methodHandle(MapNotEqualOperator.class, "notEqual", MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, Type.class, Block.class, Block.class);

    private MapNotEqualOperator()
    {
        super(NOT_EQUAL, ImmutableList.of(comparableTypeParameter("K"), comparableTypeParameter("V")), StandardTypes.BOOLEAN, ImmutableList.of("map<K,V>", "map<K,V>"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = types.get("V");

        Type type = typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(keyType.getTypeSignature(), valueType.getTypeSignature()), ImmutableList.of());
        TypeSignature typeSignature = type.getTypeSignature();

        MethodHandle keyEqualsFunction = functionRegistry.resolveOperator(EQUAL, ImmutableList.of(keyType, keyType)).getMethodHandle();
        MethodHandle keyHashcodeFunction = functionRegistry.resolveOperator(HASH_CODE, ImmutableList.of(keyType)).getMethodHandle();
        MethodHandle valueEqualsFunction = functionRegistry.resolveOperator(EQUAL, ImmutableList.of(valueType, valueType)).getMethodHandle();

        MethodHandle method = METHOD_HANDLE.bindTo(keyEqualsFunction).bindTo(keyHashcodeFunction).bindTo(valueEqualsFunction).bindTo(keyType).bindTo(valueType);
        return operatorInfo(NOT_EQUAL, RETURN_TYPE, ImmutableList.of(typeSignature, typeSignature), method, true, ImmutableList.of(false, false));
    }

    public static Boolean notEqual(MethodHandle keyEqualsFunction, MethodHandle keyHashcodeFunction, MethodHandle valueEqualsFunction, Type keyType, Type valueType, Block left, Block right)
    {
        Boolean equals = MapEqualOperator.equals(keyEqualsFunction, keyHashcodeFunction, valueEqualsFunction, keyType, valueType, left, right);
        if (equals == null) {
            return null;
        }

        return !equals;
    }
}
