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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeJsonUtils.getObjectMap;
import static com.facebook.presto.type.TypeJsonUtils.castKey;
import static com.facebook.presto.type.TypeJsonUtils.castValue;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MapEqualOperator
        extends ParametricOperator
{
    public static final MapEqualOperator MAP_EQUAL = new MapEqualOperator();
    private static final TypeSignature RETURN_TYPE = parseTypeSignature(StandardTypes.BOOLEAN);

    private MapEqualOperator()
    {
        super(EQUAL, ImmutableList.of(comparableTypeParameter("K"), comparableTypeParameter("V")), StandardTypes.BOOLEAN, ImmutableList.of("map<K,V>", "map<K,V>"));
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

        MethodHandle methodHandle = methodHandle(MapEqualOperator.class, "equals", MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, Type.class, Slice.class, Slice.class);
        MethodHandle method = methodHandle.bindTo(keyEqualsFunction).bindTo(keyHashcodeFunction).bindTo(valueEqualsFunction).bindTo(keyType).bindTo(valueType);
        return operatorInfo(EQUAL, RETURN_TYPE, ImmutableList.of(typeSignature, typeSignature), method, true, ImmutableList.of(false, false));
    }

    public static Boolean equals(MethodHandle keyEqualsFunction, MethodHandle keyHashcodeFunction, MethodHandle valueEqualsFunction, Type keyType, Type valueType, Slice left, Slice right)
    {
        Map<Object, Object> leftMap = getObjectMap(left);
        Map<Object, Object> rightMap = getObjectMap(right);

        Map<KeyWrapper, Object> wrappedLeftMap = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> entry : leftMap.entrySet()) {
            wrappedLeftMap.put(new KeyWrapper(entry.getKey(), keyEqualsFunction, keyHashcodeFunction, keyType), entry.getValue());
        }

        Map<KeyWrapper, Object> wrappedRightMap = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> entry : rightMap.entrySet()) {
            wrappedRightMap.put(new KeyWrapper(entry.getKey(), keyEqualsFunction, keyHashcodeFunction, keyType), entry.getValue());
        }

        if (wrappedLeftMap.size() != wrappedRightMap.size()) {
            return false;
        }

        boolean nullValueExists = false;
        for (Map.Entry<KeyWrapper, Object> entry : wrappedRightMap.entrySet()) {
            KeyWrapper key = entry.getKey();
            if (!wrappedLeftMap.containsKey(key)) {
                return false;
            }

            boolean equalValues;
            try {
                Object leftValue = wrappedLeftMap.get(key);
                Object rightValue = entry.getValue();
                if (leftValue == null || rightValue == null) {
                    equalValues = true;
                    nullValueExists = true;
                }
                else {
                    equalValues = (Boolean) valueEqualsFunction.invoke(castValue(valueType, leftValue), castValue(valueType, rightValue));
                }
            }
            catch (Throwable t) {
                throw new PrestoException(INTERNAL_ERROR, t);
            }

            if (!equalValues) {
                return false;
            }
        }

        if (nullValueExists) {
            return null;
        }
        return true;
    }

    private static final class KeyWrapper
    {
        private final Object key;
        private final MethodHandle hashCode;
        private final MethodHandle equals;
        private final Type keyType;

        public KeyWrapper(Object key, MethodHandle equals, MethodHandle hashCode, Type keyType)
        {
            this.key = key;
            this.equals = equals;
            this.hashCode = hashCode;
            this.keyType = keyType;
        }

        @Override
        public int hashCode()
        {
            try {
                return ((Long) hashCode.invoke(castKey(keyType, (String) key))).hashCode();
            }
            catch (Throwable t) {
                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null || !getClass().equals(obj.getClass())) {
                return false;
            }
            KeyWrapper other = (KeyWrapper) obj;
            try {
                return (Boolean) equals.invoke(castKey(keyType, (String) key), castKey(keyType, (String) other.key));
            }
            catch (Throwable t) {
                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
    }
}
