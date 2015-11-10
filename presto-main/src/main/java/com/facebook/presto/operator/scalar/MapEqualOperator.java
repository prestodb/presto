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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.type.TypeUtils.castValue;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MapEqualOperator
        extends SqlOperator
{
    public static final MapEqualOperator MAP_EQUAL = new MapEqualOperator();
    private static final MethodHandle METHOD_HANDLE = methodHandle(MapEqualOperator.class, "equals", MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, Type.class, Block.class, Block.class);

    private MapEqualOperator()
    {
        super(EQUAL, ImmutableList.of(comparableTypeParameter("K"), comparableTypeParameter("V")), StandardTypes.BOOLEAN, ImmutableList.of("map<K,V>", "map<K,V>"));
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = types.get("V");

        MethodHandle keyEqualsFunction = functionRegistry.getScalarFunctionImplementation(internalOperator(EQUAL, BOOLEAN, ImmutableList.of(keyType, keyType))).getMethodHandle();
        MethodHandle keyHashcodeFunction = functionRegistry.getScalarFunctionImplementation(internalOperator(HASH_CODE, BIGINT, ImmutableList.of(keyType))).getMethodHandle();
        MethodHandle valueEqualsFunction = functionRegistry.getScalarFunctionImplementation(internalOperator(EQUAL, BOOLEAN, ImmutableList.of(valueType, valueType))).getMethodHandle();

        MethodHandle methodHandle = METHOD_HANDLE.bindTo(keyEqualsFunction).bindTo(keyHashcodeFunction).bindTo(valueEqualsFunction).bindTo(keyType).bindTo(valueType);
        return new ScalarFunctionImplementation(true, ImmutableList.of(false, false), methodHandle, isDeterministic());
    }

    public static Boolean equals(MethodHandle keyEqualsFunction, MethodHandle keyHashcodeFunction, MethodHandle valueEqualsFunction, Type keyType, Type valueType, Block leftMapBlock, Block rightMapBlock)
    {
        Map<KeyWrapper, Integer> wrappedLeftMap = new LinkedHashMap<>();
        for (int position = 0; position < leftMapBlock.getPositionCount(); position += 2) {
            wrappedLeftMap.put(new KeyWrapper(castValue(keyType, leftMapBlock, position), keyEqualsFunction, keyHashcodeFunction), position + 1);
        }

        Map<KeyWrapper, Integer> wrappedRightMap = new LinkedHashMap<>();
        for (int position = 0; position < rightMapBlock.getPositionCount(); position += 2) {
            wrappedRightMap.put(new KeyWrapper(castValue(keyType, rightMapBlock, position), keyEqualsFunction, keyHashcodeFunction), position + 1);
        }

        if (wrappedLeftMap.size() != wrappedRightMap.size()) {
            return false;
        }

        for (Map.Entry<KeyWrapper, Integer> entry : wrappedRightMap.entrySet()) {
            KeyWrapper key = entry.getKey();
            Integer leftValuePosition = wrappedLeftMap.get(key);
            if (leftValuePosition == null) {
                return false;
            }

            Object leftValue = castValue(valueType, leftMapBlock, leftValuePosition);
            if (leftValue == null) {
                return null;
            }

            Object rightValue = castValue(valueType, rightMapBlock, entry.getValue());
            if (rightValue == null) {
                return null;
            }

            try {
                Boolean result = (Boolean) valueEqualsFunction.invoke(leftValue, rightValue);
                if (result == null) {
                    return null;
                }
                else if (!result) {
                    return false;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        return true;
    }

    private static final class KeyWrapper
    {
        private final Object key;
        private final MethodHandle hashCode;
        private final MethodHandle equals;

        public KeyWrapper(Object key, MethodHandle equals, MethodHandle hashCode)
        {
            this.key = key;
            this.equals = equals;
            this.hashCode = hashCode;
        }

        @Override
        public int hashCode()
        {
            try {
                return Long.hashCode((long) hashCode.invoke(key));
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

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
                return (Boolean) equals.invoke(key, other.key);
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
    }
}
