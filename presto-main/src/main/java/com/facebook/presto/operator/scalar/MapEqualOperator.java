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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;

import java.lang.invoke.MethodHandle;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;

@ScalarOperator(EQUAL)
public final class MapEqualOperator
{
    private MapEqualOperator() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean equals(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"K", "K"}) MethodHandle keyEqualsFunction,
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"K"}) MethodHandle keyHashcodeFunction,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"V", "V"}) MethodHandle valueEqualsFunction,
            @TypeParameter("K") Type keyType,
            @TypeParameter("V") Type valueType,
            @SqlType("map(K,V)") Block leftMapBlock,
            @SqlType("map(K,V)") Block rightMapBlock)
    {
        Map<KeyWrapper, Integer> wrappedLeftMap = new LinkedHashMap<>();
        for (int position = 0; position < leftMapBlock.getPositionCount(); position += 2) {
            wrappedLeftMap.put(new KeyWrapper(readNativeValue(keyType, leftMapBlock, position), keyEqualsFunction, keyHashcodeFunction), position + 1);
        }

        Map<KeyWrapper, Integer> wrappedRightMap = new LinkedHashMap<>();
        for (int position = 0; position < rightMapBlock.getPositionCount(); position += 2) {
            wrappedRightMap.put(new KeyWrapper(readNativeValue(keyType, rightMapBlock, position), keyEqualsFunction, keyHashcodeFunction), position + 1);
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

            Object leftValue = readNativeValue(valueType, leftMapBlock, leftValuePosition);
            if (leftValue == null) {
                return null;
            }

            Object rightValue = readNativeValue(valueType, rightMapBlock, entry.getValue());
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

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
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

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
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

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }
}
