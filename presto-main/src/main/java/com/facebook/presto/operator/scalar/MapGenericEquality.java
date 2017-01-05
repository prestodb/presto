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
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;

import java.lang.invoke.MethodHandle;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;

public final class MapGenericEquality
{
    private MapGenericEquality() {}

    public interface EqualityPredicate
    {
        Boolean equals(int leftMapIndex, int rightMapIndex) throws Throwable;
    }

    public static Boolean genericEqual(
            MethodHandle keyEqualsFunction,
            MethodHandle keyHashcodeFunction,
            Type keyType,
            Block leftMapBlock,
            Block rightMapBlock,
            EqualityPredicate predicate)
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

            try {
                Boolean result = predicate.equals(leftValuePosition, entry.getValue());
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
                return (boolean) equals.invoke(key, other.key);
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }
}
