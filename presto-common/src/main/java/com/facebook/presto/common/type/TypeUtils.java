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
package com.facebook.presto.common.type;

import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class TypeUtils
{
    public static final int NULL_HASH_CODE = 0;

    private TypeUtils()
    {
    }

    public static boolean isNumericType(Type type)
    {
        return isNonDecimalNumericType(type) || type instanceof DecimalType;
    }

    public static boolean isNonDecimalNumericType(Type type)
    {
        return isExactNumericType(type) || isApproximateNumericType(type);
    }

    public static boolean isExactNumericType(Type type)
    {
        return type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT);
    }

    public static boolean isApproximateNumericType(Type type)
    {
        return type.equals(DOUBLE) || type.equals(REAL);
    }

    /**
     * Get the native value as an object in the value at {@code position} of {@code block}.
     */
    public static Object readNativeValue(Type type, Block block, int position)
    {
        Class<?> javaType = type.getJavaType();

        if (block.isNull(position)) {
            return null;
        }
        if (javaType == long.class) {
            return type.getLong(block, position);
        }
        if (javaType == double.class) {
            return type.getDouble(block, position);
        }
        if (javaType == boolean.class) {
            return type.getBoolean(block, position);
        }
        if (javaType == Slice.class) {
            return type.getSlice(block, position);
        }
        return type.getObject(block, position);
    }

    /**
     * Write a native value object to the current entry of {@code blockBuilder}.
     */
    public static void writeNativeValue(Type type, BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (type.getJavaType() == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(blockBuilder, ((Number) value).doubleValue());
        }
        else if (type.getJavaType() == long.class) {
            type.writeLong(blockBuilder, ((Number) value).longValue());
        }
        else if (type.getJavaType() == Slice.class) {
            Slice slice;
            if (value instanceof byte[]) {
                slice = Slices.wrappedBuffer((byte[]) value);
            }
            else if (value instanceof String) {
                slice = Slices.utf8Slice((String) value);
            }
            else {
                slice = (Slice) value;
            }
            type.writeSlice(blockBuilder, slice, 0, slice.length());
        }
        else {
            type.writeObject(blockBuilder, value);
        }
    }

    public static long hashPosition(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return NULL_HASH_CODE;
        }
        return type.hash(block, position);
    }

    static void checkElementNotNull(boolean isNull, String errorMsg)
    {
        if (isNull) {
            throw new NotSupportedException(errorMsg);
        }
    }

    static void validateEnumMap(Map<String, ?> enumMap)
    {
        if (enumMap.containsKey(null)) {
            throw new IllegalArgumentException("Enum cannot contain null key");
        }
        int nUniqueAndNotNullValues = enumMap.values().stream()
                .filter(Objects::nonNull).collect(toSet()).size();
        if (nUniqueAndNotNullValues != enumMap.size()) {
            throw new IllegalArgumentException("Enum cannot contain null or duplicate values");
        }
        int nCaseInsensitiveKeys = enumMap.keySet().stream().map(k -> k.toUpperCase(ENGLISH)).collect(Collectors.toSet()).size();
        if (nCaseInsensitiveKeys != enumMap.size()) {
            throw new IllegalArgumentException("Enum cannot contain case-insensitive duplicate keys");
        }
    }

    static <V> Map<String, V> normalizeEnumMap(Map<String, V> entries)
    {
        return entries.entrySet().stream()
                .collect(toMap(e -> e.getKey().toUpperCase(ENGLISH), Map.Entry::getValue));
    }
}
