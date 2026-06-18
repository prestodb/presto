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

import com.facebook.presto.common.InvalidFunctionArgumentException;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
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

    public static boolean isEnumType(Type type)
    {
        return type instanceof EnumType || type instanceof TypeWithName && ((TypeWithName) type).getType() instanceof EnumType;
    }

    public static boolean isDistinctType(Type type)
    {
        return type instanceof DistinctType;
    }

    /**
     * Recursive version of isDistinctType.
     */
    public static boolean containsDistinctType(List<Type> types)
    {
        LinkedList<Type> allTypes = new LinkedList<>(types);
        while (!allTypes.isEmpty()) {
            Type type = allTypes.removeLast();
            if (isDistinctType(type)) {
                return true;
            }
            allTypes.addAll(type.getTypeParameters());
        }
        return false;
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
            if (value instanceof BigDecimal) {
                type.writeLong(blockBuilder, ((BigDecimal) value).unscaledValue().longValue());
            }
            else {
                type.writeLong(blockBuilder, ((Number) value).longValue());
            }
        }
        else if (type.getJavaType() == Slice.class) {
            Slice slice;
            if (value instanceof byte[]) {
                slice = Slices.wrappedBuffer((byte[]) value);
            }
            else if (value instanceof String) {
                slice = Slices.utf8Slice((String) value);
            }
            else if (value instanceof BigDecimal) {
                slice = encodeScaledValue((BigDecimal) value);
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

    public static boolean isFloatingPointNaN(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");

        if (type.equals(DOUBLE)) {
            return Double.isNaN((double) value);
        }
        if (type.equals(REAL)) {
            return Float.isNaN(intBitsToFloat(toIntExact((long) value)));
        }
        return false;
    }

    static void checkElementNotNull(boolean isNull, String errorMsg)
    {
        if (isNull) {
            throw new InvalidFunctionArgumentException(errorMsg);
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

    /**
     * For our definitions of double and real, Nan=NaN is true
     * NaN is greater than all other values, and +0=-0 is true.
     * the below functions enforce that definition
     */
    public static boolean doubleEquals(double a, double b)
    {
        // the first check ensures +0 == -0 is true. the second ensures that NaN == NaN is true
        // for all other cases a == b and doubleToLongBits(a) == doubleToLongBits(b) will return
        // the same result
        // doubleToLongBits converts all NaNs to the same representation
        return a == b || doubleToLongBits(a) == doubleToLongBits(b);
    }

    public static long doubleHashCode(double value)
    {
        // canonicalize +0 and -0 to a single value
        value = value == -0 ? 0 : value;
        // doubleToLongBits converts all NaNs to the same representation
        return AbstractLongType.hash(doubleToLongBits(value));
    }

    public static int doubleCompare(double a, double b)
    {
        // these three ifs can only be true if neither value is NaN
        if (a < b) {
            return -1;
        }
        if (a > b) {
            return 1;
        }
        // this check ensure doubleCompare(+0, -0) will return 0
        // if we just did doubleToLongBits comparison, then they
        // would not compare as equal
        if (a == b) {
            return 0;
        }

        // this ensures that doubleCompare(NaN, NaN) will return 0
        // doubleToLongBits converts all NaNs to the same representation
        long aBits = doubleToLongBits(a);
        long bBits = doubleToLongBits(b);
        return Long.compare(aBits, bBits);
    }

    public static boolean realEquals(float a, float b)
    {
        // the first check ensures +0 == -0 is true. the second ensures that NaN == NaN is true
        // for all other cases a == b and floatToIntBits(a) == floatToIntBits(b) will return
        // the same result
        // floatToIntBits converts all NaNs to the same representation
        return a == b || floatToIntBits(a) == floatToIntBits(b);
    }

    public static long realHashCode(float value)
    {
        // canonicalize +0 and -0 to a single value
        value = value == -0 ? 0 : value;
        // floatToIntBits converts all NaNs to the same representation
        return AbstractIntType.hash(floatToIntBits(value));
    }

    public static int realCompare(float a, float b)
    {
        // these three ifs can only be true if neither value is NaN
        if (a < b) {
            return -1;
        }
        if (a > b) {
            return 1;
        }
        // this check ensure floatCompare(+0, -0) will return 0
        // if we just did floatToIntBits comparison, then they
        // would not compare as equal
        if (a == b) {
            return 0;
        }

        // this ensures that realCompare(NaN, NaN) will return 0
        // floatToIntBits converts all NaNs to the same representation
        int aBits = floatToIntBits(a);
        int bBits = floatToIntBits(b);
        return Integer.compare(aBits, bBits);
    }
}
