/**
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
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
package com.facebook.presto.slice;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public final class SizeOf
{
    public static final byte SIZE_OF_BYTE = 1;
    public static final byte SIZE_OF_SHORT = 2;
    public static final byte SIZE_OF_INT = 4;
    public static final byte SIZE_OF_LONG = 8;
    public static final byte SIZE_OF_FLOAT = 4;
    public static final byte SIZE_OF_DOUBLE = 8;

    private static final Unsafe UNSAFE;
    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
            if (UNSAFE == null) {
                throw new RuntimeException("Unsafe access not available");
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static long sizeOf(boolean[] array)
    {
        if (array == null) {
            return 0;
        }
        return Unsafe.ARRAY_BOOLEAN_BASE_OFFSET + (Unsafe.ARRAY_BOOLEAN_INDEX_SCALE * array.length);
    }

    public static long sizeOf(byte[] array)
    {
        if (array == null) {
            return 0;
        }
        return Unsafe.ARRAY_BYTE_BASE_OFFSET + (Unsafe.ARRAY_BYTE_INDEX_SCALE * array.length);
    }

    public static long sizeOf(short[] array)
    {
        if (array == null) {
            return 0;
        }
        return Unsafe.ARRAY_SHORT_BASE_OFFSET + (Unsafe.ARRAY_SHORT_INDEX_SCALE * array.length);
    }

    public static long sizeOf(int[] array)
    {
        if (array == null) {
            return 0;
        }
        return Unsafe.ARRAY_INT_BASE_OFFSET + (Unsafe.ARRAY_INT_INDEX_SCALE * array.length);
    }

    public static long sizeOf(long[] array)
    {
        if (array == null) {
            return 0;
        }
        return Unsafe.ARRAY_LONG_BASE_OFFSET + (Unsafe.ARRAY_LONG_INDEX_SCALE * array.length);
    }

    public static long sizeOf(float[] array)
    {
        if (array == null) {
            return 0;
        }
        return Unsafe.ARRAY_FLOAT_BASE_OFFSET + (Unsafe.ARRAY_FLOAT_INDEX_SCALE * array.length);
    }

    public static long sizeOf(double[] array)
    {
        if (array == null) {
            return 0;
        }
        return Unsafe.ARRAY_DOUBLE_BASE_OFFSET + (Unsafe.ARRAY_DOUBLE_INDEX_SCALE * array.length);
    }

    public static long sizeOf(Object[] array)
    {
        if (array == null) {
            return 0;
        }
        return Unsafe.ARRAY_OBJECT_BASE_OFFSET + (Unsafe.ARRAY_OBJECT_INDEX_SCALE * array.length);
    }

    private SizeOf()
    {
    }
}
