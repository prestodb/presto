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
package com.facebook.presto.operator;

import com.facebook.presto.spi.api.Experimental;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_INDEX_SCALE;

// This class is a copy-paste of ByteArrays from airlift with range checks removed.
@Experimental
public class UncheckedByteArrays
{
    private static final Unsafe unsafe;

    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte getByteUnchecked(byte[] bytes, int index)
    {
        return unsafe.getByte(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET);
    }

    public static int setByteUnchecked(byte[] bytes, int index, byte value)
    {
        unsafe.putByte(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET, value);
        return index + ARRAY_BYTE_INDEX_SCALE;
    }

    public static short getShortUnchecked(byte[] bytes, int index)
    {
        return unsafe.getShort(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET);
    }

    public static int setShortUnchecked(byte[] bytes, int index, short value)
    {
        unsafe.putShort(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET, value);
        return index + ARRAY_SHORT_INDEX_SCALE;
    }

    public static int getIntUnchecked(byte[] bytes, int index)
    {
        return unsafe.getInt(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET);
    }

    public static int setIntUnchecked(byte[] bytes, int index, int value)
    {
        unsafe.putInt(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET, value);
        return index + ARRAY_INT_INDEX_SCALE;
    }

    public static long getLongUnchecked(byte[] bytes, int index)
    {
        return unsafe.getLong(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET);
    }

    public static int setLongUnchecked(byte[] bytes, int index, long value)
    {
        unsafe.putLong(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET, value);
        return index + ARRAY_LONG_INDEX_SCALE;
    }

    private UncheckedByteArrays()
    {}
}
