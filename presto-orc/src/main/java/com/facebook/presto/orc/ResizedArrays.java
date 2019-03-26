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

package com.facebook.presto.orc;

import java.util.Arrays;

public class ResizedArrays
{
    private static final int MIN_INCREMENT = 100;

    private ResizedArrays() {}

    /**
     * Creates an int array with size rounded up. Intended for use
     * when the maximum size is known for the current use. Some extra
     * is reserved for the case where the next use is larger. Do not
     * use this when expecting the size to grow dynamically.
     */
    public static int[] newIntArrayForReuse(int size)
    {
        return new int[roundupSize(size)];
    }

    public static boolean[] newBooleanArrayForReuse(int size)
    {
        return new boolean[roundupSize(size)];
    }

    public static byte[] newByteArrayForReuse(int size)
    {
        return new byte[roundupSize(size)];
    }

    /**
     * Allocates or makes a resized copy of @param array. The size is
     * at least MIN_INCREMENT greater than requested or double the
     * original size, whichever is greater.  This is for use in
     * resizing arrays in applications where the final size is not
     * known.
     */
    public static int[] resize(int[] array, int size)
    {
        if (array == null) {
            return new int[size + MIN_INCREMENT];
        }
        return Arrays.copyOf(array, Math.max(size + MIN_INCREMENT, array.length * 2));
    }

    public static boolean[] resize(boolean[] array, int size)
    {
        if (array == null) {
            return new boolean[size + MIN_INCREMENT];
        }
        return Arrays.copyOf(array, Math.max(size + MIN_INCREMENT, array.length * 2));
    }

    public static long[] resize(long[] array, int size)
    {
        if (array == null) {
            return new long[size + MIN_INCREMENT];
        }
        return Arrays.copyOf(array, Math.max(size + MIN_INCREMENT, array.length * 2));
    }

    public static byte[] resize(byte[] array, int size)
    {
        if (array == null) {
            return new byte[size + MIN_INCREMENT];
        }
        return Arrays.copyOf(array, Math.max(size + MIN_INCREMENT, array.length * 2));
    }

    private static int roundupSize(int size)
    {
        return size + size / 4;
    }
}
