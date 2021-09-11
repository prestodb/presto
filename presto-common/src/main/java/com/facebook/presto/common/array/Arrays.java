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
package com.facebook.presto.common.array;

import com.facebook.presto.common.block.ArrayAllocator;

import static com.facebook.presto.common.array.Arrays.ExpansionFactor.SMALL;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.INITIALIZE;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.NONE;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.PRESERVE;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class Arrays
{
    private Arrays() {}

    public static boolean[] ensureCapacity(boolean[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new boolean[(int) (capacity * SMALL.expansionFactor)];
        }

        return buffer;
    }

    public static byte[] ensureCapacity(byte[] buffer, int capacity)
    {
        return ensureCapacity(buffer, capacity, SMALL, NONE);
    }

    public static byte[] ensureCapacity(byte[] buffer, int capacity, ExpansionFactor expansionFactor, ExpansionOption expansionOption)
    {
        int newCapacity = (int) (capacity * expansionFactor.expansionFactor);

        if (buffer == null) {
            buffer = new byte[newCapacity];
        }
        else if (buffer.length < capacity) {
            if (expansionOption == PRESERVE) {
                buffer = java.util.Arrays.copyOf(buffer, newCapacity);
            }
            else {
                buffer = new byte[newCapacity];
            }
        }
        else if (expansionOption == INITIALIZE) {
            java.util.Arrays.fill(buffer, (byte) 0);
        }

        return buffer;
    }

    public static byte[] ensureCapacity(byte[] buffer, int capacity, int estimatedMaxCapacity, ExpansionFactor expansionFactor, ExpansionOption expansionOption, ArrayAllocator allocator)
    {
        int newCapacity = max(capacity, min((int) (capacity * expansionFactor.expansionFactor), estimatedMaxCapacity));

        byte[] newBuffer;
        if (buffer == null) {
            newBuffer = allocator.borrowByteArray(newCapacity);
        }
        else if (buffer.length < capacity) {
            newBuffer = allocator.borrowByteArray(newCapacity);
            if (expansionOption == PRESERVE) {
                System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
            }
            allocator.returnArray(buffer);
        }
        else {
            newBuffer = buffer;
        }

        if (expansionOption == INITIALIZE) {
            java.util.Arrays.fill(newBuffer, (byte) 0);
        }

        return newBuffer;
    }

    public static short[] ensureCapacity(short[] buffer, int capacity)
    {
        if (buffer == null) {
            buffer = new short[capacity];
        }
        else if (buffer.length < capacity) {
            buffer = java.util.Arrays.copyOf(buffer, capacity);
        }

        return buffer;
    }

    public static int[] ensureCapacity(int[] buffer, int capacity)
    {
        return ensureCapacity(buffer, capacity, SMALL, NONE);
    }

    public static int[] ensureCapacity(int[] buffer, int capacity, ExpansionFactor expansionFactor, ExpansionOption expansionOption)
    {
        int newCapacity = (int) (capacity * expansionFactor.expansionFactor);

        if (buffer == null) {
            buffer = new int[newCapacity];
        }
        else if (buffer.length < capacity) {
            if (expansionOption == PRESERVE) {
                buffer = java.util.Arrays.copyOf(buffer, newCapacity);
            }
            else {
                buffer = new int[newCapacity];
            }
        }
        else if (expansionOption == INITIALIZE) {
            java.util.Arrays.fill(buffer, 0);
        }

        return buffer;
    }

    public static int[] ensureCapacity(int[] buffer, int capacity, ExpansionFactor expansionFactor, ExpansionOption expansionOption, ArrayAllocator allocator)
    {
        int newCapacity = (int) (capacity * expansionFactor.expansionFactor);

        int[] newBuffer;
        if (buffer == null) {
            newBuffer = allocator.borrowIntArray(newCapacity);
        }
        else if (buffer.length < capacity) {
            newBuffer = allocator.borrowIntArray(newCapacity);
            if (expansionOption == PRESERVE) {
                System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
            }
            allocator.returnArray(buffer);
        }
        else {
            newBuffer = buffer;
        }

        if (expansionOption == INITIALIZE) {
            java.util.Arrays.fill(newBuffer, 0);
        }

        return newBuffer;
    }

    public static long[] ensureCapacity(long[] buffer, int capacity)
    {
        return ensureCapacity(buffer, capacity, SMALL, NONE);
    }

    public static long[] ensureCapacity(long[] buffer, int capacity, ExpansionFactor expansionFactor, ExpansionOption expansionOption)
    {
        int newCapacity = (int) (capacity * expansionFactor.expansionFactor);

        if (buffer == null) {
            buffer = new long[newCapacity];
        }
        else if (buffer.length < capacity) {
            if (expansionOption == PRESERVE) {
                buffer = java.util.Arrays.copyOf(buffer, newCapacity);
            }
            else {
                buffer = new long[newCapacity];
            }
        }
        else if (expansionOption == INITIALIZE) {
            java.util.Arrays.fill(buffer, 0L);
        }

        return buffer;
    }

    public static int[][] ensureCapacity(int[][] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new int[capacity][];
        }

        return buffer;
    }

    public static boolean[][] ensureCapacity(boolean[][] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new boolean[capacity][];
        }

        return buffer;
    }

    public enum ExpansionFactor
    {
        SMALL(1.0),
        MEDIUM(1.5),
        LARGE(2.0);

        private final double expansionFactor;

        ExpansionFactor(double expansionFactor)
        {
            this.expansionFactor = expansionFactor;
        }
    }

    public enum ExpansionOption
    {
        PRESERVE,
        INITIALIZE,
        NONE;
    }
}
