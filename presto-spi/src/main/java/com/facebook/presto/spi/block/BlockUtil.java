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
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.lang.Math.ceil;
import static java.util.stream.Collectors.toSet;

final class BlockUtil
{
    private static final double BLOCK_RESET_SKEW = 1.25;

    private static final int DEFAULT_CAPACITY = 64;
    // See java.util.ArrayList for an explanation
    static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private BlockUtil()
    {
    }

    static void checkValidPositions(List<Integer> positions, int positionCount)
    {
        Set<Integer> invalidPositions = positions.stream().filter(position -> position >= positionCount).collect(toSet());
        if (!invalidPositions.isEmpty()) {
            throw new IllegalArgumentException("Invalid positions " + invalidPositions + " in block with " + positionCount + " positions");
        }
    }

    static void checkValidRegion(int positionCount, int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }
    }

    static int calculateNewArraySize(int currentSize)
    {
        // grow array by 50%
        long newSize = currentSize + (currentSize >> 1);

        // verify new size is within reasonable bounds
        if (newSize < DEFAULT_CAPACITY) {
            newSize = DEFAULT_CAPACITY;
        }
        else if (newSize > MAX_ARRAY_SIZE) {
            newSize = MAX_ARRAY_SIZE;
            if (newSize == currentSize) {
                throw new IllegalArgumentException("Can not grow array beyond " + MAX_ARRAY_SIZE);
            }
        }
        return (int) newSize;
    }

    static int calculateBlockResetSize(int currentSize)
    {
        long newSize = (long) ceil(currentSize * BLOCK_RESET_SKEW);

        // verify new size is within reasonable bounds
        if (newSize < DEFAULT_CAPACITY) {
            newSize = DEFAULT_CAPACITY;
        }
        else if (newSize > MAX_ARRAY_SIZE) {
            newSize = MAX_ARRAY_SIZE;
        }
        return (int) newSize;
    }

    static int calculateBlockResetBytes(int currentBytes)
    {
        long newBytes = (long) ceil(currentBytes * BLOCK_RESET_SKEW);
        if (newBytes > MAX_ARRAY_SIZE) {
            return MAX_ARRAY_SIZE;
        }
        return (int) newBytes;
    }

    /**
     * Recalculate the <code>offsets</code> array for the specified range.
     * The returned <code>offsets</code> array contains <code>length + 1</code> integers
     * with the first value set to 0.
     * If the range matches the entire <code>offsets</code> array,  the input array will be returned.
     */
    static int[] compactOffsets(int[] offsets, int index, int length)
    {
        if (index == 0 && offsets.length == length + 1) {
            return offsets;
        }

        int[] newOffsets = new int[length + 1];
        for (int i = 1; i <= length; i++) {
            newOffsets[i] = offsets[index + i] - offsets[index];
        }
        return newOffsets;
    }

    /**
     * Returns a slice containing values in the specified range of the specified slice.
     * If the range matches the entire slice, the input slice will be returned.
     * Otherwise, a copy will be returned.
     */
    static Slice compactSlice(Slice slice, int index, int length)
    {
        if (slice.isCompact() && index == 0 && length == slice.length()) {
            return slice;
        }
        return Slices.copyOf(slice, index, length);
    }

    /**
     * Returns an array containing elements in the specified range of the specified array.
     * If the range matches the entire array, the input array will be returned.
     * Otherwise, a copy will be returned.
     */
    static boolean[] compactArray(boolean[] array, int index, int length)
    {
        if (index == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, index, index + length);
    }

    static byte[] compactArray(byte[] array, int index, int length)
    {
        if (index == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, index, index + length);
    }

    static short[] compactArray(short[] array, int index, int length)
    {
        if (index == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, index, index + length);
    }

    static int[] compactArray(int[] array, int index, int length)
    {
        if (index == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, index, index + length);
    }

    static long[] compactArray(long[] array, int index, int length)
    {
        if (index == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, index, index + length);
    }

    /**
     * Returns <tt>true</tt> if the two specified arrays contain the same object in every position.
     * Unlike the {@link Arrays#equals(Object[],Object[])} method, this method compares using reference equals.
     */
    static boolean arraySame(Object[] array1, Object[] array2)
    {
        if (array1 == null || array2 == null || array1.length != array2.length) {
            throw new IllegalArgumentException("array1 and array2 cannot be null and should have same length");
        }

        for (int i = 0; i < array1.length; i++) {
            if (array1[i] != array2[i]) {
                return false;
            }
        }
        return true;
    }
}
