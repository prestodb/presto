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

import static java.util.Objects.requireNonNull;

final class BlockUtil
{
    private static final double BLOCK_RESET_SKEW = 1.25;

    private static final int DEFAULT_CAPACITY = 64;
    // See java.util.ArrayList for an explanation
    static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private BlockUtil()
    {
    }

    static void checkValidPositions(int[] positions, int offset, int length, int positionCount)
    {
        checkValidPositionsArray(positions, offset, length);

        for (int i = 0; i < length; ++i) {
            int position = positions[offset + i];
            if (position > positionCount) {
                throw new IllegalArgumentException("Invalid position " + position + " in block with " + positionCount + " positions");
            }
        }
    }

    static void checkValidPositionsArray(int[] positions, int offset, int length)
    {
        requireNonNull(positions, "positions array is null");
        if (offset < 0 || offset > positions.length) {
            throw new IndexOutOfBoundsException("Invalid offset " + offset + " for positions array with " + positions.length + " elements");
        }

        if (length < 0 || offset + length > positions.length) {
            throw new IndexOutOfBoundsException("Invalid length " + length + " for positions array with " + positions.length + " elements and offset " + offset);
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
        long newSize = (long) Math.ceil(currentSize * BLOCK_RESET_SKEW);

        // verify new size is within reasonable bounds
        if (newSize < DEFAULT_CAPACITY) {
            newSize = DEFAULT_CAPACITY;
        }
        else if (newSize > MAX_ARRAY_SIZE) {
            newSize = MAX_ARRAY_SIZE;
        }
        return (int) newSize;
    }
}
