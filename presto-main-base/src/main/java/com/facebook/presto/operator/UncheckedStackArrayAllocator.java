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

import com.facebook.presto.common.block.ArrayAllocator;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Arrays;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static java.util.Objects.requireNonNull;

/**
 * A simple stack based {@link ArrayAllocator} that caches returned arrays.  When retrieving an array which is
 * cached, this implementation will first discard all smaller arrays encountered to ensure amortized
 * constant time access to the appropriately sized array. The implementation does NOT keep track of borrowed
 * arrays therefore user need to make sure do not return the same array more than once.
 */
@NotThreadSafe
public class UncheckedStackArrayAllocator
        implements ArrayAllocator
{
    private static final int DEFAULT_CAPACITY = 1000;

    private int[][] intArrays;
    private int intArraysTop = -1;
    private int borrowedIntArrays;

    private byte[][] byteArrays;
    private int byteArraysTop = -1;
    private int borrowedByteArrays;

    private long estimatedSizeInBytes;

    public UncheckedStackArrayAllocator()
    {
        this(DEFAULT_CAPACITY);
    }

    public UncheckedStackArrayAllocator(int initialCapacity)
    {
        checkArgument(initialCapacity > 0, "initialCapacity must be positive");
        intArrays = new int[initialCapacity][];
        byteArrays = new byte[initialCapacity][];
    }

    @Override
    public int[] borrowIntArray(int positionCount)
    {
        int[] array;
        while (intArraysTop >= 0 && intArrays[intArraysTop].length < positionCount) {
            estimatedSizeInBytes -= sizeOfIntArray(intArrays[intArraysTop].length);
            intArrays[intArraysTop] = null;
            --intArraysTop;
        }

        if (intArraysTop < 0) {
            array = new int[positionCount];
            estimatedSizeInBytes += sizeOfIntArray(positionCount);
        }
        else {
            array = intArrays[intArraysTop];
            --intArraysTop;
        }

        ++borrowedIntArrays;

        return array;
    }

    @Override
    public void returnArray(int[] array)
    {
        requireNonNull(array, "array is null");
        if (intArraysTop == intArrays.length - 1) {
            intArrays = Arrays.copyOf(intArrays, intArrays.length * 2);
        }
        intArrays[++intArraysTop] = array;
        --borrowedIntArrays;
    }

    @Override
    public byte[] borrowByteArray(int positionCount)
    {
        byte[] array;
        while (byteArraysTop >= 0 && byteArrays[byteArraysTop].length < positionCount) {
            estimatedSizeInBytes -= sizeOfByteArray(byteArrays[byteArraysTop].length);
            byteArrays[byteArraysTop] = null;
            --byteArraysTop;
        }

        if (byteArraysTop < 0) {
            array = new byte[positionCount];
            estimatedSizeInBytes += sizeOfByteArray(array.length);
        }
        else {
            array = byteArrays[byteArraysTop];
            --byteArraysTop;
        }

        ++borrowedByteArrays;

        return array;
    }

    @Override
    public void returnArray(byte[] array)
    {
        requireNonNull(array, "array is null");
        if (byteArraysTop == byteArrays.length - 1) {
            byteArrays = Arrays.copyOf(byteArrays, byteArrays.length * 2);
        }
        byteArrays[++byteArraysTop] = array;
        --borrowedByteArrays;
    }

    @Override
    public int getBorrowedArrayCount()
    {
        return borrowedIntArrays + borrowedByteArrays;
    }

    @Override
    public long getEstimatedSizeInBytes()
    {
        return estimatedSizeInBytes;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("intArraysCapacity", intArrays.length)
                .add("intArraysSize", intArraysTop)
                .add("borrowedIntArraysSize", borrowedIntArrays)
                .add("byteArraysCapacity", byteArrays.length)
                .add("byteArraysSize", byteArraysTop)
                .add("borrowedByteArraysSize", borrowedByteArrays)
                .add("estimatedSizeInBytes", estimatedSizeInBytes)
                .toString();
    }
}
