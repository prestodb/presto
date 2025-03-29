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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Collections.newSetFromMap;
import static java.util.Objects.requireNonNull;

/**
 * A simple {@link ArrayAllocator} that caches returned arrays.  When retrieving an array which is
 * cached, this implementation will first discard all smaller arrays encountered to ensure amortized
 * constant time access to the appropriately sized array.
 */
@NotThreadSafe
public class SimpleArrayAllocator
        implements ArrayAllocator
{
    private static final int DEFAULT_MAX_OUTSTANDING = 1000;
    private final int maxOutstandingArrays;

    private final Deque<int[]> intArrays = new ArrayDeque<>();
    private final Set<int[]> borrowedIntArrays = newSetFromMap(new IdentityHashMap<>());

    private final Deque<byte[]> byteArrays = new ArrayDeque<>();
    private final Set<byte[]> borrowedByteArrays = newSetFromMap(new IdentityHashMap<>());

    private long estimatedSizeInBytes;

    public SimpleArrayAllocator()
    {
        this(DEFAULT_MAX_OUTSTANDING);
    }

    public SimpleArrayAllocator(int maxOutstandingArrays)
    {
        checkArgument(maxOutstandingArrays > 0, "maxOutstandingArrays must be positive");
        this.maxOutstandingArrays = maxOutstandingArrays;
    }

    @Override
    public int[] borrowIntArray(int positionCount)
    {
        checkState(getBorrowedArrayCount() < maxOutstandingArrays, "Requested too many arrays: %s", getBorrowedArrayCount());
        int[] array;
        while (!intArrays.isEmpty() && intArrays.peek().length < positionCount) {
            array = intArrays.pop();
            estimatedSizeInBytes -= sizeOf(array);
        }
        if (intArrays.isEmpty()) {
            array = new int[positionCount];
            estimatedSizeInBytes += sizeOf(array);
        }
        else {
            array = intArrays.pop();
        }
        verify(borrowedIntArrays.add(array), "Attempted to borrow array which was already borrowed");
        return array;
    }

    @Override
    public void returnArray(int[] array)
    {
        requireNonNull(array, "array is null");
        checkArgument(borrowedIntArrays.remove(array), "Returned int array which was not borrowed");
        intArrays.push(array);
    }

    @Override
    public byte[] borrowByteArray(int positionCount)
    {
        checkState(getBorrowedArrayCount() < maxOutstandingArrays, "Requested too many arrays: %s", getBorrowedArrayCount());
        byte[] array;
        while (!byteArrays.isEmpty() && byteArrays.peek().length < positionCount) {
            array = byteArrays.pop();
            estimatedSizeInBytes -= sizeOf(array);
        }
        if (byteArrays.isEmpty()) {
            array = new byte[positionCount];
            estimatedSizeInBytes += sizeOf(array);
        }
        else {
            array = byteArrays.pop();
        }
        verify(borrowedByteArrays.add(array), "Attempted to borrow array which was already borrowed");
        return array;
    }

    @Override
    public void returnArray(byte[] array)
    {
        requireNonNull(array, "array is null");
        checkArgument(borrowedByteArrays.remove(array), "Returned byte array which was not borrowed");
        byteArrays.push(array);
    }

    @Override
    public int getBorrowedArrayCount()
    {
        return borrowedIntArrays.size() + borrowedByteArrays.size();
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
                .add("intArraysSize", intArrays.size())
                .add("borrowedIntArraysSize", borrowedIntArrays.size())
                .add("byteArraysSize", byteArrays.size())
                .add("borrowedByteArraysSize", borrowedByteArrays.size())
                .add("estimatedSizeInBytes", estimatedSizeInBytes)
                .toString();
    }
}
