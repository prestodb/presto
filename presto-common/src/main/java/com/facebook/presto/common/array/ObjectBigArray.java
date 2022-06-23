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

import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.facebook.presto.common.array.BigArrays.INITIAL_SEGMENTS;
import static com.facebook.presto.common.array.BigArrays.SEGMENT_SIZE;
import static com.facebook.presto.common.array.BigArrays.offset;
import static com.facebook.presto.common.array.BigArrays.segment;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;

// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/)
// Copyright (C) 2010-2013 Sebastiano Vigna
public final class ObjectBigArray<T>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ObjectBigArray.class).instanceSize();
    private static final long SIZE_OF_SEGMENT = sizeOfObjectArray(SEGMENT_SIZE);

    private final Object initialValue;

    private Object[][] array;
    private long capacity;
    private int segments;

    /**
     * Creates a new big array containing one initial segment
     */
    public ObjectBigArray()
    {
        this(null);
    }

    public ObjectBigArray(Object initialValue)
    {
        this.initialValue = initialValue;
        array = new Object[INITIAL_SEGMENTS][];
        allocateNewSegment();
    }

    /**
     * Returns the size of this big array in bytes.
     */
    public long sizeOf()
    {
        return INSTANCE_SIZE + SizeOf.sizeOf(array) + (segments * SIZE_OF_SEGMENT);
    }

    /**
     * Returns the current capacity of this big array
     */
    public long getCapacity()
    {
        return capacity;
    }

    /**
     * Returns the element of this big array at specified index.
     *
     * @param index a position in this big array.
     * @return the element of this big array at the specified position.
     */
    @SuppressWarnings("unchecked")
    public T get(long index)
    {
        return (T) array[segment(index)][offset(index)];
    }

    /**
     * Gets the element of this big array at specified index and then sets the provided
     * replacement value to the same index
     *
     * @param index a position in this big array.
     *
     * @return the previously stored value for the specified index
     */
    @SuppressWarnings("unchecked")
    public T getAndSet(long index, T replacement)
    {
        Object[] segment = array[segment(index)];
        int offset = offset(index);
        T result = (T) segment[offset];
        segment[offset] = replacement;
        return result;
    }

    /**
     * Sets the element of this big array at specified index.
     *
     * @param index a position in this big array.
     */
    public void set(long index, T value)
    {
        array[segment(index)][offset(index)] = value;
    }

    /**
     * Sets the element of this big array at specified index if and only if the existing value
     * at that index is null
     *
     * @param index a position in this big array.
     *
     * @return whether the previous value was null and the new value stored
     */
    public boolean setIfNull(long index, T value)
    {
        Object[] segment = array[segment(index)];
        int offset = offset(index);
        if (segment[offset] == null) {
            segment[offset] = value;
            return true;
        }
        return false;
    }

    /**
     * Ensures this big array is at least the specified length.  If the array is smaller, segments
     * are added until the array is larger then the specified length.
     */
    public void ensureCapacity(long length)
    {
        if (capacity > length) {
            return;
        }

        grow(length);
    }

    private void grow(long length)
    {
        // how many segments are required to get to the length?
        int requiredSegments = segment(length) + 1;

        // grow base array if necessary
        if (array.length < requiredSegments) {
            array = Arrays.copyOf(array, requiredSegments);
        }

        // add new segments
        while (segments < requiredSegments) {
            allocateNewSegment();
        }
    }

    private void allocateNewSegment()
    {
        Object[] newSegment = new Object[SEGMENT_SIZE];
        if (initialValue != null) {
            Arrays.fill(newSegment, initialValue);
        }
        array[segments] = newSegment;
        capacity += SEGMENT_SIZE;
        segments++;
    }
}
