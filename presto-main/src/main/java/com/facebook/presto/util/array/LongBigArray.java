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
package com.facebook.presto.util.array;

import io.airlift.slice.SizeOf;

import java.util.Arrays;

import static com.facebook.presto.util.array.BigArrays.INITIAL_SEGMENTS;
import static com.facebook.presto.util.array.BigArrays.SEGMENT_SIZE;
import static com.facebook.presto.util.array.BigArrays.offset;
import static com.facebook.presto.util.array.BigArrays.segment;
import static io.airlift.slice.SizeOf.sizeOfLongArray;

// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/)
// Copyright (C) 2010-2013 Sebastiano Vigna
public final class LongBigArray
{
    private static final long SIZE_OF_SEGMENT = sizeOfLongArray(SEGMENT_SIZE);

    private final long initialValue;

    private long[][] array;
    private int capacity;
    private int segments;

    /**
     * Creates a new big array containing one initial segment
     */
    public LongBigArray()
    {
        this(0L);
    }

    /**
     * Creates a new big array containing one initial segment filled with the specified default value
     */
    public LongBigArray(long initialValue)
    {
        this.initialValue = initialValue;
        array = new long[INITIAL_SEGMENTS][];
        allocateNewSegment();
    }

    /**
     * Returns the size of this big array in bytes.
     */
    public long sizeOf()
    {
        return SizeOf.sizeOf(array) + (segments * SIZE_OF_SEGMENT);
    }

    /**
     * Returns the element of this big array at specified index.
     *
     * @param index a position in this big array.
     * @return the element of this big array at the specified position.
     */
    public long get(long index)
    {
        return array[segment(index)][offset(index)];
    }

    /**
     * Sets the element of this big array at specified index.
     *
     * @param index a position in this big array.
     */
    public void set(long index, long value)
    {
        array[segment(index)][offset(index)] = value;
    }

    /**
     * Increments the element of this big array at specified index.
     *
     * @param index a position in this big array.
     */
    public void increment(long index)
    {
        array[segment(index)][offset(index)]++;
    }

    /**
     * Adds the specified value to the specified element of this big array.
     *
     * @param index a position in this big array.
     * @param value the value
     */
    public void add(long index, long value)
    {
        array[segment(index)][offset(index)] += value;
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
        long[] newSegment = new long[SEGMENT_SIZE];
        if (initialValue != 0) {
            Arrays.fill(newSegment, initialValue);
        }
        array[segments] = newSegment;
        capacity += SEGMENT_SIZE;
        segments++;
    }
}
