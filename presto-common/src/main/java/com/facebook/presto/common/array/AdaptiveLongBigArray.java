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

import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfLongArray;

/**
 * This variation of BigArray is designed to expand segments up to some reasonable extend
 * and add more segments if the maximum segment capacity is reached
 * This implementation allows to keep the redirection table small so it does fit into L1 CPU cache
 */
public class AdaptiveLongBigArray
{
    // visible for testing
    static final int INSTANCE_SIZE = ClassLayout.parseClass(AdaptiveLongBigArray.class).instanceSize();

    // settings are constants due to efficiency considerations
    static final int INITIAL_SEGMENT_LENGTH = 16 * 1024; // 128KB
    static final int MAX_SEGMENT_LENGTH = 32 * 1024 * 1024; // 256MB
    static final long MAX_SEGMENT_SIZE_IN_BYTES = sizeOfLongArray(MAX_SEGMENT_LENGTH);
    static final int INITIAL_SEGMENTS = 10;
    static final int SEGMENT_SHIFT = 25;
    static final int SEGMENT_MASK = MAX_SEGMENT_LENGTH - 1;

    // segments are allocated lazily in ensureCapacity
    // The first segment is allocated initially with INITIAL_SEGMENT_LENGTH
    // and then gradually expanded until it reaches MAX_SEGMENT_LENGTH
    // Second and subsequent segments are directly allocated with MAX_SEGMENT_LENGTH
    private long[][] array;
    // number of allocated segments
    private int segments;
    // number of elements that currently can be stored in the container
    private int capacity;

    public AdaptiveLongBigArray()
    {
        array = new long[INITIAL_SEGMENTS][];
    }

    public long getRetainedSizeInBytes()
    {
        long result = INSTANCE_SIZE + sizeOf(array);
        if (segments == 1) {
            result += sizeOfLongArray(array[0].length);
        }
        else if (segments > 1) {
            result += segments * MAX_SEGMENT_SIZE_IN_BYTES;
        }
        return result;
    }

    public long get(int index)
    {
        return array[segment(index)][offset(index)];
    }

    public void set(int index, long value)
    {
        array[segment(index)][offset(index)] = value;
    }

    public void swap(int first, int second)
    {
        long[] firstSegment = array[segment(first)];
        int firstOffset = offset(first);

        long[] secondSegment = array[segment(second)];
        int secondOffset = offset(second);

        long tmp = firstSegment[firstOffset];
        firstSegment[firstOffset] = secondSegment[secondOffset];
        secondSegment[secondOffset] = tmp;
    }

    public void ensureCapacity(int length)
    {
        if (capacity >= length) {
            return;
        }

        int lastIndex = length - 1;
        int segment = segment(lastIndex);
        int offset = offset(lastIndex);

        // expand segments array if needed
        if (segment >= array.length) {
            int segmentsArrayCapacity = array.length;
            while (segment >= segmentsArrayCapacity) {
                segmentsArrayCapacity *= 2;
            }
            array = Arrays.copyOf(array, segmentsArrayCapacity);
        }

        if (segment == 0) {
            if (array[0] == null) {
                array[0] = new long[INITIAL_SEGMENT_LENGTH];
            }
            // expand segment if needed
            if (offset >= array[0].length) {
                int segmentLength = array[0].length;
                while (offset >= segmentLength) {
                    segmentLength *= 2;
                }
                array[0] = Arrays.copyOf(array[0], segmentLength);
            }
        }
        else {
            for (int i = 0; i <= segment; i++) {
                if (array[i] == null) {
                    array[i] = new long[MAX_SEGMENT_LENGTH];
                }
                if (i == 0 && array[0].length < MAX_SEGMENT_LENGTH) {
                    array[0] = Arrays.copyOf(array[0], MAX_SEGMENT_LENGTH);
                }
            }
        }

        segments = segment + 1;
        capacity = segments == 1 ? array[0].length : MAX_SEGMENT_LENGTH * segments;
    }

    private static int segment(int index)
    {
        return index >>> SEGMENT_SHIFT;
    }

    private static int offset(int index)
    {
        return index & SEGMENT_MASK;
    }

    public void clear()
    {
        array = new long[INITIAL_SEGMENTS][];
        segments = 0;
        capacity = 0;
    }
}
