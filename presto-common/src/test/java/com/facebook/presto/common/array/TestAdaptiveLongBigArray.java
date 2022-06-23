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

import org.testng.annotations.Test;

import static com.facebook.presto.common.array.AdaptiveLongBigArray.INITIAL_SEGMENTS;
import static com.facebook.presto.common.array.AdaptiveLongBigArray.INITIAL_SEGMENT_LENGTH;
import static com.facebook.presto.common.array.AdaptiveLongBigArray.MAX_SEGMENT_LENGTH;
import static com.facebook.presto.common.array.AdaptiveLongBigArray.MAX_SEGMENT_SIZE_IN_BYTES;
import static io.airlift.slice.SizeOf.sizeOfLongArray;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static org.testng.Assert.assertEquals;

public class TestAdaptiveLongBigArray
{
    @Test
    public void test()
    {
        AdaptiveLongBigArray array = new AdaptiveLongBigArray();
        array.ensureCapacity(0);
        assertEquals(array.getRetainedSizeInBytes(), expectedRetainedMemorySize(0, 0));

        array.ensureCapacity(1);
        array.set(0, 0xDEADBEAF);
        assertEquals(array.get(0), 0xDEADBEAF);
        assertEquals(array.getRetainedSizeInBytes(), expectedRetainedMemorySize(1, INITIAL_SEGMENT_LENGTH));

        array.ensureCapacity(INITIAL_SEGMENT_LENGTH);
        array.set(INITIAL_SEGMENT_LENGTH - 1, 0xDEADBEAF);
        assertEquals(array.get(INITIAL_SEGMENT_LENGTH - 1), 0xDEADBEAF);
        assertEquals(array.getRetainedSizeInBytes(), expectedRetainedMemorySize(1, INITIAL_SEGMENT_LENGTH));

        array.ensureCapacity(INITIAL_SEGMENT_LENGTH + 1);
        array.set(INITIAL_SEGMENT_LENGTH, 0xDEADBEAF);
        assertEquals(array.get(INITIAL_SEGMENT_LENGTH), 0xDEADBEAF);
        assertEquals(array.getRetainedSizeInBytes(), expectedRetainedMemorySize(1, INITIAL_SEGMENT_LENGTH * 2));

        array.ensureCapacity(2 * INITIAL_SEGMENT_LENGTH + 1);
        array.set(2 * INITIAL_SEGMENT_LENGTH, 0xDEADBEAF);
        assertEquals(array.get(2 * INITIAL_SEGMENT_LENGTH), 0xDEADBEAF);
        assertEquals(array.getRetainedSizeInBytes(), expectedRetainedMemorySize(1, INITIAL_SEGMENT_LENGTH * 4));

        array.ensureCapacity(4 * INITIAL_SEGMENT_LENGTH + 1);
        array.set(4 * INITIAL_SEGMENT_LENGTH, 0xDEADBEAF);
        assertEquals(array.get(4 * INITIAL_SEGMENT_LENGTH), 0xDEADBEAF);
        assertEquals(array.getRetainedSizeInBytes(), expectedRetainedMemorySize(1, INITIAL_SEGMENT_LENGTH * 8));

        array.ensureCapacity(MAX_SEGMENT_LENGTH);
        array.set(MAX_SEGMENT_LENGTH - 1, 0xDEADBEAF);
        assertEquals(array.get(MAX_SEGMENT_LENGTH - 1), 0xDEADBEAF);
        assertEquals(array.getRetainedSizeInBytes(), expectedRetainedMemorySize(1, MAX_SEGMENT_LENGTH));

        array.ensureCapacity(MAX_SEGMENT_LENGTH + 1);
        array.set(MAX_SEGMENT_LENGTH, 0xDEADBEAF);
        assertEquals(array.get(MAX_SEGMENT_LENGTH), 0xDEADBEAF);
        assertEquals(array.getRetainedSizeInBytes(), expectedRetainedMemorySize(2, MAX_SEGMENT_LENGTH));

        // expand directly to the second segment
        array = new AdaptiveLongBigArray();
        array.ensureCapacity(MAX_SEGMENT_LENGTH + 1);
        array.set(4 * INITIAL_SEGMENT_LENGTH, 0xBEAFDEAD);
        assertEquals(array.get(4 * INITIAL_SEGMENT_LENGTH), 0xBEAFDEAD);
        array.set(MAX_SEGMENT_LENGTH, 0xDEADBEAF);
        assertEquals(array.get(MAX_SEGMENT_LENGTH), 0xDEADBEAF);
        assertEquals(array.getRetainedSizeInBytes(), expectedRetainedMemorySize(2, MAX_SEGMENT_LENGTH));

        array = new AdaptiveLongBigArray();
        array.ensureCapacity(MAX_SEGMENT_LENGTH + 3 * INITIAL_SEGMENT_LENGTH + 1);
        array.set(MAX_SEGMENT_LENGTH + 3 * INITIAL_SEGMENT_LENGTH, 0xBEAFDEAD);
        assertEquals(array.get(MAX_SEGMENT_LENGTH + 3 * INITIAL_SEGMENT_LENGTH), 0xBEAFDEAD);
        assertEquals(array.getRetainedSizeInBytes(), expectedRetainedMemorySize(2, MAX_SEGMENT_LENGTH));

        array = new AdaptiveLongBigArray();
        array.ensureCapacity(1);
        array.set(0, 0xDEADBEAF);
        assertEquals(array.get(0), 0xDEADBEAF);
        assertEquals(array.getRetainedSizeInBytes(), expectedRetainedMemorySize(1, INITIAL_SEGMENT_LENGTH));
        array.ensureCapacity(MAX_SEGMENT_LENGTH + 3 * INITIAL_SEGMENT_LENGTH + 1);
        array.set(MAX_SEGMENT_LENGTH + 3 * INITIAL_SEGMENT_LENGTH, 0xBEAFDEAD);
        assertEquals(array.get(MAX_SEGMENT_LENGTH + 3 * INITIAL_SEGMENT_LENGTH), 0xBEAFDEAD);
        array.set(INITIAL_SEGMENT_LENGTH + 1, 0xDEADBEAF);
        assertEquals(array.get(INITIAL_SEGMENT_LENGTH + 1), 0xDEADBEAF);
        assertEquals(array.getRetainedSizeInBytes(), expectedRetainedMemorySize(2, MAX_SEGMENT_LENGTH));
    }

    @Test
    public void testSwap()
    {
        AdaptiveLongBigArray array = new AdaptiveLongBigArray();
        // swap within a segment
        array.ensureCapacity(10);
        array.set(1, 0xDEADBEAF);
        assertEquals(array.get(1), 0xDEADBEAF);
        array.set(9, 0xBEAFDEAD);
        assertEquals(array.get(9), 0xBEAFDEAD);
        array.swap(1, 9);
        assertEquals(array.get(1), 0xBEAFDEAD);
        assertEquals(array.get(9), 0xDEADBEAF);
        // swap between segments
        array.ensureCapacity(MAX_SEGMENT_LENGTH + 1);
        array.set(1, 0xDEADBEAF);
        assertEquals(array.get(1), 0xDEADBEAF);
        array.set(MAX_SEGMENT_LENGTH + 9, 0xBEAFDEAD);
        assertEquals(array.get(MAX_SEGMENT_LENGTH + 9), 0xBEAFDEAD);
        array.swap(1, MAX_SEGMENT_LENGTH + 9);
        assertEquals(array.get(1), 0xBEAFDEAD);
        assertEquals(array.get(MAX_SEGMENT_LENGTH + 9), 0xDEADBEAF);
    }

    private static long expectedRetainedMemorySize(int segments, int lastSegmentLength)
    {
        return AdaptiveLongBigArray.INSTANCE_SIZE +
                sizeOfObjectArray(INITIAL_SEGMENTS) +
                (segments > 0 ? (segments - 1) * MAX_SEGMENT_SIZE_IN_BYTES + sizeOfLongArray(lastSegmentLength) : 0);
    }
}
