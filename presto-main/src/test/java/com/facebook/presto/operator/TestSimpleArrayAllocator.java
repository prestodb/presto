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

import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.Deque;

import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestSimpleArrayAllocator
{
    @Test
    public void testNewLease()
    {
        SimpleArrayAllocator allocator = new SimpleArrayAllocator(5);

        Deque<int[]> arrayList = new ArrayDeque<>();
        for (int i = 0; i < 5; i++) {
            arrayList.push(allocator.borrowIntArray(10));
        }
        assertEquals(allocator.getBorrowedArrayCount(), 5);
        assertEquals(allocator.getEstimatedSizeInBytes(), sizeOfIntArray(10) * 5);

        for (int i = 0; i < 5; i++) {
            allocator.returnArray(arrayList.pop());
        }
        assertEquals(allocator.getBorrowedArrayCount(), 0);
        assertEquals(allocator.getEstimatedSizeInBytes(), sizeOfIntArray(10) * 5);

        int[] borrowed = allocator.borrowIntArray(101);
        assertEquals(allocator.getEstimatedSizeInBytes(), sizeOfIntArray(101));
        allocator.returnArray(borrowed);
        assertEquals(allocator.getEstimatedSizeInBytes(), sizeOfIntArray(101));
    }

    @Test
    public void testOverAllocateLeases()
    {
        SimpleArrayAllocator allocator = new SimpleArrayAllocator(2);

        allocator.borrowIntArray(10);
        allocator.borrowIntArray(10);
        assertThrows(IllegalStateException.class, () -> allocator.borrowIntArray(10));
    }

    @Test
    public void testReturnArrayNotBorrowed()
    {
        SimpleArrayAllocator allocator = new SimpleArrayAllocator(2);

        int[] array = allocator.borrowIntArray(10);
        allocator.returnArray(array);
        assertThrows(IllegalArgumentException.class, () -> allocator.returnArray(array));
    }
}
