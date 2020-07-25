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
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.Deque;

import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestSimpleArrayAllocator
{
    @Test
    public void testNewLease()
    {
        ArrayAllocator allocator = new SimpleArrayAllocator(10);
        testNewLease(allocator);

        allocator = new UncheckedStackArrayAllocator(10);
        testNewLease(allocator);
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

    private void testNewLease(ArrayAllocator allocator)
    {
        Deque<int[]> intArrayList = new ArrayDeque<>();
        for (int i = 0; i < 5; i++) {
            intArrayList.push(allocator.borrowIntArray(10));
        }
        assertEquals(allocator.getBorrowedArrayCount(), 5);
        assertEquals(allocator.getEstimatedSizeInBytes(), sizeOfIntArray(10) * 5);

        Deque<byte[]> byteArrayList = new ArrayDeque<>();
        for (int i = 0; i < 5; i++) {
            byteArrayList.push(allocator.borrowByteArray(10));
        }
        assertEquals(allocator.getBorrowedArrayCount(), 10);
        assertEquals(allocator.getEstimatedSizeInBytes(), (sizeOfIntArray(10) + sizeOfByteArray(10)) * 5);

        for (int i = 0; i < 5; i++) {
            allocator.returnArray(intArrayList.pop());
        }
        assertEquals(allocator.getBorrowedArrayCount(), 5);
        assertEquals(allocator.getEstimatedSizeInBytes(), (sizeOfIntArray(10) + sizeOfByteArray(10)) * 5);

        for (int i = 0; i < 5; i++) {
            allocator.returnArray(byteArrayList.pop());
        }
        assertEquals(allocator.getBorrowedArrayCount(), 0);
        assertEquals(allocator.getEstimatedSizeInBytes(), (sizeOfIntArray(10) + sizeOfByteArray(10)) * 5);

        int[] intBorrowed = allocator.borrowIntArray(101);
        assertEquals(allocator.getEstimatedSizeInBytes(), sizeOfIntArray(101) + sizeOfByteArray(10) * 5);
        allocator.returnArray(intBorrowed);
        assertEquals(allocator.getEstimatedSizeInBytes(), sizeOfIntArray(101) + sizeOfByteArray(10) * 5);

        byte[] byteBorrowed = allocator.borrowByteArray(101);
        assertEquals(allocator.getEstimatedSizeInBytes(), sizeOfIntArray(101) + sizeOfByteArray(101));
        allocator.returnArray(byteBorrowed);
        assertEquals(allocator.getEstimatedSizeInBytes(), sizeOfIntArray(101) + sizeOfByteArray(101));
    }
}
