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
package com.facebook.presto.common.block;

import org.testng.annotations.Test;

import static com.facebook.presto.common.block.ClosingBlockLease.newLease;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestClosingBlockLease
{
    private final Block dummyBlock = RunLengthEncodedBlock.create(BIGINT, 1L, 100);

    @Test
    public void testArrayAllocations()
    {
        CountingArrayAllocator allocator = new CountingArrayAllocator();

        assertEquals(allocator.getBorrowedArrayCount(), 0);
        int[] array = allocator.borrowIntArray(1);
        try (BlockLease ignored = newLease(dummyBlock, () -> allocator.returnArray(array))) {
            assertEquals(allocator.getBorrowedArrayCount(), 1);
        }
        assertEquals(allocator.getBorrowedArrayCount(), 0);
    }

    @Test
    public void testArrayOverReturn()
    {
        CountingArrayAllocator allocator = new CountingArrayAllocator();

        assertEquals(allocator.getBorrowedArrayCount(), 0);
        int[] array = allocator.borrowIntArray(1);
        BlockLease lease = newLease(dummyBlock, () -> allocator.returnArray(array));
        assertEquals(allocator.getBorrowedArrayCount(), 1);
        lease.close();
        assertEquals(allocator.getBorrowedArrayCount(), 0);
        lease.close();
        assertEquals(allocator.getBorrowedArrayCount(), 0);
    }

    @Test
    public void testClosingErrors()
    {
        BlockLease lease = newLease(
                dummyBlock,
                () -> {
                    throw new IllegalStateException("1");
                },
                () -> {
                    throw new IllegalArgumentException("2");
                },
                () -> {
                    throw new StringIndexOutOfBoundsException("3");
                });
        try {
            lease.close();
            fail("expected RuntimeException");
        }
        catch (RuntimeException e) {
            assertEquals(e.getCause().getClass(), IllegalStateException.class);
            assertEquals(e.getCause().getSuppressed().length, 2);
            assertEquals(e.getCause().getSuppressed()[0].getClass(), IllegalArgumentException.class);
            assertEquals(e.getCause().getSuppressed()[1].getClass(), StringIndexOutOfBoundsException.class);
        }
    }
}
