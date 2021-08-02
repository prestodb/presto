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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.VariableWidthBlockBuilder;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static com.facebook.airlift.testing.Assertions.assertLessThan;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestSegmentedSliceBlockBuilder
{
    private static final Slice SLICE = utf8Slice("abcdefghijklmnopqrstuvwxyz");

    @Test
    public void testBasicOperations()
    {
        SegmentedSliceBlockBuilder blockBuilder = new SegmentedSliceBlockBuilder(10, 10);
        long retainedSize = addElementsToBlockBuilder(blockBuilder);
        blockBuilder.reset();
        assertEquals(blockBuilder.getSizeInBytes(), 0);
        assertLessThan(blockBuilder.getRetainedSizeInBytes(), retainedSize);
        addElementsToBlockBuilder(blockBuilder);

        int index = 0;
        for (int j = 0; j < 100_000; j++) {
            for (int i = 0; i < SLICE.length(); i++) {
                Slice rawSlice = blockBuilder.getRawSlice(index);
                int offset = blockBuilder.getPositionOffset(index);
                int length = blockBuilder.getSliceLength(index);
                index++;
                assertEquals(length, 1);
                assertTrue(SLICE.equals(i, 1, rawSlice, offset, length));
            }
        }
    }

    @Test
    public void testEqualsAndHashCode()
    {
        SegmentedSliceBlockBuilder blockBuilder = new SegmentedSliceBlockBuilder(10, 10);
        VariableWidthBlockBuilder variableBlockBuilder = new VariableWidthBlockBuilder(null, 10, 10);
        for (int i = 0; i < SLICE.length(); i++) {
            blockBuilder.writeBytes(SLICE, i, 1);
            blockBuilder.closeEntry();

            variableBlockBuilder.writeBytes(SLICE, i, 1);
            variableBlockBuilder.closeEntry();
        }

        Block block = variableBlockBuilder.build();
        for (int i = 0; i < SLICE.length(); i++) {
            assertTrue(blockBuilder.equals(i, block, i, 1));
            assertTrue(blockBuilder.equals(i, 0, block, i, 0, 1));
            assertEquals(blockBuilder.hash(i), variableBlockBuilder.hash(i, 0, 1));
        }
    }

    @Test
    public void testCompareTo()
    {
        SegmentedSliceBlockBuilder blockBuilder = new SegmentedSliceBlockBuilder(10, 10);
        for (int i = 0; i < SLICE.length(); i++) {
            blockBuilder.writeBytes(SLICE, i, 1);
            blockBuilder.closeEntry();
        }

        for (int i = 0; i < SLICE.length() - 1; i++) {
            assertLessThan(blockBuilder.compareTo(i, i + 1), 0);
            assertEquals(blockBuilder.compareTo(i, i), 0);
            assertGreaterThan(blockBuilder.compareTo(i + 1, i), 0);
        }
    }

    private long addElementsToBlockBuilder(SegmentedSliceBlockBuilder blockBuilder)
    {
        int size = 1;
        long retainedSize = blockBuilder.getRetainedSizeInBytes();
        int lastOpenSegmentIndex = blockBuilder.getOpenSegmentIndex();
        for (int j = 0; j < 100_000; j++) {
            for (int i = 0; i < SLICE.length(); i++) {
                blockBuilder.writeBytes(SLICE, i, 1);
                blockBuilder.closeEntry();
                assertEquals(blockBuilder.getPositionCount(), size++);
                Slice rawSlice = blockBuilder.getRawSlice(blockBuilder.getPositionCount() - 1);
                int offset = blockBuilder.getPositionOffset(blockBuilder.getPositionCount() - 1);
                int length = blockBuilder.getSliceLength(blockBuilder.getPositionCount() - 1);
                assertEquals(length, 1);
                assertTrue(SLICE.equals(i, 1, rawSlice, offset, length));
            }
            // Each element has 1 character and 1 offset so (1 + Integer.BYTES)
            assertEquals(blockBuilder.getSizeInBytes(), blockBuilder.getPositionCount() * (1L + Integer.BYTES));

            if (blockBuilder.getOpenSegmentIndex() > lastOpenSegmentIndex) {
                // When new segment is created, retained should should increase due to
                // copied slices and new offsets array allocation.
                assertGreaterThan(blockBuilder.getRetainedSizeInBytes(), retainedSize);
            }
            assertGreaterThanOrEqual(blockBuilder.getRetainedSizeInBytes(), retainedSize);
            retainedSize = blockBuilder.getRetainedSizeInBytes();
            lastOpenSegmentIndex = blockBuilder.getOpenSegmentIndex();
        }
        return retainedSize;
    }
}
