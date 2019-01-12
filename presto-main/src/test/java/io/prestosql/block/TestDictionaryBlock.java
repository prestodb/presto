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
package io.prestosql.block;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.DictionaryId;
import io.prestosql.spi.block.VariableWidthBlockBuilder;
import org.testng.annotations.Test;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.prestosql.block.BlockAssertions.createSlicesBlock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDictionaryBlock
        extends AbstractTestBlock
{
    @Test
    public void testSizeInBytes()
    {
        Slice[] expectedValues = createExpectedValues(10);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);
        assertEquals(dictionaryBlock.getSizeInBytes(), dictionaryBlock.getDictionary().getSizeInBytes() + (100 * SIZE_OF_INT));
    }

    @Test
    public void testLogicalSizeInBytes()
    {
        // The 10 Slices in the array will be of lengths 0 to 9.
        Slice[] expectedValues = createExpectedValues(10);

        // The dictionary within the dictionary block is expected to be a VariableWidthBlock of size 95 bytes.
        // 45 bytes for the expectedValues Slices (sum of seq(0,9)) and 50 bytes for the position and isNull array (total 10 positions).
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);
        assertEquals(dictionaryBlock.getDictionary().getLogicalSizeInBytes(), 95);

        // The 100 positions in the dictionary block index to 10 positions in the underlying dictionary (10 each).
        // Logical size calculation accounts for 4 bytes of offset and 1 byte of isNull. Therefore the expected unoptimized
        // size is 10 times the size of the underlying dictionary (VariableWidthBlock).
        assertEquals(dictionaryBlock.getLogicalSizeInBytes(), 95 * 10);

        // With alternating nulls, we have 21 positions, with the same size calculation as above.
        dictionaryBlock = createDictionaryBlock(alternatingNullValues(expectedValues), 210);
        assertEquals(dictionaryBlock.getDictionary().getPositionCount(), 21);
        assertEquals(dictionaryBlock.getDictionary().getLogicalSizeInBytes(), 150);

        // The null positions should be included in the logical size.
        assertEquals(dictionaryBlock.getLogicalSizeInBytes(), 150 * 10);
    }

    @Test
    public void testCopyRegionCreatesCompactBlock()
    {
        Slice[] expectedValues = createExpectedValues(10);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);

        DictionaryBlock copyRegionDictionaryBlock = (DictionaryBlock) dictionaryBlock.copyRegion(1, 3);
        assertTrue(copyRegionDictionaryBlock.isCompact());
    }

    @Test
    public void testCopyPositionsWithCompaction()
    {
        Slice[] expectedValues = createExpectedValues(10);
        Slice firstExpectedValue = expectedValues[0];
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);

        int[] positionsToCopy = new int[] {0, 10, 20, 30, 40};
        DictionaryBlock copiedBlock = (DictionaryBlock) dictionaryBlock.copyPositions(positionsToCopy, 0, positionsToCopy.length);

        assertEquals(copiedBlock.getDictionary().getPositionCount(), 1);
        assertEquals(copiedBlock.getPositionCount(), positionsToCopy.length);
        assertBlock(copiedBlock.getDictionary(), TestDictionaryBlock::createBlockBuilder, new Slice[] {firstExpectedValue});
        assertBlock(copiedBlock, TestDictionaryBlock::createBlockBuilder, new Slice[] {firstExpectedValue, firstExpectedValue, firstExpectedValue, firstExpectedValue, firstExpectedValue});
    }

    @Test
    public void testCopyPositionsWithCompactionsAndReorder()
    {
        Slice[] expectedValues = createExpectedValues(10);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);
        int[] positionsToCopy = new int[] {50, 55, 40, 45, 60};

        DictionaryBlock copiedBlock = (DictionaryBlock) dictionaryBlock.copyPositions(positionsToCopy, 0, positionsToCopy.length);

        assertEquals(copiedBlock.getDictionary().getPositionCount(), 2);
        assertEquals(copiedBlock.getPositionCount(), positionsToCopy.length);

        assertBlock(copiedBlock.getDictionary(), TestDictionaryBlock::createBlockBuilder, new Slice[] {expectedValues[0], expectedValues[5]});
        assertDictionaryIds(copiedBlock, 0, 1, 0, 1, 0);
    }

    @Test
    public void testCopyPositionsSamePosition()
    {
        Slice[] expectedValues = createExpectedValues(10);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);
        int[] positionsToCopy = new int[] {52, 52, 52};

        DictionaryBlock copiedBlock = (DictionaryBlock) dictionaryBlock.copyPositions(positionsToCopy, 0, positionsToCopy.length);

        assertEquals(copiedBlock.getDictionary().getPositionCount(), 1);
        assertEquals(copiedBlock.getPositionCount(), positionsToCopy.length);

        assertBlock(copiedBlock.getDictionary(), TestDictionaryBlock::createBlockBuilder, new Slice[] {expectedValues[2]});
        assertDictionaryIds(copiedBlock, 0, 0, 0);
    }

    @Test
    public void testCopyPositionsNoCompaction()
    {
        Slice[] expectedValues = createExpectedValues(1);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);

        int[] positionsToCopy = new int[] {0, 2, 4, 5};
        DictionaryBlock copiedBlock = (DictionaryBlock) dictionaryBlock.copyPositions(positionsToCopy, 0, positionsToCopy.length);

        assertEquals(copiedBlock.getPositionCount(), positionsToCopy.length);
        assertBlock(copiedBlock.getDictionary(), TestDictionaryBlock::createBlockBuilder, expectedValues);
    }

    @Test
    public void testCompact()
    {
        Slice[] expectedValues = createExpectedValues(5);
        DictionaryBlock dictionaryBlock = createDictionaryBlockWithUnreferencedKeys(expectedValues, 10);

        assertEquals(dictionaryBlock.isCompact(), false);
        DictionaryBlock compactBlock = dictionaryBlock.compact();
        assertNotEquals(dictionaryBlock.getDictionarySourceId(), compactBlock.getDictionarySourceId());

        assertEquals(compactBlock.getDictionary().getPositionCount(), (expectedValues.length / 2) + 1);
        assertBlock(compactBlock.getDictionary(), TestDictionaryBlock::createBlockBuilder, new Slice[] {expectedValues[0], expectedValues[1], expectedValues[3]});
        assertDictionaryIds(compactBlock, 0, 1, 1, 2, 2, 0, 1, 1, 2, 2);
        assertEquals(compactBlock.isCompact(), true);

        DictionaryBlock reCompactedBlock = compactBlock.compact();
        assertEquals(reCompactedBlock.getDictionarySourceId(), compactBlock.getDictionarySourceId());
    }

    @Test
    public void testCompactAllKeysReferenced()
    {
        Slice[] expectedValues = createExpectedValues(5);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 10);
        DictionaryBlock compactBlock = dictionaryBlock.compact();

        // When there is nothing to compact, we return the same block
        assertEquals(compactBlock.getDictionary(), dictionaryBlock.getDictionary());
        assertEquals(compactBlock.getPositionCount(), dictionaryBlock.getPositionCount());
        for (int position = 0; position < compactBlock.getPositionCount(); position++) {
            assertEquals(compactBlock.getId(position), dictionaryBlock.getId(position));
        }
        assertEquals(compactBlock.isCompact(), true);
    }

    @Test
    public void testBasicGetPositions()
    {
        Slice[] expectedValues = createExpectedValues(10);
        Block dictionaryBlock = new DictionaryBlock(createSlicesBlock(expectedValues), new int[] {0, 1, 2, 3, 4, 5});
        assertBlock(dictionaryBlock, TestDictionaryBlock::createBlockBuilder, new Slice[] {expectedValues[0], expectedValues[1], expectedValues[2], expectedValues[3], expectedValues[4], expectedValues[5]});
        DictionaryId dictionaryId = ((DictionaryBlock) dictionaryBlock).getDictionarySourceId();

        // first getPositions
        dictionaryBlock = dictionaryBlock.getPositions(new int[] {0, 8, 1, 2, 4, 5, 7, 9}, 2, 4);
        assertBlock(dictionaryBlock, TestDictionaryBlock::createBlockBuilder, new Slice[] {expectedValues[1], expectedValues[2], expectedValues[4], expectedValues[5]});
        assertEquals(((DictionaryBlock) dictionaryBlock).getDictionarySourceId(), dictionaryId);

        // second getPositions
        dictionaryBlock = dictionaryBlock.getPositions(new int[] {0, 1, 3, 0, 0}, 0, 3);
        assertBlock(dictionaryBlock, TestDictionaryBlock::createBlockBuilder, new Slice[] {expectedValues[1], expectedValues[2], expectedValues[5]});
        assertEquals(((DictionaryBlock) dictionaryBlock).getDictionarySourceId(), dictionaryId);

        // third getPositions; we do not validate if -1 is an invalid position
        dictionaryBlock = dictionaryBlock.getPositions(new int[] {-1, -1, 0, 1, 2}, 2, 3);
        assertBlock(dictionaryBlock, TestDictionaryBlock::createBlockBuilder, new Slice[] {expectedValues[1], expectedValues[2], expectedValues[5]});
        assertEquals(((DictionaryBlock) dictionaryBlock).getDictionarySourceId(), dictionaryId);

        // mixed getPositions
        dictionaryBlock = dictionaryBlock.getPositions(new int[] {0, 2, 2}, 0, 3);
        assertBlock(dictionaryBlock, TestDictionaryBlock::createBlockBuilder, new Slice[] {expectedValues[1], expectedValues[5], expectedValues[5]});
        assertEquals(((DictionaryBlock) dictionaryBlock).getDictionarySourceId(), dictionaryId);

        // duplicated getPositions
        dictionaryBlock = dictionaryBlock.getPositions(new int[] {1, 1, 1, 1, 1}, 0, 5);
        assertBlock(dictionaryBlock, TestDictionaryBlock::createBlockBuilder, new Slice[] {expectedValues[5], expectedValues[5], expectedValues[5], expectedValues[5], expectedValues[5]});
        assertEquals(((DictionaryBlock) dictionaryBlock).getDictionarySourceId(), dictionaryId);

        // out of range
        for (int position : ImmutableList.of(-1, 6)) {
            try {
                dictionaryBlock.getPositions(new int[] {position}, 0, 1);
                fail("Expected to fail");
            }
            catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().startsWith("Invalid position"));
            }
        }

        for (int offset : ImmutableList.of(-1, 6)) {
            try {
                dictionaryBlock.getPositions(new int[] {0}, offset, 1);
                fail("Expected to fail");
            }
            catch (IndexOutOfBoundsException e) {
                assertTrue(e.getMessage().startsWith("Invalid offset"));
            }
        }

        for (int length : ImmutableList.of(-1, 6)) {
            try {
                dictionaryBlock.getPositions(new int[] {0}, 0, length);
                fail("Expected to fail");
            }
            catch (IndexOutOfBoundsException e) {
                assertTrue(e.getMessage().startsWith("Invalid offset"));
            }
        }
    }

    @Test
    public void testCompactGetPositions()
    {
        DictionaryBlock block = new DictionaryBlock(createSlicesBlock(createExpectedValues(10)), new int[] {0, 1, 2, 3, 4, 5}).compact();

        // 3, 3, 4, 5, 2, 0, 1, 1
        block = (DictionaryBlock) block.getPositions(new int[] {3, 3, 4, 5, 2, 0, 1, 1}, 0, 7);
        assertTrue(block.isCompact());

        // 3, 3, 4, 5, 2, 0, 1, 1, 0, 2, 5, 4, 3
        block = (DictionaryBlock) block.getPositions(new int[] {0, 1, 2, 3, 4, 5, 6, 6, 5, 4, 3, 2, 1}, 0, 12);
        assertTrue(block.isCompact());

        // 3, 4, 3, 4, 3
        block = (DictionaryBlock) block.getPositions(new int[] {0, 2, 0, 2, 0}, 0, 5);
        assertFalse(block.isCompact());

        block = block.compact();
        // 3, 4, 4, 4
        block = (DictionaryBlock) block.getPositions(new int[] {0, 1, 1, 1}, 0, 4);
        assertTrue(block.isCompact());

        // 4, 4, 4, 4
        block = (DictionaryBlock) block.getPositions(new int[] {1, 1, 1, 1}, 0, 4);
        assertFalse(block.isCompact());

        block = block.compact();
        // 4
        block = (DictionaryBlock) block.getPositions(new int[] {0}, 0, 1);
        assertTrue(block.isCompact());

        // empty
        block = (DictionaryBlock) block.getPositions(new int[] {}, 0, 0);
        assertFalse(block.isCompact());

        block = block.compact();
        // empty
        block = (DictionaryBlock) block.getPositions(new int[] {}, 0, 0);
        assertTrue(block.isCompact());
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        int positionCount = 10;
        int dictionaryPositionCount = 100;
        Slice[] expectedValues = createExpectedValues(positionCount);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, dictionaryPositionCount);
        for (int position = 0; position < dictionaryPositionCount; position++) {
            assertEquals(dictionaryBlock.getEstimatedDataSizeForStats(position), expectedValues[position % positionCount].length());
        }
    }

    private static DictionaryBlock createDictionaryBlockWithUnreferencedKeys(Slice[] expectedValues, int positionCount)
    {
        // adds references to 0 and all odd indexes
        int dictionarySize = expectedValues.length;
        int[] ids = new int[positionCount];

        for (int i = 0; i < positionCount; i++) {
            int index = i % dictionarySize;
            if (index % 2 == 0 && index != 0) {
                index--;
            }
            ids[i] = index;
        }
        return new DictionaryBlock(createSlicesBlock(expectedValues), ids);
    }

    private static DictionaryBlock createDictionaryBlock(Slice[] expectedValues, int positionCount)
    {
        int dictionarySize = expectedValues.length;
        int[] ids = new int[positionCount];

        for (int i = 0; i < positionCount; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(createSlicesBlock(expectedValues), ids);
    }

    private static BlockBuilder createBlockBuilder()
    {
        return new VariableWidthBlockBuilder(null, 100, 1);
    }

    private static void assertDictionaryIds(DictionaryBlock dictionaryBlock, int... expected)
    {
        assertEquals(dictionaryBlock.getPositionCount(), expected.length);
        for (int position = 0; position < dictionaryBlock.getPositionCount(); position++) {
            assertEquals(dictionaryBlock.getId(position), expected[position]);
        }
    }
}
