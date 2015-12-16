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
package com.facebook.presto.block;

import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.Slices.wrappedIntArray;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDictionaryBlock
        extends AbstractTestBlock
{
    @Test
    public void testSizeInBytes()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(10);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);

        int sizeInBytes = 0;
        for (Slice expectedValue : expectedValues) {
            sizeInBytes += expectedValue.length();
        }
        assertEquals(dictionaryBlock.getSizeInBytes(), sizeInBytes + (100 * SIZE_OF_INT));
    }

    @Test
    public void testCopyRegionCreatesCompactBlock()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(10);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);

        DictionaryBlock copyRegionDictionaryBlock = (DictionaryBlock) dictionaryBlock.copyRegion(1, 3);
        assertTrue(copyRegionDictionaryBlock.isCompact());
    }

    @Test
    public void testCopyPositionsWithCompaction()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(10);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);

        List<Integer> positionsToCopy = Ints.asList(0, 10, 20, 30, 40);
        DictionaryBlock copiedBlock = (DictionaryBlock) dictionaryBlock.copyPositions(positionsToCopy);

        assertEquals(copiedBlock.getDictionary().getPositionCount(), 1);
        assertEquals(copiedBlock.getPositionCount(), positionsToCopy.size());
        assertBlock(copiedBlock.getDictionary(), Arrays.copyOfRange(expectedValues, 0, 1));
    }

    @Test
    public void testCopyPositionsWithCompactionsAndReorder()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(10);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);
        List<Integer> positionsToCopy = Ints.asList(50, 55, 40, 45, 60);

        DictionaryBlock copiedBlock = (DictionaryBlock) dictionaryBlock.copyPositions(positionsToCopy);

        assertEquals(copiedBlock.getDictionary().getPositionCount(), 2);
        assertEquals(copiedBlock.getPositionCount(), positionsToCopy.size());

        assertBlock(copiedBlock.getDictionary(), new Slice[] { expectedValues[0], expectedValues[5] });
        assertEquals(copiedBlock.getIds(), wrappedIntArray(0, 1, 0, 1, 0));
    }

    @Test
    public void testCopyPositionsSamePosition()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(10);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);
        List<Integer> positionsToCopy = Ints.asList(52, 52, 52);

        DictionaryBlock copiedBlock = (DictionaryBlock) dictionaryBlock.copyPositions(positionsToCopy);

        assertEquals(copiedBlock.getDictionary().getPositionCount(), 1);
        assertEquals(copiedBlock.getPositionCount(), positionsToCopy.size());

        assertBlock(copiedBlock.getDictionary(), new Slice[] { expectedValues[2] });
        assertEquals(copiedBlock.getIds(), wrappedIntArray(0, 0, 0));
    }

    @Test
    public void testCopyPositionsNoCompaction()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(1);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);

        List<Integer> positionsToCopy = Ints.asList(0, 2, 4, 5);
        DictionaryBlock copiedBlock = (DictionaryBlock) dictionaryBlock.copyPositions(positionsToCopy);

        assertEquals(copiedBlock.getPositionCount(), positionsToCopy.size());
        assertBlock(copiedBlock.getDictionary(), expectedValues);
    }

    @Test
    public void testCompact()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(5);
        DictionaryBlock dictionaryBlock = createDictionaryBlockWithUnreferencedKeys(expectedValues, 10);

        assertEquals(dictionaryBlock.isCompact(), false);
        DictionaryBlock compactBlock = dictionaryBlock.compact();

        assertEquals(compactBlock.getDictionary().getPositionCount(), (expectedValues.length / 2) + 1);
        assertBlock(compactBlock.getDictionary(), new Slice[] { expectedValues[0], expectedValues[1], expectedValues[3] });
        assertEquals(compactBlock.getIds(), wrappedIntArray(0, 1, 1, 2, 2, 0, 1, 1, 2, 2));
        assertEquals(compactBlock.isCompact(), true);
    }

    @Test
    public void testCompactAllKeysReferenced()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(5);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 10);
        DictionaryBlock compactBlock = dictionaryBlock.compact();

        // When there is nothing to compact, we return the same block
        assertEquals(compactBlock.getDictionary(), dictionaryBlock.getDictionary());
        assertEquals(compactBlock.getIds(), dictionaryBlock.getIds());
        assertEquals(compactBlock.isCompact(), true);
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
        return new DictionaryBlock(positionCount, new SliceArrayBlock(dictionarySize, expectedValues), wrappedIntArray(ids));
    }

    private static DictionaryBlock createDictionaryBlock(Slice[] expectedValues, int positionCount)
    {
        int dictionarySize = expectedValues.length;
        int[] ids = new int[positionCount];

        for (int i = 0; i < positionCount; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(positionCount, new SliceArrayBlock(dictionarySize, expectedValues), wrappedIntArray(ids));
    }
}
