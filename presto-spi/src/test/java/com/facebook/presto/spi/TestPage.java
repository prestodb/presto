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
package com.facebook.presto.spi;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.DictionaryId;
import com.facebook.presto.spi.block.SliceArrayBlock;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.block.DictionaryId.randomDictionaryId;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestPage
{
    @Test
    public void testGetRegion()
            throws Exception
    {
        assertEquals(new Page(10).getRegion(5, 5).getPositionCount(), 5);
    }

    @Test
    public void testGetEmptyRegion()
            throws Exception
    {
        assertEquals(new Page(0).getRegion(0, 0).getPositionCount(), 0);
        assertEquals(new Page(10).getRegion(5, 0).getPositionCount(), 0);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class, expectedExceptionsMessageRegExp = "Invalid position .* in page with .* positions")
    public void testGetRegionExceptions()
            throws Exception
    {
        new Page(0).getRegion(1, 1);
    }

    @Test
    public void testGetRegionFromNoColumnPage()
            throws Exception
    {
        assertEquals(new Page(100).getRegion(0, 10).getPositionCount(), 10);
    }

    @Test
    public void testCompactDictionaryBlocks()
            throws Exception
    {
        int positionCount = 100;

        // Create 2 dictionary blocks with the same source id
        DictionaryId commonSourceId = randomDictionaryId();
        int commonDictionaryUsedPositions = 20;
        int[] commonDictionaryIds = getDictionaryIds(positionCount, commonDictionaryUsedPositions);

        // first dictionary contains "varbinary" values
        Slice[] dictionaryValues1 = createExpectedValues(50);
        SliceArrayBlock dictionary1 = new SliceArrayBlock(dictionaryValues1.length, dictionaryValues1);
        DictionaryBlock commonSourceIdBlock1 = new DictionaryBlock(positionCount, dictionary1, commonDictionaryIds, commonSourceId);

        // second dictionary block is "length(firstColumn)"
        BlockBuilder dictionary2 = BIGINT.createBlockBuilder(new BlockBuilderStatus(), dictionary1.getPositionCount());
        for (Slice expectedValue : dictionaryValues1) {
            BIGINT.writeLong(dictionary2, expectedValue.length());
        }
        DictionaryBlock commonSourceIdBlock2 = new DictionaryBlock(positionCount, dictionary2.build(), commonDictionaryIds, commonSourceId);

        // Create block with a different source id, dictionary size, used
        int otherDictionaryUsedPositions = 30;
        int[] otherDictionaryIds = getDictionaryIds(positionCount, otherDictionaryUsedPositions);
        SliceArrayBlock dictionary3 = new SliceArrayBlock(70, createExpectedValues(70));
        DictionaryBlock randomSourceIdBlock = new DictionaryBlock(positionCount, dictionary3, otherDictionaryIds);

        Page page = new Page(commonSourceIdBlock1, randomSourceIdBlock, commonSourceIdBlock2);
        page.compact();

        // dictionary blocks should all be compact
        assertTrue(((DictionaryBlock) page.getBlock(0)).isCompact());
        assertTrue(((DictionaryBlock) page.getBlock(1)).isCompact());
        assertTrue(((DictionaryBlock) page.getBlock(2)).isCompact());
        assertEquals(((DictionaryBlock) page.getBlock(0)).getDictionary().getPositionCount(), commonDictionaryUsedPositions);
        assertEquals(((DictionaryBlock) page.getBlock(1)).getDictionary().getPositionCount(), otherDictionaryUsedPositions);
        assertEquals(((DictionaryBlock) page.getBlock(2)).getDictionary().getPositionCount(), commonDictionaryUsedPositions);

        // Blocks that had the same source id before compacting page should have the same source id after compacting page
        assertNotEquals(((DictionaryBlock) page.getBlock(0)).getDictionarySourceId(), ((DictionaryBlock) page.getBlock(1)).getDictionarySourceId());
        assertEquals(((DictionaryBlock) page.getBlock(0)).getDictionarySourceId(), ((DictionaryBlock) page.getBlock(2)).getDictionarySourceId());
    }

    private static Slice[] createExpectedValues(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(position);
        }
        return expectedValues;
    }

    private static Slice createExpectedValue(int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(16);
        for (int index = 0; index < length; index++) {
            dynamicSliceOutput.writeByte(length * (index + 1));
        }
        return dynamicSliceOutput.slice();
    }

    private static int[] getDictionaryIds(int positionCount, int dictionarySize)
    {
        checkArgument(positionCount > dictionarySize);
        int[] ids = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            ids[i] = i % dictionarySize;
        }
        return ids;
    }
}
