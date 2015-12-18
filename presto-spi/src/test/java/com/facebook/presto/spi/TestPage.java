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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.UUID;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static io.airlift.slice.Slices.wrappedIntArray;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

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
        Slice[] expectedValues = createExpectedValues(10);
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), expectedValues.length);
        for (Slice expectedValue : expectedValues) {
            BIGINT.writeLong(blockBuilder, expectedValue.length());
        }
        Block lengthsDictionary = blockBuilder.build();

        // Create 2 dictionary blocks with the same source id
        UUID commonSourceId = UUID.randomUUID();
        DictionaryBlock commonSourceIdBlock1 = createDictionaryBlock(expectedValues, 100, commonSourceId);
        DictionaryBlock commonSourceIdBlock2 = new DictionaryBlock(commonSourceIdBlock1.getPositionCount(), lengthsDictionary, commonSourceIdBlock1.getIds(), commonSourceId);

        // Create block with a different source id
        DictionaryBlock randomSourceIdBlock = createDictionaryBlock(expectedValues, 100, UUID.randomUUID());

        Page page = new Page(commonSourceIdBlock1, randomSourceIdBlock, commonSourceIdBlock2);
        page.compact();

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

    protected static Slice createExpectedValue(int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(16);
        for (int index = 0; index < length; index++) {
            dynamicSliceOutput.writeByte(length * (index + 1));
        }
        return dynamicSliceOutput.slice();
    }

    private static DictionaryBlock createDictionaryBlock(Slice[] expectedValues, int positionCount, UUID uuid)
    {
        int dictionarySize = expectedValues.length;
        int[] ids = new int[positionCount];

        for (int i = 0; i < positionCount; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(positionCount, new SliceArrayBlock(dictionarySize, expectedValues), wrappedIntArray(ids), uuid);
    }
}
