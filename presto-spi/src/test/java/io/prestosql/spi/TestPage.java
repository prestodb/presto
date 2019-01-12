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
package io.prestosql.spi;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.DictionaryId;
import org.testng.annotations.Test;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.block.DictionaryId.randomDictionaryId;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestPage
{
    @Test
    public void testGetRegion()
    {
        assertEquals(new Page(10).getRegion(5, 5).getPositionCount(), 5);
    }

    @Test
    public void testGetEmptyRegion()
    {
        assertEquals(new Page(0).getRegion(0, 0).getPositionCount(), 0);
        assertEquals(new Page(10).getRegion(5, 0).getPositionCount(), 0);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class, expectedExceptionsMessageRegExp = "Invalid position 1 and length 1 in page with 0 positions")
    public void testGetRegionExceptions()
    {
        new Page(0).getRegion(1, 1);
    }

    @Test
    public void testGetRegionFromNoColumnPage()
    {
        assertEquals(new Page(100).getRegion(0, 10).getPositionCount(), 10);
    }

    @Test
    public void testCompactDictionaryBlocks()
    {
        int positionCount = 100;

        // Create 2 dictionary blocks with the same source id
        DictionaryId commonSourceId = randomDictionaryId();
        int commonDictionaryUsedPositions = 20;
        int[] commonDictionaryIds = getDictionaryIds(positionCount, commonDictionaryUsedPositions);

        // first dictionary contains "varbinary" values
        Slice[] dictionaryValues1 = createExpectedValues(50);
        Block dictionary1 = createSlicesBlock(dictionaryValues1);
        DictionaryBlock commonSourceIdBlock1 = new DictionaryBlock(positionCount, dictionary1, commonDictionaryIds, commonSourceId);

        // second dictionary block is "length(firstColumn)"
        BlockBuilder dictionary2 = BIGINT.createBlockBuilder(null, dictionary1.getPositionCount());
        for (Slice expectedValue : dictionaryValues1) {
            BIGINT.writeLong(dictionary2, expectedValue.length());
        }
        DictionaryBlock commonSourceIdBlock2 = new DictionaryBlock(positionCount, dictionary2.build(), commonDictionaryIds, commonSourceId);

        // Create block with a different source id, dictionary size, used
        int otherDictionaryUsedPositions = 30;
        int[] otherDictionaryIds = getDictionaryIds(positionCount, otherDictionaryUsedPositions);
        Block dictionary3 = createSlicesBlock(createExpectedValues(70));
        DictionaryBlock randomSourceIdBlock = new DictionaryBlock(dictionary3, otherDictionaryIds);

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

    @Test
    public void testGetPositions()
    {
        int entries = 10;
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, entries);
        for (int i = 0; i < entries; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();

        Page page = new Page(block, block, block).getPositions(new int[] {0, 1, 1, 1, 2, 5, 5}, 1, 5);
        assertEquals(page.getPositionCount(), 5);
        for (int i = 0; i < 3; i++) {
            assertEquals(page.getBlock(i).getLong(0, 0), 1);
            assertEquals(page.getBlock(i).getLong(1, 0), 1);
            assertEquals(page.getBlock(i).getLong(2, 0), 1);
            assertEquals(page.getBlock(i).getLong(3, 0), 2);
            assertEquals(page.getBlock(i).getLong(4, 0), 5);
        }
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

    private static Block createSlicesBlock(Slice[] values)
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, 100);

        for (Slice value : values) {
            verify(value != null);
            VARBINARY.writeSlice(builder, value);
        }
        return builder.build();
    }
}
