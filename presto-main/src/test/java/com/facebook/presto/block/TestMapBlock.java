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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.SingleMapBlock;
import com.facebook.presto.spi.type.MapType;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;

public class TestMapBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        testWith(createTestMap(9, 3, 4, 0, 8, 0, 6, 5));
    }

    private Map<String, Long>[] createTestMap(int... entryCounts)
    {
        Map<String, Long>[] result = new Map[entryCounts.length];
        for (int rowNumber = 0; rowNumber < entryCounts.length; rowNumber++) {
            int entryCount = entryCounts[rowNumber];
            ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();
            for (int entryNumber = 0; entryNumber < entryCount; entryNumber++) {
                builder.put("key" + entryNumber, rowNumber * 100L + entryNumber);
            }
            result[rowNumber] = builder.build();
        }
        return result;
    }

    private void testWith(Map<String, Long>[] expectedValues)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);

        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder, Ints.asList(0, 1, 3, 4, 7));
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(0, 1, 3, 4, 7));
        assertBlockFilteredPositions(expectedValues, blockBuilder, Ints.asList(2, 3, 5, 6));
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(2, 3, 5, 6));

        Map<String, Long>[] expectedValuesWithNull = (Map<String, Long>[]) alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);

        assertBlock(blockBuilderWithNull, expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull, Ints.asList(0, 1, 5, 6, 7, 10, 11, 12, 15));
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), Ints.asList(0, 1, 5, 6, 7, 10, 11, 12, 15));
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull, Ints.asList(2, 3, 4, 9, 13, 14));
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), Ints.asList(2, 3, 4, 9, 13, 14));
    }

    private BlockBuilder createBlockBuilderWithValues(Map<String, Long>[] maps)
    {
        MapType mapType = mapType(VARCHAR, BIGINT);
        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(new BlockBuilderStatus(), 1);
        for (Map<String, Long> map : maps) {
            createBlockBuilderWithValues(map, mapBlockBuilder);
        }
        return mapBlockBuilder;
    }

    private void createBlockBuilderWithValues(Map<String, Long> map, BlockBuilder mapBlockBuilder)
    {
        if (map == null) {
            mapBlockBuilder.appendNull();
        }
        else {
            BlockBuilder elementBlockBuilder = mapBlockBuilder.beginBlockEntry();
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                VARCHAR.writeSlice(elementBlockBuilder, utf8Slice(entry.getKey()));
                BIGINT.writeLong(elementBlockBuilder, entry.getValue());
            }
            mapBlockBuilder.closeEntry();
        }
    }

    @Override
    protected <T> void assertPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue instanceof Map) {
            assertValue(block, position, (Map<String, Long>) expectedValue);
            return;
        }
        super.assertPositionValue(block, position, expectedValue);
    }

    private void assertValue(Block mapBlock, int position, Map<String, Long> map)
    {
        MapType mapType = mapType(VARCHAR, BIGINT);

        // null maps are handled by assertPositionValue
        requireNonNull(map, "map is null");

        assertFalse(mapBlock.isNull(position));
        SingleMapBlock elementBlock = (SingleMapBlock) mapType.getObject(mapBlock, position);
        // assert inserted keys
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            int pos = elementBlock.seekKey(utf8Slice(entry.getKey()));
            assertNotEquals(pos, -1);
            assertEquals(BIGINT.getLong(elementBlock, pos), (long) entry.getValue());
        }
        // assert non-existent keys
        for (int i = 0; i < 10; i++) {
            assertEquals(elementBlock.seekKey(utf8Slice("not-inserted-" + i)), -1);
        }
    }
}
