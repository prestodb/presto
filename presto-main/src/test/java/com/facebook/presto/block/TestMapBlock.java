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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.ByteArrayBlock;
import com.facebook.presto.spi.block.MapBlockBuilder;
import com.facebook.presto.spi.block.SingleMapBlock;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.spi.block.MethodHandleUtil.compose;
import static com.facebook.presto.spi.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestMapBlock
        extends AbstractTestBlock
{
    private static final TypeManager TYPE_MANAGER = new TypeRegistry();

    static {
        // associate TYPE_MANAGER with a function registry
        new FunctionRegistry(TYPE_MANAGER, new BlockEncodingManager(TYPE_MANAGER), new FeaturesConfig());
    }

    @Test
    public void test()
    {
        testWith(createTestMap(9, 3, 4, 0, 8, 0, 6, 5));
    }

    public void testCompactBlock()
    {
        // Test Constructor
        Block emptyBlock = new ByteArrayBlock(0, new boolean[0], new byte[0]);
        Block keyBlock = new ByteArrayBlock(16, new boolean[16], createExpectedValue(16).getBytes());
        Block valueBlock = new ByteArrayBlock(16, new boolean[16], createExpectedValue(16).getBytes());
        Block largerKeyBlock = new ByteArrayBlock(20, new boolean[20], createExpectedValue(20).getBytes());
        Block largerValueBlock = new ByteArrayBlock(20, new boolean[20], createExpectedValue(20).getBytes());
        int[] offsets = {0, 1, 1, 2, 4, 8, 16};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        assertCompact(mapType(TINYINT, TINYINT).createBlockFromKeyValue(new boolean[0], new int[1], emptyBlock, emptyBlock));
        assertCompact(mapType(TINYINT, TINYINT).createBlockFromKeyValue(valueIsNull, offsets, keyBlock, valueBlock));
        assertNotCompact(mapType(TINYINT, TINYINT).createBlockFromKeyValue(valueIsNull, offsets, largerKeyBlock.getRegion(0, 16), largerValueBlock.getRegion(0, 16)));

        // Test getRegion and copyRegion
        Block block = mapType(TINYINT, TINYINT).createBlockFromKeyValue(valueIsNull, offsets, keyBlock, valueBlock);
        assertGetRegionCompactness(block);
        assertCopyRegionCompactness(block);
        assertCopyRegionCompactness(mapType(TINYINT, TINYINT).createBlockFromKeyValue(valueIsNull, offsets, largerKeyBlock.getRegion(0, 16), largerValueBlock.getRegion(0, 16)));

        // Test BlockBuilder
        BlockBuilder emptyBlockBuilder = mapType(TINYINT, TINYINT).createBlockBuilder(new BlockBuilderStatus(), 0);
        assertNotCompact(emptyBlockBuilder);
        assertCompact(emptyBlockBuilder.build());

        Map<Byte, Byte>[] maps = new Map[17];
        for (int i = 0; i < 17; i++) {
            maps[i] = new HashMap<>();
            for (int j = 0; j < i; j++) {
                maps[i].put((byte) j, (byte) j);
            }
        }

        BlockBuilder nonFullBlockBuilder = createBlockBuilderWithValues(maps, false);
        assertNotCompact(nonFullBlockBuilder);
        assertNotCompact(nonFullBlockBuilder.build());
        assertCopyRegionCompactness(nonFullBlockBuilder);

        BlockBuilder fullBlockBuilder = createBlockBuilderWithValues(maps, true);
        assertNotCompact(fullBlockBuilder);
        assertCompact(fullBlockBuilder.build());
        assertCopyRegionCompactness(fullBlockBuilder);

        // NOTE: MapBlockBuilder will return itself if getRegion() is called to slice the whole block.
        // assertCompact(fullBlockBuilder.getRegion(0, fullBlockBuilder.getPositionCount()));
        assertNotCompact(fullBlockBuilder.getRegion(0, fullBlockBuilder.getPositionCount() - 1));
        assertNotCompact(fullBlockBuilder.getRegion(1, fullBlockBuilder.getPositionCount() - 1));
    }

    private Map<String, Long>[] createTestMap(int... entryCounts)
    {
        Map<String, Long>[] result = new Map[entryCounts.length];
        for (int rowNumber = 0; rowNumber < entryCounts.length; rowNumber++) {
            int entryCount = entryCounts[rowNumber];
            Map<String, Long> map = new HashMap<>();
            for (int entryNumber = 0; entryNumber < entryCount; entryNumber++) {
                map.put("key" + entryNumber, entryNumber == 5 ? null : rowNumber * 100L + entryNumber);
            }
            result[rowNumber] = map;
        }
        return result;
    }

    private void testWith(Map<String, Long>[] expectedValues)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);

        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder, 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, blockBuilder, 2, 3, 5, 6);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 2, 3, 5, 6);

        Block block = createBlockWithValuesFromKeyValueBlock(expectedValues);

        assertBlock(block, expectedValues);
        assertBlockFilteredPositions(expectedValues, block, 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, block, 2, 3, 5, 6);

        Map<String, Long>[] expectedValuesWithNull = (Map<String, Long>[]) alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);

        assertBlock(blockBuilderWithNull, expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull, 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull, 2, 3, 4, 9, 13, 14);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), 2, 3, 4, 9, 13, 14);

        Block blockWithNull = createBlockWithValuesFromKeyValueBlock(expectedValuesWithNull);

        assertBlock(blockWithNull, expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockWithNull, 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockWithNull, 2, 3, 4, 9, 13, 14);
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

    private Block createBlockWithValuesFromKeyValueBlock(Map<String, Long>[] maps)
    {
        List<String> keys = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        int[] offsets = new int[maps.length + 1];
        boolean[] mapIsNull = new boolean[maps.length];
        for (int i = 0; i < maps.length; i++) {
            Map<String, Long> map = maps[i];
            mapIsNull[i] = map == null;
            if (map == null) {
                offsets[i + 1] = offsets[i];
            }
            else {
                for (Map.Entry<String, Long> entry : map.entrySet()) {
                    keys.add(entry.getKey());
                    values.add(entry.getValue());
                }
                offsets[i + 1] = offsets[i] + map.size();
            }
        }
        return mapType(VARCHAR, BIGINT).createBlockFromKeyValue(mapIsNull, offsets, createStringsBlock(keys), createLongsBlock(values));
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
                if (entry.getValue() == null) {
                    elementBlockBuilder.appendNull();
                }
                else {
                    BIGINT.writeLong(elementBlockBuilder, entry.getValue());
                }
            }
            mapBlockBuilder.closeEntry();
        }
    }

    private static BlockBuilder createBlockBuilderWithValues(Map<Byte, Byte>[] maps, boolean useAccurateCapacityEstimation)
    {
        MethodHandle keyNativeEquals = TYPE_MANAGER.resolveOperator(OperatorType.EQUAL, ImmutableList.of(TINYINT, TINYINT));
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(TINYINT));
        MethodHandle keyNativeHashCode = TYPE_MANAGER.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(TINYINT));
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(TINYINT));

        int totalBytes = Arrays.stream(maps).mapToInt(Map::size).sum();
        BlockBuilderStatus blockBuilderStatus = new BlockBuilderStatus();
        BlockBuilder blockBuilder = new MapBlockBuilder(
                TINYINT,
                TINYINT.createBlockBuilder(blockBuilderStatus, useAccurateCapacityEstimation ? totalBytes : totalBytes * 2),
                TINYINT.createBlockBuilder(blockBuilderStatus, useAccurateCapacityEstimation ? totalBytes : totalBytes * 2),
                keyBlockNativeEquals,
                keyNativeHashCode,
                keyBlockHashCode,
                blockBuilderStatus,
                useAccurateCapacityEstimation ? maps.length : maps.length * 2);

        for (Map<Byte, Byte> map : maps) {
            if (map == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = blockBuilder.beginBlockEntry();
                for (Map.Entry<Byte, Byte> entry : map.entrySet()) {
                    TINYINT.writeLong(elementBlockBuilder, entry.getKey());
                    TINYINT.writeLong(elementBlockBuilder, entry.getValue());
                }
                blockBuilder.closeEntry();
            }
        }
        return blockBuilder;
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
        assertEquals(elementBlock.getPositionCount(), map.size() * 2);

        // Test new/hash-index access: assert inserted keys
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            int pos = elementBlock.seekKey(utf8Slice(entry.getKey()));
            assertNotEquals(pos, -1);
            if (entry.getValue() == null) {
                assertTrue(elementBlock.isNull(pos));
            }
            else {
                assertFalse(elementBlock.isNull(pos));
                assertEquals(BIGINT.getLong(elementBlock, pos), (long) entry.getValue());
            }
        }
        // Test new/hash-index access: assert non-existent keys
        for (int i = 0; i < 10; i++) {
            assertEquals(elementBlock.seekKey(utf8Slice("not-inserted-" + i)), -1);
        }

        // Test legacy/iterative access
        for (int i = 0; i < elementBlock.getPositionCount(); i += 2) {
            String actualKey = VARCHAR.getSlice(elementBlock, i).toStringUtf8();
            Long actualValue;
            if (elementBlock.isNull(i + 1)) {
                actualValue = null;
            }
            else {
                actualValue = BIGINT.getLong(elementBlock, i + 1);
            }
            assertTrue(map.containsKey(actualKey));
            assertEquals(actualValue, map.get(actualKey));
        }
    }
}
