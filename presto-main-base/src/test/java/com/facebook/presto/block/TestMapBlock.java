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

import com.facebook.presto.common.block.AbstractMapBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ByteArrayBlock;
import com.facebook.presto.common.block.MapBlock;
import com.facebook.presto.common.block.MapBlockBuilder;
import com.facebook.presto.common.block.SingleMapBlock;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.MapType;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.block.BlockAssertions.createLongDictionaryBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRleBlockWithRandomValue;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingEnvironment.getOperatorMethodHandle;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestMapBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        testWith(createTestMap(9, 3, 4, 0, 8, 0, 6, 5));
    }

    @Test
    public void testCompactBlock()
    {
        Block emptyBlock = new ByteArrayBlock(0, Optional.empty(), new byte[0]);
        Block compactKeyBlock = new ByteArrayBlock(16, Optional.empty(), createExpectedValue(16).getBytes());
        Block compactValueBlock = new ByteArrayBlock(16, Optional.empty(), createExpectedValue(16).getBytes());
        Block inCompactKeyBlock = new ByteArrayBlock(16, Optional.empty(), createExpectedValue(17).getBytes());
        Block inCompactValueBlock = new ByteArrayBlock(16, Optional.empty(), createExpectedValue(17).getBytes());
        int[] offsets = {0, 1, 1, 2, 4, 8, 16};
        boolean[] mapIsNull = {false, true, false, false, false, false};

        testCompactBlock(mapType(TINYINT, TINYINT).createBlockFromKeyValue(0, Optional.empty(), new int[1], emptyBlock, emptyBlock));
        testCompactBlock(mapType(TINYINT, TINYINT).createBlockFromKeyValue(mapIsNull.length, Optional.of(mapIsNull), offsets, compactKeyBlock, compactValueBlock));
        // TODO: Add test case for a sliced MapBlock

        // underlying key/value block is not compact
        testIncompactBlock(mapType(TINYINT, TINYINT).createBlockFromKeyValue(mapIsNull.length, Optional.of(mapIsNull), offsets, inCompactKeyBlock, inCompactValueBlock));
    }

    // TODO: remove this test when we have a more unified testWith() using assertBlock()
    @Test
    public void testLazyHashTableBuildOverBlockRegion()
    {
        assertLazyHashTableBuildOverBlockRegion(createTestMap(9, 3, 4, 0, 8, 0, 6, 5));
        assertLazyHashTableBuildOverBlockRegion(alternatingNullValues(createTestMap(9, 3, 4, 0, 8, 0, 6, 5)));
    }

    @Test
    public void testSingleValueBlock()
    {
        // 1 entry map.
        Map<String, Long>[] values = createTestMap(50);
        BlockBuilder mapBlockBuilder = createBlockBuilderWithValues(values);
        Block mapBlock = mapBlockBuilder.build();
        assertSame(mapBlock, mapBlock.getSingleValueBlock(0));
        assertNotSame(mapBlockBuilder, mapBlockBuilder.getSingleValueBlock(0));

        // 2 entries map.
        values = createTestMap(50, 50);
        mapBlockBuilder = createBlockBuilderWithValues(values);
        mapBlock = mapBlockBuilder.build();
        Block firstElement = mapBlock.getRegion(0, 1);
        assertNotSame(firstElement, firstElement.getSingleValueBlock(0));

        Block secondElementCopy = mapBlock.copyRegion(1, 1);
        assertSame(secondElementCopy, secondElementCopy.getSingleValueBlock(0));

        // Test with null elements.
        values = new Map[] {null};
        mapBlockBuilder = createBlockBuilderWithValues(values);
        mapBlock = mapBlockBuilder.build();
        assertSame(mapBlock, mapBlock.getSingleValueBlock(0));
        assertNotSame(mapBlock, mapBlockBuilder.getSingleValueBlock(0));

        // Test with 2 null elements.
        values = new Map[] {null, null};
        mapBlockBuilder = createBlockBuilderWithValues(values);
        mapBlock = mapBlockBuilder.build();
        assertNotSame(mapBlock, mapBlock.getSingleValueBlock(0));
    }

    @Test
    public void testSeekKey()
    {
        Block keyBlock = createStringsBlock("k");
        Block valueBlock = createStringsBlock("v");
        Block mapBlock = mapType(VARCHAR, VARCHAR).createBlockFromKeyValue(1, Optional.empty(), IntStream.range(0, 2).toArray(), keyBlock, valueBlock);
        SingleMapBlock singleMapBlock = (SingleMapBlock) mapBlock.getBlock(0);
        MethodHandle keyNativeHashCode = getOperatorMethodHandle(OperatorType.HASH_CODE, VARCHAR);
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(VARCHAR));
        MethodHandle keyNativeEquals = getOperatorMethodHandle(OperatorType.EQUAL, VARCHAR, VARCHAR);
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(VARCHAR));

        // Testing not found case. The key to seek should be longer than AbstractVariableWidthType#EXPECTED_BYTES_PER_ENTRY(32) and has same hash code as the key in keyBlock.
        // This is because the default capacity of the slice of the keyBlock for 1 single character key is EXPECTED_BYTES_PER_ENTRY. We want to make the seeked key out of boundary
        // to make sure no exception is thrown.
        assertEquals(singleMapBlock.seekKeyExact(Slices.utf8Slice(new String(new char[10]).replace("\0", "Doudou")), keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode), -1);
        // Testing found case.
        assertEquals(singleMapBlock.seekKeyExact(Slices.utf8Slice("k"), keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode), 1);
    }

    @Test
    public void testLogicalSizeInBytes()
    {
        int positionCount = 100;
        int[] offsets = IntStream.rangeClosed(0, positionCount).toArray();
        boolean[] nulls = new boolean[positionCount];

        // Map(LongArrayBlock, LongArrayBlock)
        Block keyBlock1 = createRandomLongsBlock(positionCount, 0);
        Block valueBlock1 = createRandomLongsBlock(positionCount, 0);
        Block mapOfLongAndLong = mapType(BIGINT, BIGINT).createBlockFromKeyValue(positionCount, Optional.of(nulls), offsets, keyBlock1, valueBlock1);
        assertEquals(mapOfLongAndLong.getLogicalSizeInBytes(), 3100);

        // Map(RLE(LongArrayBlock), RLE(LongArrayBlock))
        Block keyBlock2 = createRLEBlock(1, positionCount);
        Block valueBlock2 = createRLEBlock(2, positionCount);
        Block mapOfRleOfLongAndRleOfLong = mapType(BIGINT, BIGINT).createBlockFromKeyValue(positionCount, Optional.of(nulls), offsets, keyBlock2, valueBlock2);
        assertEquals(mapOfRleOfLongAndRleOfLong.getLogicalSizeInBytes(), 3100);

        // Map(RLE(LongArrayBlock), RLE(Map(LongArrayBlock, LongArrayBlock)))
        Block keyBlock3 = createRLEBlock(1, positionCount);
        Block valueBlock3 = createRleBlockWithRandomValue(mapType(BIGINT, BIGINT).createBlockFromKeyValue(positionCount, Optional.of(nulls), offsets, keyBlock1, valueBlock1), positionCount); //createRleBlockWithRandomValue(fromElementBlock(positionCount, Optional.of(nulls), arrayOffsets, createRandomLongsBlock(positionCount * 2, 0)), positionCount);
        Block mapOfRleOfLongAndRleOfMapOfLongAndLong = mapType(BIGINT, mapType(BIGINT, BIGINT)).createBlockFromKeyValue(positionCount, Optional.of(nulls), offsets, keyBlock3, valueBlock3);
        assertEquals(mapOfRleOfLongAndRleOfMapOfLongAndLong.getLogicalSizeInBytes(), 5300);

        // Map(RLE(LongArrayBlock), Map(LongArrayBlock, RLE(LongArrayBlock)))
        Block keyBlock4 = createRLEBlock(1, positionCount);
        Block valueBlock4 = mapType(BIGINT, BIGINT).createBlockFromKeyValue(positionCount, Optional.of(nulls), offsets, keyBlock1, createRLEBlock(1, positionCount));
        Block mapBlock4 = mapType(BIGINT, mapType(BIGINT, BIGINT)).createBlockFromKeyValue(positionCount, Optional.of(nulls), offsets, keyBlock4, valueBlock4);
        assertEquals(mapBlock4.getLogicalSizeInBytes(), 5300);

        // Map(Dictionary(LongArrayBlock), Dictionary(LongArrayBlock))
        Block keyBlock5 = createLongDictionaryBlock(0, positionCount);
        Block valuesBlock5 = createLongDictionaryBlock(0, positionCount);
        Block mapBlock5 = mapType(BIGINT, BIGINT).createBlockFromKeyValue(positionCount, Optional.of(nulls), offsets, keyBlock5, valuesBlock5);
        assertEquals(mapBlock5.getLogicalSizeInBytes(), 3100);

        // Map(Dictionary(LongArrayBlock), Map(LongArrayBlock, Dictionary(LongArrayBlock)))
        Block keyBlock6 = createLongDictionaryBlock(0, positionCount);
        Block valuesBlock6 = mapType(BIGINT, BIGINT).createBlockFromKeyValue(positionCount, Optional.of(nulls), offsets, keyBlock1, createLongDictionaryBlock(0, positionCount));
        Block mapBlock6 = mapType(BIGINT, BIGINT).createBlockFromKeyValue(positionCount, Optional.of(nulls), offsets, keyBlock6, valuesBlock6);
        assertEquals(mapBlock6.getLogicalSizeInBytes(), 5300);
    }

    private void assertLazyHashTableBuildOverBlockRegion(Map<String, Long>[] testValues)
    {
        // use prefix block to build the hash table
        {
            MapBlock block = createBlockWithValuesFromKeyValueBlock(testValues);
            BlockBuilder blockBuilder = createBlockBuilderWithValues(testValues);

            assertFalse(block.isHashTablesPresent());

            verifyBlockAndBuilderRegion(testValues, block, blockBuilder, 0, 4);
            assertTrue(block.isHashTablesPresent());

            verifyBlockAndBuilderRegion(testValues, block, blockBuilder, 2, 4);

            verifyBlockAndBuilderRegion(testValues, block, blockBuilder, 4, 4);
        }

        // use mid-section block to build the hash table
        {
            MapBlock block = createBlockWithValuesFromKeyValueBlock(testValues);
            BlockBuilder blockBuilder = createBlockBuilderWithValues(testValues);

            assertFalse(block.isHashTablesPresent());

            verifyBlockAndBuilderRegion(testValues, block, blockBuilder, 2, 4);
            assertTrue(block.isHashTablesPresent());

            verifyBlockAndBuilderRegion(testValues, block, blockBuilder, 0, 4);

            verifyBlockAndBuilderRegion(testValues, block, blockBuilder, 4, 4);
        }

        // use suffix block to build the hash table
        {
            MapBlock block = createBlockWithValuesFromKeyValueBlock(testValues);
            BlockBuilder blockBuilder = createBlockBuilderWithValues(testValues);

            assertFalse(block.isHashTablesPresent());

            verifyBlockAndBuilderRegion(testValues, block, blockBuilder, 4, 4);
            assertTrue(block.isHashTablesPresent());

            verifyBlockAndBuilderRegion(testValues, block, blockBuilder, 2, 4);

            verifyBlockAndBuilderRegion(testValues, block, blockBuilder, 0, 4);
        }
    }

    private void verifyBlockAndBuilderRegion(Map<String, Long>[] testValues, MapBlock block, BlockBuilder blockBuilder, int offset, int length)
    {
        verifyMapRegion(testValues, block, blockBuilder, offset, length);
        // MapBlockBuilder also implements AbstractMapBlock interface, verify it.
        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) blockBuilder;
        verifyMapRegion(testValues, mapBlockBuilder, blockBuilder, offset, length);
    }

    private void verifyMapRegion(Map<String, Long>[] testValues, AbstractMapBlock block, BlockBuilder blockBuilder, int offset, int length)
    {
        boolean isHashTablePresent = block.isHashTablesPresent();
        MapBlock region = (MapBlock) block.getRegion(offset, length);
        assertEquals(region.isHashTablesPresent(), isHashTablePresent);
        assertBlock(region, () -> blockBuilder.newBlockBuilderLike(null), Arrays.copyOfRange(testValues, offset, offset + length));
        assertTrue(region.isHashTablesPresent());
    }

    private static Map<String, Long>[] createTestMap(int... entryCounts)
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

        assertBlock(blockBuilder, () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlock(blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder, () -> blockBuilder.newBlockBuilderLike(null), 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, blockBuilder, () -> blockBuilder.newBlockBuilderLike(null), 2, 3, 5, 6);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), 2, 3, 5, 6);

        Block block = createBlockWithValuesFromKeyValueBlock(expectedValues);

        assertBlock(block, () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlockFilteredPositions(expectedValues, block, () -> blockBuilder.newBlockBuilderLike(null), 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, block, () -> blockBuilder.newBlockBuilderLike(null), 2, 3, 5, 6);

        Map<String, Long>[] expectedValuesWithNull = alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);

        assertBlock(blockBuilderWithNull, () -> blockBuilder.newBlockBuilderLike(null), expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull, () -> blockBuilder.newBlockBuilderLike(null), 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull, () -> blockBuilder.newBlockBuilderLike(null), 2, 3, 4, 9, 13, 14);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), 2, 3, 4, 9, 13, 14);

        Block blockWithNull = createBlockWithValuesFromKeyValueBlock(expectedValuesWithNull);

        assertBlock(blockWithNull, () -> blockBuilder.newBlockBuilderLike(null), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockWithNull, () -> blockBuilder.newBlockBuilderLike(null), 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockWithNull, () -> blockBuilder.newBlockBuilderLike(null), 2, 3, 4, 9, 13, 14);
    }

    private static BlockBuilder createBlockBuilderWithValues(Map<String, Long>[] maps)
    {
        MapType mapType = mapType(VARCHAR, BIGINT);
        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, 1);
        for (Map<String, Long> map : maps) {
            createBlockBuilderWithValues(map, mapBlockBuilder);
        }
        return mapBlockBuilder;
    }

    private static MapBlock createBlockWithValuesFromKeyValueBlock(Map<String, Long>[] maps)
    {
        List<String> keys = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        int positionCount = maps.length;
        int[] offsets = new int[positionCount + 1];
        boolean[] mapIsNull = new boolean[positionCount];
        for (int i = 0; i < positionCount; i++) {
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
        return (MapBlock) mapType(VARCHAR, BIGINT).createBlockFromKeyValue(positionCount, Optional.of(mapIsNull), offsets, createStringsBlock(keys), createLongsBlock(values));
    }

    private static void createBlockBuilderWithValues(Map<String, Long> map, BlockBuilder mapBlockBuilder)
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

    @Override
    protected <T> void assertCheckedPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue instanceof Map) {
            assertValue(block, position, (Map<String, Long>) expectedValue);
            return;
        }
        super.assertCheckedPositionValue(block, position, expectedValue);
    }

    @Override
    protected <T> void assertPositionValueUnchecked(Block block, int internalPosition, T expectedValue)
    {
        if (expectedValue instanceof Map) {
            assertValueUnchecked(block, internalPosition, (Map<String, Long>) expectedValue);
            return;
        }
        super.assertPositionValueUnchecked(block, internalPosition, expectedValue);
    }

    private static void assertValue(Block mapBlock, int position, Map<String, Long> map)
    {
        MapType mapType = mapType(VARCHAR, BIGINT);
        MethodHandle keyNativeHashCode = getOperatorMethodHandle(OperatorType.HASH_CODE, VARCHAR);
        MethodHandle keyNativeEquals = getOperatorMethodHandle(OperatorType.EQUAL, VARCHAR, VARCHAR);
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(VARCHAR));
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(VARCHAR));

        // null maps are handled by assertPositionValue
        requireNonNull(map, "map is null");

        assertFalse(mapBlock.isNull(position));
        SingleMapBlock elementBlock = (SingleMapBlock) mapType.getObject(mapBlock, position);
        assertEquals(elementBlock.getPositionCount(), map.size() * 2);

        // Test new/hash-index access: assert inserted keys
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            int pos = elementBlock.seekKey(utf8Slice(entry.getKey()), keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode);
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
            assertEquals(elementBlock.seekKey(utf8Slice("not-inserted-" + i), keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode), -1);
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

    private static void assertValueUnchecked(Block mapBlock, int internalPosition, Map<String, Long> map)
    {
        MapType mapType = mapType(VARCHAR, BIGINT);
        MethodHandle keyNativeHashCode = getOperatorMethodHandle(OperatorType.HASH_CODE, VARCHAR);
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(VARCHAR));
        MethodHandle keyNativeEquals = getOperatorMethodHandle(OperatorType.EQUAL, VARCHAR, VARCHAR);
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(VARCHAR));

        // null maps are handled by assertPositionValue
        requireNonNull(map, "map is null");

        assertFalse(mapBlock.isNullUnchecked((internalPosition)));
        SingleMapBlock elementBlock = (SingleMapBlock) mapType.getBlockUnchecked(mapBlock, (internalPosition));
        assertEquals(elementBlock.getPositionCount(), map.size() * 2);

        // Test new/hash-index access: assert inserted keys
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            int pos = elementBlock.seekKey(utf8Slice(entry.getKey()), keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode);
            assertNotEquals(pos, -1);
            if (entry.getValue() == null) {
                assertTrue(elementBlock.isNullUnchecked(pos + elementBlock.getOffsetBase()));
            }
            else {
                assertFalse(elementBlock.isNullUnchecked(pos + elementBlock.getOffsetBase()));
                assertEquals(BIGINT.getLongUnchecked(elementBlock, pos + elementBlock.getOffsetBase()), (long) entry.getValue());
            }
        }
        // Test new/hash-index access: assert non-existent keys
        for (int i = 0; i < 10; i++) {
            assertEquals(elementBlock.seekKey(utf8Slice("not-inserted-" + i), keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode), -1);
        }

        // Test legacy/iterative access
        for (int i = 0; i < elementBlock.getPositionCount(); i += 2) {
            String actualKey = VARCHAR.getSliceUnchecked(elementBlock, i + elementBlock.getOffset()).toStringUtf8();
            Long actualValue;
            if (elementBlock.isNullUnchecked(i + 1 + elementBlock.getOffset())) {
                actualValue = null;
            }
            else {
                actualValue = BIGINT.getLongUnchecked(elementBlock, i + 1 + elementBlock.getOffsetBase());
            }
            assertTrue(map.containsKey(actualKey));
            assertEquals(actualValue, map.get(actualKey));
        }
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        Map<String, Long>[] expectedValues = alternatingNullValues(createTestMap(9, 3, 4, 0, 8, 0, 6, 5));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        Block block = blockBuilder.build();
        assertEquals(block.getPositionCount(), expectedValues.length);
        for (int i = 0; i < block.getPositionCount(); i++) {
            int expectedSize = getExpectedEstimatedDataSize(expectedValues[i]);
            assertEquals(blockBuilder.getEstimatedDataSizeForStats(i), expectedSize);
            assertEquals(block.getEstimatedDataSizeForStats(i), expectedSize);
        }
    }

    private static int getExpectedEstimatedDataSize(Map<String, Long> map)
    {
        if (map == null) {
            return 0;
        }
        int size = 0;
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            size += entry.getKey().length();
            size += entry.getValue() == null ? 0 : Long.BYTES;
        }
        return size;
    }
}
