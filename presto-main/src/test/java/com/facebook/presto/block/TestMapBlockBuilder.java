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
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.block.MapBlock;
import com.facebook.presto.common.block.MapBlockBuilder;
import com.facebook.presto.common.block.SingleMapBlock;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.MapType;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingEnvironment.getOperatorMethodHandle;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMapBlockBuilder
{
    private static final MethodHandle KEY_NATIVE_EQUALS = getOperatorMethodHandle(OperatorType.EQUAL, BIGINT, BIGINT);
    private static final MethodHandle KEY_BLOCK_EQUALS = compose(KEY_NATIVE_EQUALS, nativeValueGetter(BIGINT), nativeValueGetter(BIGINT));
    private static final MethodHandle KEY_NATIVE_HASH_CODE = getOperatorMethodHandle(OperatorType.HASH_CODE, BIGINT);
    private static final MethodHandle KEY_BLOCK_HASH_CODE = compose(KEY_NATIVE_HASH_CODE, nativeValueGetter(BIGINT));
    private static final MethodHandle KEY_BLOCK_NATIVE_EQUALS = compose(KEY_NATIVE_EQUALS, nativeValueGetter(BIGINT));
    private static final int MAP_POSITIONS = 100;

    @Test
    public void testMapBlockBuilderWithNullKeys()
    {
        MapBlockBuilder blockBuilder = createMapBlockBuilder();
        for (int i = 0; i < MAP_POSITIONS; i++) {
            if (i % 10 == 0) {
                blockBuilder.appendNull(); // Map is null
            }
            else {
                BlockBuilder entryBuilder = blockBuilder.beginBlockEntry(); // Map is valid.
                for (int j = 0; j < i; j++) {
                    if (j == 5) {
                        entryBuilder.appendNull(); // Null Keys
                    }
                    else {
                        BIGINT.writeLong(entryBuilder, j);
                    }
                    BIGINT.writeLong(entryBuilder, i);
                }
                blockBuilder.closeEntry();
            }
            assertFalse(blockBuilder.isHashTablesPresent());
        }

        // Verify the contents of the map.
        MapBlock mapBlock = (MapBlock) blockBuilder.build();
        assertFalse(mapBlock.isHashTablesPresent());
        ColumnarMap columnarMap = ColumnarMap.toColumnarMap(mapBlock);

        for (int i = 0; i < MAP_POSITIONS; i++) {
            assertEquals(columnarMap.isNull(i), i % 10 == 0);
            Block keysBlock = columnarMap.getKeysBlock();
            Block valuesBlock = columnarMap.getValuesBlock();
            if (!columnarMap.isNull(i)) {
                int offset = columnarMap.getOffset(i);
                for (int j = 0; j < i; j++) {
                    assertEquals(keysBlock.isNull(offset + j), j == 5);
                    if (!keysBlock.isNull(offset + j)) {
                        assertEquals(BIGINT.getLong(keysBlock, offset + j), j);
                    }
                    assertEquals(BIGINT.getLong(valuesBlock, offset + j), i);
                }
            }
        }
        // Verify block and newBlockBuilder does not have the hashTables present.
        assertFalse(mapBlock.isHashTablesPresent());

        MapBlockBuilder anotherBuilder = (MapBlockBuilder) blockBuilder.newBlockBuilderLike(null);
        assertFalse(anotherBuilder.isHashTablesPresent());
    }

    @Test
    public void testMapBlockSeek()
    {
        MapBlockBuilder blockBuilder = createMapBlockBuilder();
        for (int i = 0; i < MAP_POSITIONS; i++) {
            BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
            for (int j = 0; j < i; j++) {
                BIGINT.writeLong(entryBuilder, j); // key
                BIGINT.writeLong(entryBuilder, i); // value
            }
            blockBuilder.closeEntry();
        }
        assertFalse(blockBuilder.isHashTablesPresent());

        MapBlock mapBlock = (MapBlock) blockBuilder.build();
        assertFalse(mapBlock.isHashTablesPresent());

        for (int i = 0; i < MAP_POSITIONS; i++) {
            SingleMapBlock singleMapBlock = (SingleMapBlock) mapBlock.getBlock(i);
            for (int j = 0; j < i; j++) {
                assertEquals(singleMapBlock.seekKeyExact(j, KEY_NATIVE_HASH_CODE, KEY_BLOCK_NATIVE_EQUALS, KEY_BLOCK_HASH_CODE), j * 2 + 1);
            }
            assertEquals(mapBlock.isHashTablesPresent(), i > 0);
        }
        // Block copies the HashMap, so block builder's hash table is still not present.
        assertFalse(blockBuilder.isHashTablesPresent());
    }

    @Test
    public void testCloseEntryStrict()
            throws Exception
    {
        MapBlockBuilder mapBlockBuilder = createMapBlockBuilder();

        // Add MAP_POSITIONS maps with only one entry but the same key
        for (int i = 0; i < MAP_POSITIONS; i++) {
            appendSingleEntryMap(mapBlockBuilder, 1);
        }
        assertFalse(mapBlockBuilder.isHashTablesPresent());

        BlockBuilder entryBuilder = mapBlockBuilder.beginBlockEntry();
        // Add 50 keys so we get some chance to get hash conflict
        // The purpose of this test is to make sure offset is calculated correctly in MapBlockBuilder.closeEntryStrict()
        for (int i = 0; i < 50; i++) {
            BIGINT.writeLong(entryBuilder, i);
            BIGINT.writeLong(entryBuilder, -1);
        }
        mapBlockBuilder.closeEntryStrict(KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE);
        assertTrue(mapBlockBuilder.isHashTablesPresent());

        // Verify Keys
        for (int i = 0; i < MAP_POSITIONS; i++) {
            SingleMapBlock block = (SingleMapBlock) mapBlockBuilder.getBlock(i);
            assertEquals(block.seekKeyExact(1, KEY_NATIVE_HASH_CODE, KEY_BLOCK_NATIVE_EQUALS, KEY_BLOCK_HASH_CODE), 1);
        }

        SingleMapBlock singleMapBlock = (SingleMapBlock) mapBlockBuilder.getBlock(MAP_POSITIONS);
        for (int i = 0; i < 50; i++) {
            assertEquals(singleMapBlock.seekKeyExact(i, KEY_NATIVE_HASH_CODE, KEY_BLOCK_NATIVE_EQUALS, KEY_BLOCK_HASH_CODE), i * 2 + 1);
        }

        // Verify that Block also has the hash tables loaded.
        MapBlock mapBlock = (MapBlock) mapBlockBuilder.build();
        assertTrue(mapBlock.isHashTablesPresent());
    }

    @Test
    public void testMapBuilderSeekLoadsHashMap()
    {
        MapBlockBuilder mapBlockBuilder = createMapBlockBuilder();

        for (int i = 0; i < MAP_POSITIONS; i++) {
            appendSingleEntryMap(mapBlockBuilder, i);
        }
        assertFalse(mapBlockBuilder.isHashTablesPresent());

        // Verify Keys
        for (int i = 0; i < MAP_POSITIONS; i++) {
            SingleMapBlock block = (SingleMapBlock) mapBlockBuilder.getBlock(i);
            assertEquals(block.seekKeyExact(i, KEY_NATIVE_HASH_CODE, KEY_BLOCK_NATIVE_EQUALS, KEY_BLOCK_HASH_CODE), 1);
            assertTrue(mapBlockBuilder.isHashTablesPresent());
        }

        for (int i = 0; i < MAP_POSITIONS; i++) {
            //Add more entries and verify the keys.
            appendSingleEntryMap(mapBlockBuilder, i);
            verifyOnlyKeyInMap(mapBlockBuilder, MAP_POSITIONS + i, i);
        }

        // Verify that Block and Block Builder also has hash tables present when created.
        MapBlock mapBlock = (MapBlock) mapBlockBuilder.build();
        assertTrue(mapBlock.isHashTablesPresent());

        MapBlockBuilder anotherBuilder = (MapBlockBuilder) mapBlockBuilder.newBlockBuilderLike(null);
        assertTrue(anotherBuilder.isHashTablesPresent());
    }

    @Test
    public void testAppendStructureWithMissingHash()
    {
        MapBlockBuilder mapBlockBuilder = createMapBlockBuilder();

        for (int i = 0; i < MAP_POSITIONS; i++) {
            appendSingleEntryMap(mapBlockBuilder, i);
        }
        assertFalse(mapBlockBuilder.isHashTablesPresent());

        MapBlockBuilder anotherBuilder = createMapBlockBuilder();
        for (int i = 0; i < MAP_POSITIONS; i++) {
            SingleMapBlock block = (SingleMapBlock) mapBlockBuilder.getBlock(i);
            anotherBuilder.appendStructure(block);
        }
        assertFalse(anotherBuilder.isHashTablesPresent());

        for (int i = 0; i < MAP_POSITIONS; i++) {
            verifyOnlyKeyInMap(anotherBuilder, i, i);
            assertTrue(anotherBuilder.isHashTablesPresent());
        }
    }

    @Test
    public void testAppendStructureWithHashPresent()
    {
        MapBlockBuilder mapBlockBuilder = createMapBlockBuilder();

        for (int i = 0; i < MAP_POSITIONS; i++) {
            appendSingleEntryMap(mapBlockBuilder, i);
        }
        assertFalse(mapBlockBuilder.isHashTablesPresent());

        MapBlockBuilder anotherBuilder = (MapBlockBuilder) mapBlockBuilder.newBlockBuilderLike(null);
        MapBlock mapBlock = (MapBlock) mapBlockBuilder.build();
        for (int i = 0; i < MAP_POSITIONS; i++) {
            anotherBuilder.appendStructureInternal(mapBlock, i);
        }

        SingleMapBlock singleMapBlock = (SingleMapBlock) mapBlock.getBlock(1);
        singleMapBlock.seekKeyExact(1, KEY_NATIVE_HASH_CODE, KEY_BLOCK_NATIVE_EQUALS, KEY_BLOCK_HASH_CODE);

        assertFalse(anotherBuilder.isHashTablesPresent());
        for (int i = 0; i < MAP_POSITIONS; i++) {
            //Adding mapBlock with hash table present, forces the builder to build its hash table.
            anotherBuilder.appendStructureInternal(mapBlock, i);
            assertTrue(anotherBuilder.isHashTablesPresent());

            // Verify the keys for them automatically.
            verifyOnlyKeyInMap(anotherBuilder, MAP_POSITIONS + i, i);
        }

        for (int i = 0; i < MAP_POSITIONS; i++) {
            verifyOnlyKeyInMap(anotherBuilder, i, i);
        }
    }

    @Test
    public void testDirectBlockEntry()
    {
        MapType innerMapType = new MapType(
                BIGINT,
                BIGINT,
                KEY_BLOCK_EQUALS,
                KEY_BLOCK_HASH_CODE);

        MapType mapType = new MapType(
                BIGINT,
                innerMapType,
                KEY_BLOCK_EQUALS,
                KEY_BLOCK_HASH_CODE);

        MapBlockBuilder blockBuilder = (MapBlockBuilder) mapType.createBlockBuilder(null, MAP_POSITIONS);

        int numberOfOuterElements = 10;
        int numberOfInnerElements = 500;
        int outerKeyBase = 100;
        int keyBase = 1_000;
        int valueBase = 1_000_000;

        // The following code produces a Map{Long, Map{Long, Long}}.
        // The number of rows is MAP_POSITIONS(100)
        // Each row has 10 entries of map{long, map{long, long}}, (This is called outer map)
        // Each outer map's value has 500 entries of map {long, long} (This is called inner map).
        for (int element = 0; element < MAP_POSITIONS; element++) {
            blockBuilder.beginDirectEntry();

            BlockBuilder outerKeyBuilder = blockBuilder.getKeyBlockBuilder();
            for (int outer = 0; outer < numberOfOuterElements; outer++) {
                BIGINT.writeLong(outerKeyBuilder, element * outerKeyBase + outer);
            }

            MapBlockBuilder outerValueBuilder = (MapBlockBuilder) blockBuilder.getValueBlockBuilder();
            for (int outer = 0; outer < numberOfOuterElements; outer++) {
                outerValueBuilder.beginDirectEntry();
                BlockBuilder innerKeyBuilder = outerValueBuilder.getKeyBlockBuilder();
                for (int inner = 0; inner < numberOfInnerElements; inner++) {
                    BIGINT.writeLong(innerKeyBuilder, inner + outer * keyBase);
                }

                BlockBuilder innerValueBuilder = outerValueBuilder.getValueBlockBuilder();
                for (int inner = 0; inner < numberOfInnerElements; inner++) {
                    BIGINT.writeLong(innerValueBuilder, inner + outer * valueBase);
                }
                outerValueBuilder.closeEntry();
            }
            blockBuilder.closeEntry();
        }

        assertEquals(blockBuilder.getPositionCount(), MAP_POSITIONS);
        for (int element = 0; element < blockBuilder.getPositionCount(); element++) {
            SingleMapBlock outerBlock = (SingleMapBlock) blockBuilder.getBlock(element);
            assertEquals(outerBlock.getPositionCount(), numberOfOuterElements * 2);
            for (int outer = 0; outer < numberOfOuterElements; outer++) {
                assertEquals(outerBlock.getLong(outer * 2), (long) element * outerKeyBase + outer);
                SingleMapBlock innerValueBlock = (SingleMapBlock) outerBlock.getBlock(outer * 2 + 1);
                assertEquals(innerValueBlock.getPositionCount(), numberOfInnerElements * 2);

                for (int inner = 0; inner < numberOfInnerElements; inner++) {
                    assertEquals(innerValueBlock.getLong(inner * 2), (long) outer * keyBase + inner);
                    assertEquals(innerValueBlock.getLong(inner * 2 + 1), (long) outer * valueBase + inner);
                }
            }
        }
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testMismatchedKeyValuePositionCountThrows()
    {
        MapBlockBuilder mapBlockBuilder = createMapBlockBuilder();
        mapBlockBuilder.beginDirectEntry();
        // Write 2 keys.
        mapBlockBuilder.getKeyBlockBuilder().writeLong(1);
        mapBlockBuilder.getKeyBlockBuilder().writeLong(2);
        // Write 3 values.
        mapBlockBuilder.getValueBlockBuilder().writeLong(3);
        mapBlockBuilder.getValueBlockBuilder().writeLong(4);
        mapBlockBuilder.getValueBlockBuilder().writeLong(5);
        mapBlockBuilder.closeEntry();
    }

    private void appendSingleEntryMap(MapBlockBuilder mapBlockBuilder, int i)
    {
        BlockBuilder entryBuilder = mapBlockBuilder.beginBlockEntry();
        BIGINT.writeLong(entryBuilder, i);
        BIGINT.writeLong(entryBuilder, -1);
        mapBlockBuilder.closeEntry();
    }

    private void verifyOnlyKeyInMap(AbstractMapBlock block, int position, int key)
    {
        SingleMapBlock singleMapBlock = (SingleMapBlock) block.getBlock(position);
        assertEquals(singleMapBlock.seekKeyExact(key, KEY_NATIVE_HASH_CODE, KEY_BLOCK_NATIVE_EQUALS, KEY_BLOCK_HASH_CODE), 1);
        int nonExistentKey = key + 1; // Any int other than key will do.
        assertEquals(singleMapBlock.seekKeyExact(nonExistentKey, KEY_NATIVE_HASH_CODE, KEY_BLOCK_NATIVE_EQUALS, KEY_BLOCK_HASH_CODE), -1);
    }

    private MapBlockBuilder createMapBlockBuilder()
    {
        MapType mapType = new MapType(
                BIGINT,
                BIGINT,
                KEY_BLOCK_EQUALS,
                KEY_BLOCK_HASH_CODE);
        return (MapBlockBuilder) mapType.createBlockBuilder(null, MAP_POSITIONS);
    }
}
