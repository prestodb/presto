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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.block.BlockAssertions.createEmptyLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestHashTable
{
    private static final String FUNCTION_NAME = "typed_set_test";

    @Test
    public void testConstructor()
    {
        try {
            //noinspection ResultOfObjectAllocationIgnored
            BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(1);
            new HashTable(null, blockBuilder, 1);
            fail("Should throw exception if type is null");
        }
        catch (NullPointerException | IllegalArgumentException e) {
            // ignored
        }

        try {
            //noinspection ResultOfObjectAllocationIgnored
            new HashTable(BIGINT, null, 1);
            fail("Should throw exception if block is null");
        }
        catch (NullPointerException | IllegalArgumentException e) {
            // ignored
        }

        BlockBuilder keys = VARCHAR.createBlockBuilder(null, 5);

        for (int i = -2; i <= -1; i++) {
            try {
                //noinspection ResultOfObjectAllocationIgnored
                BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(1);
                new HashTable(BIGINT, blockBuilder, i);
                fail("Should throw exception if expectedSize < 0");
            }
            catch (IllegalArgumentException e) {
                // ignored
            }
        }
    }

    @Test
    public void testBigintSimpleHashTable()
    {
        List<Integer> expectedSetSizes = ImmutableList.of(1, 10, 100, 1000);
        List<Block> longBlocks =
                ImmutableList.of(
                        createEmptyLongsBlock(),
                        createLongsBlock(1L),
                        createLongsBlock(1L, 2L, 3L),
                        createLongsBlock(1L, 2L, 3L, 1L, 2L, 3L),
                        createLongsBlock(1L, null, 3L),
                        createLongsBlock(null, null, null),
                        createLongSequenceBlock(0, 100),
                        createLongSequenceBlock(-100, 100),
                        createLongsBlock(nCopies(1, null)),
                        createLongsBlock(nCopies(100, null)),
                        createLongsBlock(nCopies(expectedSetSizes.get(expectedSetSizes.size() - 1) * 2, null)),
                        createLongsBlock(nCopies(expectedSetSizes.get(expectedSetSizes.size() - 1) * 2, 0L)));

        for (int expectedSetSize : expectedSetSizes) {
            for (Block block : longBlocks) {
                testBigintBlock(block, expectedSetSize);
                testBigintBlockBuilder(block, expectedSetSize);
            }
        }
    }

    private static void testBigintBlock(Block longBlock, int expectedSetSize)
    {
        HashTable hashTable = new HashTable(BIGINT, longBlock, longBlock.getPositionCount());
        Set<Long> set = new HashSet<>();
        for (int blockPosition = 0; blockPosition < longBlock.getPositionCount(); blockPosition++) {
            long number = BIGINT.getLong(longBlock, blockPosition);
            assertEquals(hashTable.contains(longBlock, blockPosition), set.contains(number));
            assertEquals(hashTable.size(), set.size());

            set.add(number);
            hashTable.addIfAbsent(longBlock, blockPosition);

            assertEquals(hashTable.contains(longBlock, blockPosition), set.contains(number));
            assertEquals(hashTable.size(), set.size());
        }
    }

    private static void testBigintBlockBuilder(Block longBlock, int expectedSetSize)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, longBlock.getPositionCount());
        HashTable hashTable = new HashTable(BIGINT, blockBuilder, longBlock.getPositionCount());
        Set<Long> set = new HashSet<>();
        for (int blockPosition = 0; blockPosition < longBlock.getPositionCount(); blockPosition++) {
            long number = BIGINT.getLong(longBlock, blockPosition);
            assertEquals(hashTable.contains(longBlock, blockPosition), set.contains(number));
            assertEquals(hashTable.size(), set.size());

            set.add(number);
            hashTable.addIfAbsent(longBlock, blockPosition);

            assertEquals(hashTable.contains(longBlock, blockPosition), set.contains(number));
            assertEquals(hashTable.size(), set.size());
        }
    }
}
