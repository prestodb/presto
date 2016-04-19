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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.block.BlockAssertions.createEmptyLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestTypedSet
{
    @Test
    public void testConstructor()
            throws Exception
    {
        for (int i = -2; i <= -1; i++) {
            try {
                //noinspection ResultOfObjectAllocationIgnored
                new TypedSet(BIGINT, i);
                fail("Should throw exception if expectedSize < 0");
            }
            catch (IllegalArgumentException e) {
                // ignored
            }
        }

        try {
            //noinspection ResultOfObjectAllocationIgnored
            new TypedSet(null, 1);
            fail("Should throw exception if type is null");
        }
        catch (NullPointerException | IllegalArgumentException e) {
            // ignored
        }
    }

    @Test
    public void testGetElementPosition()
            throws Exception
    {
        int elementCount = 100;
        TypedSet typedSet = new TypedSet(BIGINT, elementCount);
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(elementCount);
        for (int i = 0; i < elementCount; i++) {
            BIGINT.writeLong(blockBuilder, i);
            typedSet.add(blockBuilder, i);
        }
        for (int j = 0; j < blockBuilder.getPositionCount(); j++) {
            assertEquals(typedSet.positionOf(blockBuilder, j), j);
        }
    }

    @Test
    public void testBigintSimpleTypedSet()
            throws Exception
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
                        createLongsBlock(Collections.nCopies(1, null)),
                        createLongsBlock(Collections.nCopies(100, null)),
                        createLongsBlock(Collections.nCopies(expectedSetSizes.get(expectedSetSizes.size() - 1) * 2, null)),
                        createLongsBlock(Collections.nCopies(expectedSetSizes.get(expectedSetSizes.size() - 1) * 2, 0L))
                );

        for (int expectedSetSize : expectedSetSizes) {
            for (Block block : longBlocks) {
                testBigint(block, expectedSetSize);
            }
        }
    }

    private static void testBigint(Block longBlock, int expectedSetSize)
    {
        TypedSet typedSet = new TypedSet(BIGINT, expectedSetSize);
        Set<Long> set = new HashSet<>();
        for (int blockPosition = 0; blockPosition < longBlock.getPositionCount(); blockPosition++) {
            long number = BIGINT.getLong(longBlock, blockPosition);
            assertEquals(typedSet.contains(longBlock, blockPosition), set.contains(number));
            assertEquals(typedSet.size(), set.size());

            set.add(number);
            typedSet.add(longBlock, blockPosition);

            assertEquals(typedSet.contains(longBlock, blockPosition), set.contains(number));
            assertEquals(typedSet.size(), set.size());
        }
    }
}
