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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.block.BlockAssertions.createEmptyLongsBlock;
import static io.prestosql.block.BlockAssertions.createLongSequenceBlock;
import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static io.prestosql.spi.StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTypedSet
{
    private static final String FUNCTION_NAME = "typed_set_test";

    @Test
    public void testConstructor()
    {
        for (int i = -2; i <= -1; i++) {
            try {
                //noinspection ResultOfObjectAllocationIgnored
                new TypedSet(BIGINT, i, FUNCTION_NAME);
                fail("Should throw exception if expectedSize < 0");
            }
            catch (IllegalArgumentException e) {
                // ignored
            }
        }

        try {
            //noinspection ResultOfObjectAllocationIgnored
            new TypedSet(null, 1, FUNCTION_NAME);
            fail("Should throw exception if type is null");
        }
        catch (NullPointerException | IllegalArgumentException e) {
            // ignored
        }
    }

    @Test
    public void testGetElementPosition()
    {
        int elementCount = 100;
        // Set initialTypedSetEntryCount to a small number to trigger rehash()
        int initialTypedSetEntryCount = 10;
        TypedSet typedSet = new TypedSet(BIGINT, initialTypedSetEntryCount, FUNCTION_NAME);
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(elementCount);
        for (int i = 0; i < elementCount; i++) {
            BIGINT.writeLong(blockBuilder, i);
            typedSet.add(blockBuilder, i);
        }

        assertEquals(typedSet.size(), elementCount);

        for (int j = 0; j < blockBuilder.getPositionCount(); j++) {
            assertEquals(typedSet.positionOf(blockBuilder, j), j);
        }
    }

    @Test
    public void testGetElementPositionWithNull()
    {
        int elementCount = 100;
        // Set initialTypedSetEntryCount to a small number to trigger rehash()
        int initialTypedSetEntryCount = 10;
        TypedSet typedSet = new TypedSet(BIGINT, initialTypedSetEntryCount, FUNCTION_NAME);
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(elementCount);
        for (int i = 0; i < elementCount; i++) {
            if (i % 10 == 0) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, i);
            }
            typedSet.add(blockBuilder, i);
        }

        // The internal elementBlock and hashtable of the typedSet should contain
        // all distinct non-null elements plus one null
        assertEquals(typedSet.size(), elementCount - elementCount / 10 + 1);

        int nullCount = 0;
        for (int j = 0; j < blockBuilder.getPositionCount(); j++) {
            // The null is only added to typedSet once, so the internal elementBlock subscript is shifted by nullCountMinusOne
            if (!blockBuilder.isNull(j)) {
                assertEquals(typedSet.positionOf(blockBuilder, j), j - nullCount + 1);
            }
            else {
                // The first null added to typedSet is at position 0
                assertEquals(typedSet.positionOf(blockBuilder, j), 0);
                nullCount++;
            }
        }
    }

    @Test
    public void testGetElementPositionWithProvidedEmptyBlockBuilder()
    {
        int elementCount = 100;
        // Set initialTypedSetEntryCount to a small number to trigger rehash()
        int initialTypedSetEntryCount = 10;

        BlockBuilder emptyBlockBuilder = BIGINT.createFixedSizeBlockBuilder(elementCount);
        TypedSet typedSet = new TypedSet(BIGINT, emptyBlockBuilder, initialTypedSetEntryCount, FUNCTION_NAME);
        BlockBuilder externalBlockBuilder = BIGINT.createFixedSizeBlockBuilder(elementCount);
        for (int i = 0; i < elementCount; i++) {
            if (i % 10 == 0) {
                externalBlockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(externalBlockBuilder, i);
            }
            typedSet.add(externalBlockBuilder, i);
        }

        assertEquals(typedSet.size(), emptyBlockBuilder.getPositionCount());
        assertEquals(typedSet.size(), elementCount - elementCount / 10 + 1);

        for (int j = 0; j < typedSet.size(); j++) {
            assertEquals(typedSet.positionOf(emptyBlockBuilder, j), j);
        }
    }

    @Test
    public void testGetElementPositionWithProvidedNonEmptyBlockBuilder()
    {
        int elementCount = 100;
        // Set initialTypedSetEntryCount to a small number to trigger rehash()
        int initialTypedSetEntryCount = 10;

        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT));
        BlockBuilder firstBlockBuilder = pageBuilder.getBlockBuilder(0);

        for (int i = 0; i < elementCount; i++) {
            BIGINT.writeLong(firstBlockBuilder, i);
        }
        pageBuilder.declarePositions(elementCount);

        // The secondBlockBuilder should already have elementCount rows.
        BlockBuilder secondBlockBuilder = pageBuilder.getBlockBuilder(0);

        TypedSet typedSet = new TypedSet(BIGINT, secondBlockBuilder, initialTypedSetEntryCount, FUNCTION_NAME);
        BlockBuilder externalBlockBuilder = BIGINT.createFixedSizeBlockBuilder(elementCount);
        for (int i = 0; i < elementCount; i++) {
            if (i % 10 == 0) {
                externalBlockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(externalBlockBuilder, i);
            }
            typedSet.add(externalBlockBuilder, i);
        }

        assertEquals(typedSet.size(), secondBlockBuilder.getPositionCount() - elementCount);
        assertEquals(typedSet.size(), elementCount - elementCount / 10 + 1);

        for (int i = 0; i < typedSet.size(); i++) {
            int expectedPositionInSecondBlockBuilder = i + elementCount;
            assertEquals(typedSet.positionOf(secondBlockBuilder, expectedPositionInSecondBlockBuilder), expectedPositionInSecondBlockBuilder);
        }
    }

    @Test
    public void testGetElementPositionRandom()
    {
        TypedSet set = new TypedSet(VARCHAR, 1, FUNCTION_NAME);
        testGetElementPositionRandomFor(set);

        BlockBuilder emptyBlockBuilder = VARCHAR.createBlockBuilder(null, 3);
        TypedSet setWithPassedInBuilder = new TypedSet(VARCHAR, emptyBlockBuilder, 1, FUNCTION_NAME);
        testGetElementPositionRandomFor(setWithPassedInBuilder);
    }

    @Test
    public void testBigintSimpleTypedSet()
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
                testBigint(block, expectedSetSize);
            }
        }
    }

    @Test
    public void testMemoryExceeded()
    {
        try {
            TypedSet typedSet = new TypedSet(BIGINT, 10, FUNCTION_NAME);
            for (int i = 0; i <= TypedSet.FOUR_MEGABYTES + 1; i++) {
                Block block = createLongsBlock(nCopies(1, (long) i));
                typedSet.add(block, 0);
            }
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), EXCEEDED_FUNCTION_MEMORY_LIMIT.toErrorCode());
        }
    }

    private void testGetElementPositionRandomFor(TypedSet set)
    {
        BlockBuilder keys = VARCHAR.createBlockBuilder(null, 5);
        VARCHAR.writeSlice(keys, utf8Slice("hello"));
        VARCHAR.writeSlice(keys, utf8Slice("bye"));
        VARCHAR.writeSlice(keys, utf8Slice("abc"));

        for (int i = 0; i < keys.getPositionCount(); i++) {
            set.add(keys, i);
        }

        BlockBuilder values = VARCHAR.createBlockBuilder(null, 5);
        VARCHAR.writeSlice(values, utf8Slice("bye"));
        VARCHAR.writeSlice(values, utf8Slice("abc"));
        VARCHAR.writeSlice(values, utf8Slice("hello"));
        VARCHAR.writeSlice(values, utf8Slice("bad"));
        values.appendNull();

        assertEquals(set.positionOf(values, 4), -1);
        assertEquals(set.positionOf(values, 2), 0);
        assertEquals(set.positionOf(values, 1), 2);
        assertEquals(set.positionOf(values, 0), 1);
        assertFalse(set.contains(values, 3));

        set.add(values, 4);
        assertTrue(set.contains(values, 4));
    }

    private static void testBigint(Block longBlock, int expectedSetSize)
    {
        TypedSet typedSet = new TypedSet(BIGINT, expectedSetSize, FUNCTION_NAME);
        testBigintFor(typedSet, longBlock);

        BlockBuilder emptyBlockBuilder = BIGINT.createBlockBuilder(null, expectedSetSize);
        TypedSet typedSetWithPassedInBuilder = new TypedSet(BIGINT, emptyBlockBuilder, expectedSetSize, FUNCTION_NAME);
        testBigintFor(typedSetWithPassedInBuilder, longBlock);
    }

    private static void testBigintFor(TypedSet typedSet, Block longBlock)
    {
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
