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
package com.facebook.presto.operator.project;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.spi.block.DictionaryId.randomDictionaryId;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;

public class TestDictionaryAwarePageProjection
{
    @Test
    public void testDelegateMethods()
            throws Exception
    {
        DictionaryAwarePageProjection projection = createProjection();
        assertEquals(projection.isDeterministic(), true);
        assertEquals(projection.getInputChannels().getInputChannels(), ImmutableList.of(3));
        assertEquals(projection.getType(), BIGINT);
    }

    @Test
    public void testSimpleBlock()
            throws Exception
    {
        Block block = createLongSequenceBlock(0, 100);
        testProject(block, block.getClass());
    }

    @Test
    public void testRleBlock()
            throws Exception
    {
        Block value = createLongSequenceBlock(42, 43);
        RunLengthEncodedBlock block = new RunLengthEncodedBlock(value, 100);

        testProject(block, RunLengthEncodedBlock.class);
    }

    @Test
    public void testDictionaryBlock()
            throws Exception
    {
        DictionaryBlock block = createDictionaryBlock(10, 100);

        testProject(block, DictionaryBlock.class);
    }

    @Test
    public void testDictionaryBlockProcessingWithUnusedFailure()
            throws Exception
    {
        DictionaryBlock block = createDictionaryBlockWithUnusedEntries(10, 100);

        // failures in the dictionary processing will cause a fallback to normal columnar processing
        testProject(block, LongArrayBlock.class);
    }

    @Test
    public void testDictionaryProcessingEnableDisable()
            throws Exception
    {
        DictionaryAwarePageProjection projection = createProjection();

        // function will always processes the first dictionary
        DictionaryBlock ineffectiveBlock = createDictionaryBlock(100, 20);
        testProjectRange(ineffectiveBlock, DictionaryBlock.class, projection);
        testProjectList(ineffectiveBlock, DictionaryBlock.class, projection);

        // last dictionary not effective, so dictionary processing is disabled
        DictionaryBlock effectiveBlock = createDictionaryBlock(10, 100);
        testProjectRange(effectiveBlock, LongArrayBlock.class, projection);
        testProjectList(effectiveBlock, LongArrayBlock.class, projection);

        // last dictionary not effective, so dictionary processing is enabled again
        testProjectRange(ineffectiveBlock, DictionaryBlock.class, projection);
        testProjectList(ineffectiveBlock, DictionaryBlock.class, projection);

        // last dictionary not effective, so dictionary processing is disabled again
        testProjectRange(effectiveBlock, LongArrayBlock.class, projection);
        testProjectList(effectiveBlock, LongArrayBlock.class, projection);
    }

    private static DictionaryBlock createDictionaryBlock(int dictionarySize, int blockSize)
    {
        Block dictionary = createLongSequenceBlock(0, dictionarySize);
        int[] ids = new int[blockSize];
        Arrays.setAll(ids, index -> index % dictionarySize);
        return new DictionaryBlock(dictionary, ids);
    }

    private static DictionaryBlock createDictionaryBlockWithUnusedEntries(int dictionarySize, int blockSize)
    {
        Block dictionary = createLongSequenceBlock(-10, dictionarySize);
        int[] ids = new int[blockSize];
        Arrays.setAll(ids, index -> (index % dictionarySize) + 10);
        return new DictionaryBlock(dictionary, ids);
    }

    private static void testProject(Block block, Class<? extends Block> expectedResultType)
    {
        testProjectRange(block, expectedResultType, createProjection());
        testProjectList(block, expectedResultType, createProjection());
        testProjectRange(lazyWrapper(block), expectedResultType, createProjection());
        testProjectList(lazyWrapper(block), expectedResultType, createProjection());
    }

    private static void testProjectRange(Block block, Class<? extends Block> expectedResultType, DictionaryAwarePageProjection projection)
    {
        Block result = projection.project(null, new Page(block), SelectedPositions.positionsRange(5, 10));
        assertBlockEquals(
                BIGINT,
                result,
                block.getRegion(5, 10));
        assertInstanceOf(result, expectedResultType);
    }

    private static void testProjectList(Block block, Class<? extends Block> expectedResultType, DictionaryAwarePageProjection projection)
    {
        int[] positions = {0, 2, 4, 6, 8, 10};
        Block result = projection.project(null, new Page(block), SelectedPositions.positionsList(positions, 0, positions.length));
        assertBlockEquals(
                BIGINT,
                result,
                block.copyPositions(new IntArrayList(positions)));
        assertInstanceOf(result, expectedResultType);
    }

    private static DictionaryAwarePageProjection createProjection()
    {
        return new DictionaryAwarePageProjection(
                new TestPageProjection(),
                block -> randomDictionaryId());
    }

    private static LazyBlock lazyWrapper(Block block)
    {
        return new LazyBlock(block.getPositionCount(), lazyBlock -> lazyBlock.setBlock(block));
    }

    private static class TestPageProjection
            implements PageProjection
    {
        @Override
        public Type getType()
        {
            return BIGINT;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(3);
        }

        @Override
        public Block project(ConnectorSession session, Page page, SelectedPositions selectedPositions)
        {
            Block block = page.getBlock(0);
            BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), selectedPositions.size());
            if (selectedPositions.isList()) {
                int offset = selectedPositions.getOffset();
                int[] positions = selectedPositions.getPositions();
                for (int index = offset; index < offset + selectedPositions.size(); index++) {
                    blockBuilder.writeLong(verifyPositive(block.getLong(positions[index], 0)));
                }
            }
            else {
                int offset = selectedPositions.getOffset();
                for (int position = offset; position < offset + selectedPositions.size(); position++) {
                    blockBuilder.writeLong(verifyPositive(block.getLong(position, 0)));
                }
            }
            return blockBuilder.build();
        }

        private static long verifyPositive(long value)
        {
            if (value < 0) {
                throw new IllegalArgumentException("value is negative: " + value);
            }
            return value;
        }
    }
}
