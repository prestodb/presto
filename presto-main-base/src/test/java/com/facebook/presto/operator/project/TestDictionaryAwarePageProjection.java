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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.LazyBlock;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.common.block.DictionaryId.randomDictionaryId;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDictionaryAwarePageProjection
{
    private static final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("test-%s"));

    @DataProvider(name = "forceYield")
    public static Object[][] forceYield()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test
    public void testDelegateMethods()
    {
        DictionaryAwarePageProjection projection = createProjection();
        assertEquals(projection.isDeterministic(), true);
        assertEquals(projection.getInputChannels().getInputChannels(), ImmutableList.of(3));
    }

    @Test(dataProvider = "forceYield")
    public void testSimpleBlock(boolean forceYield)
    {
        Block block = createLongSequenceBlock(0, 100);
        testProject(block, block.getClass(), forceYield);
    }

    @Test(dataProvider = "forceYield")
    public void testRleBlock(boolean forceYield)
    {
        Block value = createLongSequenceBlock(42, 43);
        RunLengthEncodedBlock block = new RunLengthEncodedBlock(value, 100);

        testProject(block, RunLengthEncodedBlock.class, forceYield);
    }

    @Test(dataProvider = "forceYield")
    public void testRleBlockWithFailure(boolean forceYield)
    {
        Block value = createLongSequenceBlock(-43, -42);
        RunLengthEncodedBlock block = new RunLengthEncodedBlock(value, 100);

        testProjectFails(block, RunLengthEncodedBlock.class, forceYield);
    }

    @Test(dataProvider = "forceYield")
    public void testDictionaryBlock(boolean forceYield)
    {
        DictionaryBlock block = createDictionaryBlock(10, 100);

        testProject(block, DictionaryBlock.class, forceYield);
    }

    @Test(dataProvider = "forceYield")
    public void testDictionaryBlockWithFailure(boolean forceYield)
    {
        DictionaryBlock block = createDictionaryBlockWithFailure(10, 100);

        testProjectFails(block, DictionaryBlock.class, forceYield);
    }

    @Test(dataProvider = "forceYield")
    public void testDictionaryBlockProcessingWithUnusedFailure(boolean forceYield)
    {
        DictionaryBlock block = createDictionaryBlockWithUnusedEntries(10, 100);

        // failures in the dictionary processing will cause a fallback to normal columnar processing
        testProject(block, LongArrayBlock.class, forceYield);
    }

    @Test
    public void testDictionaryProcessingIgnoreYield()
    {
        DictionaryAwarePageProjection projection = createProjection();

        // the same input block will bypass yield with multiple projections
        DictionaryBlock block = createDictionaryBlock(10, 100);
        testProjectRange(block, DictionaryBlock.class, projection, true);
        testProjectFastReturnIgnoreYield(block, projection);
        testProjectFastReturnIgnoreYield(block, projection);
        testProjectFastReturnIgnoreYield(block, projection);
    }

    @Test(dataProvider = "forceYield")
    public void testDictionaryProcessingEnableDisable(boolean forceYield)
    {
        DictionaryAwarePageProjection projection = createProjection();

        // function will always processes the first dictionary
        DictionaryBlock ineffectiveBlock = createDictionaryBlock(100, 20);
        testProjectRange(ineffectiveBlock, DictionaryBlock.class, projection, forceYield);
        testProjectFastReturnIgnoreYield(ineffectiveBlock, projection);
        // dictionary processing can reuse the last dictionary
        // in this case, we don't even check yield signal; make yieldForce to false
        testProjectList(ineffectiveBlock, DictionaryBlock.class, projection, false);

        // last dictionary not effective, so dictionary processing is disabled
        DictionaryBlock effectiveBlock = createDictionaryBlock(10, 100);
        testProjectRange(effectiveBlock, LongArrayBlock.class, projection, forceYield);
        testProjectList(effectiveBlock, LongArrayBlock.class, projection, forceYield);

        // last dictionary effective, so dictionary processing is enabled again
        testProjectRange(ineffectiveBlock, DictionaryBlock.class, projection, forceYield);
        testProjectFastReturnIgnoreYield(ineffectiveBlock, projection);
        // dictionary processing can reuse the last dictionary
        // in this case, we don't even check yield signal; make yieldForce to false
        testProjectList(ineffectiveBlock, DictionaryBlock.class, projection, false);

        // last dictionary not effective, so dictionary processing is disabled again
        testProjectRange(effectiveBlock, LongArrayBlock.class, projection, forceYield);
        testProjectList(effectiveBlock, LongArrayBlock.class, projection, forceYield);
    }

    private static DictionaryBlock createDictionaryBlock(int dictionarySize, int blockSize)
    {
        Block dictionary = createLongSequenceBlock(0, dictionarySize);
        int[] ids = new int[blockSize];
        Arrays.setAll(ids, index -> index % dictionarySize);
        return new DictionaryBlock(dictionary, ids);
    }

    private static DictionaryBlock createDictionaryBlockWithFailure(int dictionarySize, int blockSize)
    {
        Block dictionary = createLongSequenceBlock(-10, dictionarySize - 10);
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

    private static List<Block> projectWithYield(Work<List<Block>> work, DriverYieldSignal yieldSignal)
    {
        int yieldCount = 0;
        while (true) {
            yieldSignal.setWithDelay(1, executor);
            yieldSignal.forceYieldForTesting();
            if (work.process()) {
                assertGreaterThan(yieldCount, 0);
                return work.getResult();
            }
            yieldCount++;
            if (yieldCount > 1_000_000) {
                fail("projection is not making progress");
            }
            yieldSignal.reset();
        }
    }

    private static void testProject(Block block, Class<? extends Block> expectedResultType, boolean forceYield)
    {
        testProjectRange(block, expectedResultType, createProjection(), forceYield);
        testProjectList(block, expectedResultType, createProjection(), forceYield);
        testProjectRange(lazyWrapper(block), expectedResultType, createProjection(), forceYield);
        testProjectList(lazyWrapper(block), expectedResultType, createProjection(), forceYield);
    }

    private static void testProjectFails(Block block, Class<? extends Block> expectedResultType, boolean forceYield)
    {
        assertThrows(NegativeValueException.class, () -> testProjectRange(block, expectedResultType, createProjection(), forceYield));
        assertThrows(NegativeValueException.class, () -> testProjectList(block, expectedResultType, createProjection(), forceYield));
        assertThrows(NegativeValueException.class, () -> testProjectRange(lazyWrapper(block), expectedResultType, createProjection(), forceYield));
        assertThrows(NegativeValueException.class, () -> testProjectList(lazyWrapper(block), expectedResultType, createProjection(), forceYield));
    }

    private static void testProjectRange(Block block, Class<? extends Block> expectedResultType, DictionaryAwarePageProjection projection, boolean forceYield)
    {
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        Work<List<Block>> work = projection.project(null, yieldSignal, new Page(block), SelectedPositions.positionsRange(5, 10));
        List<Block> result;
        if (forceYield) {
            result = projectWithYield(work, yieldSignal);
        }
        else {
            assertTrue(work.process());
            result = work.getResult();
        }
        assertBlockEquals(
                BIGINT,
                result.get(0),
                block.getRegion(5, 10));
        assertInstanceOf(result.get(0), expectedResultType);
    }

    private static void testProjectList(Block block, Class<? extends Block> expectedResultType, DictionaryAwarePageProjection projection, boolean forceYield)
    {
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        int[] positions = {0, 2, 4, 6, 8, 10};
        Work<List<Block>> work = projection.project(null, yieldSignal, new Page(block), SelectedPositions.positionsList(positions, 0, positions.length));
        List<Block> result;
        if (forceYield) {
            result = projectWithYield(work, yieldSignal);
        }
        else {
            assertTrue(work.process());
            result = work.getResult();
        }
        assertBlockEquals(
                BIGINT,
                result.get(0),
                block.copyPositions(positions, 0, positions.length));
        assertInstanceOf(result.get(0), expectedResultType);
    }

    private static void testProjectFastReturnIgnoreYield(Block block, DictionaryAwarePageProjection projection)
    {
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        Work<List<Block>> work = projection.project(null, yieldSignal, new Page(block), SelectedPositions.positionsRange(5, 10));
        yieldSignal.setWithDelay(1, executor);
        yieldSignal.forceYieldForTesting();

        // yield signal is ignored given the block has already been loaded
        assertTrue(work.process());
        Block result = work.getResult().get(0);
        yieldSignal.reset();

        assertBlockEquals(
                BIGINT,
                result,
                block.getRegion(5, 10));
        assertInstanceOf(result, DictionaryBlock.class);
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
        public Work<List<Block>> project(SqlFunctionProperties properties, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
        {
            return new TestPageProjectionWork(yieldSignal, page, selectedPositions);
        }

        private class TestPageProjectionWork
                implements Work<List<Block>>
        {
            private final DriverYieldSignal yieldSignal;
            private final Block block;
            private final SelectedPositions selectedPositions;

            private BlockBuilder blockBuilder;
            private int nextIndexOrPosition;
            private Block result;

            public TestPageProjectionWork(DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
            {
                this.yieldSignal = yieldSignal;
                this.block = page.getBlock(0);
                this.selectedPositions = selectedPositions;
                this.blockBuilder = BIGINT.createBlockBuilder(null, selectedPositions.size());
            }

            @Override
            public boolean process()
            {
                assertNull(result);
                if (selectedPositions.isList()) {
                    int offset = selectedPositions.getOffset();
                    int[] positions = selectedPositions.getPositions();
                    for (int index = nextIndexOrPosition + offset; index < offset + selectedPositions.size(); index++) {
                        blockBuilder.writeLong(verifyPositive(block.getLong(positions[index])));
                        if (yieldSignal.isSet()) {
                            nextIndexOrPosition = index + 1 - offset;
                            return false;
                        }
                    }
                }
                else {
                    int offset = selectedPositions.getOffset();
                    for (int position = nextIndexOrPosition + offset; position < offset + selectedPositions.size(); position++) {
                        blockBuilder.writeLong(verifyPositive(block.getLong(position)));
                        if (yieldSignal.isSet()) {
                            nextIndexOrPosition = position + 1 - offset;
                            return false;
                        }
                    }
                }
                result = blockBuilder.build();
                blockBuilder = blockBuilder.newBlockBuilderLike(null);
                return true;
            }

            @Override
            public List<Block> getResult()
            {
                assertNotNull(result);
                return ImmutableList.of(result);
            }
        }

        private static long verifyPositive(long value)
        {
            if (value < 0) {
                throw new NegativeValueException(value);
            }
            return value;
        }
    }

    private static class NegativeValueException
            extends RuntimeException
    {
        public NegativeValueException(long value)
        {
            super("value is negative: " + value);
        }
    }
}
