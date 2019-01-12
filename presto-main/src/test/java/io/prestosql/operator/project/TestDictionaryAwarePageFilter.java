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
package io.prestosql.operator.project;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.connector.ConnectorSession;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.stream.IntStream;

import static io.prestosql.block.BlockAssertions.createLongSequenceBlock;
import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestDictionaryAwarePageFilter
{
    @Test
    public void testDelegateMethods()
    {
        DictionaryAwarePageFilter filter = new DictionaryAwarePageFilter(new TestDictionaryFilter(true));
        assertEquals(filter.isDeterministic(), true);
        assertEquals(filter.getInputChannels().getInputChannels(), ImmutableList.of(3));
    }

    @Test
    public void testSimpleBlock()
    {
        Block block = createLongSequenceBlock(0, 100);
        testFilter(block, LongArrayBlock.class);
    }

    @Test
    public void testRleBlock()
    {
        testRleBlock(true);
        testRleBlock(false);
    }

    private static void testRleBlock(boolean filterRange)
    {
        DictionaryAwarePageFilter filter = createDictionaryAwarePageFilter(filterRange, LongArrayBlock.class);
        RunLengthEncodedBlock match = new RunLengthEncodedBlock(createLongSequenceBlock(4, 5), 100);
        testFilter(filter, match, filterRange);
        RunLengthEncodedBlock noMatch = new RunLengthEncodedBlock(createLongSequenceBlock(0, 1), 100);
        testFilter(filter, noMatch, filterRange);
    }

    @Test
    public void testRleBlockWithFailure()
    {
        DictionaryAwarePageFilter filter = createDictionaryAwarePageFilter(true, LongArrayBlock.class);
        RunLengthEncodedBlock fail = new RunLengthEncodedBlock(createLongSequenceBlock(-10, -9), 100);
        assertThrows(NegativeValueException.class, () -> testFilter(filter, fail, true));
    }

    @Test
    public void testDictionaryBlock()
    {
        // match some
        testFilter(createDictionaryBlock(20, 100), LongArrayBlock.class);

        // match none
        testFilter(createDictionaryBlock(20, 0), LongArrayBlock.class);

        // match all
        testFilter(new DictionaryBlock(createLongSequenceBlock(4, 5), new int[100]), LongArrayBlock.class);
    }

    @Test
    public void testDictionaryBlockWithFailure()
    {
        assertThrows(NegativeValueException.class, () -> testFilter(createDictionaryBlockWithFailure(20, 100), LongArrayBlock.class));
    }

    @Test
    public void testDictionaryBlockProcessingWithUnusedFailure()
    {
        // match some
        testFilter(createDictionaryBlockWithUnusedEntries(20, 100), DictionaryBlock.class);

        // match none
        testFilter(createDictionaryBlockWithUnusedEntries(20, 0), DictionaryBlock.class);

        // match all
        testFilter(new DictionaryBlock(createLongsBlock(4, 5, -1), new int[100]), DictionaryBlock.class);
    }

    @Test
    public void testDictionaryProcessingEnableDisable()
    {
        TestDictionaryFilter nestedFilter = new TestDictionaryFilter(true);
        DictionaryAwarePageFilter filter = new DictionaryAwarePageFilter(nestedFilter);

        DictionaryBlock ineffectiveBlock = createDictionaryBlock(100, 20);
        DictionaryBlock effectiveBlock = createDictionaryBlock(10, 100);

        // function will always processes the first dictionary
        nestedFilter.setExpectedType(LongArrayBlock.class);
        testFilter(filter, ineffectiveBlock, true);

        // last dictionary not effective, so dictionary processing is disabled
        nestedFilter.setExpectedType(DictionaryBlock.class);
        testFilter(filter, effectiveBlock, true);

        // last dictionary not effective, so dictionary processing is enabled again
        nestedFilter.setExpectedType(LongArrayBlock.class);
        testFilter(filter, ineffectiveBlock, true);

        // last dictionary not effective, so dictionary processing is disabled again
        nestedFilter.setExpectedType(DictionaryBlock.class);
        testFilter(filter, effectiveBlock, true);
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

    private static void testFilter(Block block, Class<? extends Block> expectedType)
    {
        testFilter(block, true, expectedType);
        testFilter(block, false, expectedType);
        testFilter(lazyWrapper(block), true, expectedType);
        testFilter(lazyWrapper(block), false, expectedType);
    }

    private static void testFilter(Block block, boolean filterRange, Class<? extends Block> expectedType)
    {
        DictionaryAwarePageFilter filter = createDictionaryAwarePageFilter(filterRange, expectedType);
        testFilter(filter, block, filterRange);
        // exercise dictionary caching code
        testFilter(filter, block, filterRange);
    }

    private static DictionaryAwarePageFilter createDictionaryAwarePageFilter(boolean filterRange, Class<? extends Block> expectedType)
    {
        return new DictionaryAwarePageFilter(new TestDictionaryFilter(filterRange, expectedType));
    }

    private static void testFilter(DictionaryAwarePageFilter filter, Block block, boolean filterRange)
    {
        IntSet actualSelectedPositions = toSet(filter.filter(null, new Page(block)));

        block = block.getLoadedBlock();

        IntSet expectedSelectedPositions = new IntArraySet(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (isSelected(filterRange, block.getLong(position, 0))) {
                expectedSelectedPositions.add(position);
            }
        }
        assertEquals(actualSelectedPositions, expectedSelectedPositions);
    }

    private static IntSet toSet(SelectedPositions selectedPositions)
    {
        int start = selectedPositions.getOffset();
        int end = start + selectedPositions.size();
        if (selectedPositions.isList()) {
            return new IntArraySet(Arrays.copyOfRange(selectedPositions.getPositions(), start, end));
        }
        return new IntArraySet(IntStream.range(start, end).toArray());
    }

    private static LazyBlock lazyWrapper(Block block)
    {
        return new LazyBlock(block.getPositionCount(), lazyBlock -> lazyBlock.setBlock(block));
    }

    private static boolean isSelected(boolean filterRange, long value)
    {
        if (value < 0) {
            throw new IllegalArgumentException("value is negative: " + value);
        }

        boolean selected;
        if (filterRange) {
            selected = value > 3 && value < 11;
        }
        else {
            selected = value % 3 == 1;
        }
        return selected;
    }

    /**
     * Filter for the dictionary.  This will fail if the input block is a DictionaryBlock
     */
    private static class TestDictionaryFilter
            implements PageFilter
    {
        private final boolean filterRange;
        private Class<? extends Block> expectedType;

        public TestDictionaryFilter(boolean filterRange)
        {
            this.filterRange = filterRange;
        }

        public TestDictionaryFilter(boolean filterRange, Class<? extends Block> expectedType)
        {
            this.filterRange = filterRange;
            this.expectedType = expectedType;
        }

        public void setExpectedType(Class<? extends Block> expectedType)
        {
            this.expectedType = expectedType;
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
        public SelectedPositions filter(ConnectorSession session, Page page)
        {
            assertEquals(page.getChannelCount(), 1);
            Block block = page.getBlock(0);

            boolean sequential = true;
            IntArrayList selectedPositions = new IntArrayList();
            for (int position = 0; position < block.getPositionCount(); position++) {
                long value = block.getLong(position, 0);
                verifyPositive(value);

                boolean selected = isSelected(filterRange, value);
                if (selected) {
                    if (sequential && !selectedPositions.isEmpty()) {
                        sequential = (position == selectedPositions.getInt(selectedPositions.size() - 1) + 1);
                    }
                    selectedPositions.add(position);
                }
            }
            if (selectedPositions.isEmpty()) {
                return SelectedPositions.positionsRange(0, 0);
            }
            if (sequential) {
                return SelectedPositions.positionsRange(selectedPositions.getInt(0), selectedPositions.size());
            }
            // add 3 invalid elements to the head and tail
            for (int i = 0; i < 3; i++) {
                selectedPositions.add(0, -1);
                selectedPositions.add(-1);
            }

            // verify the input block is the expected type (this is to assure that
            // dictionary processing enabled and disabled as expected)
            // this check is performed last so that dictionary processing that fails
            // is not checked (only the fall back processing is checked)
            assertTrue(expectedType.isInstance(block));

            return SelectedPositions.positionsList(selectedPositions.elements(), 3, selectedPositions.size() - 6);
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
