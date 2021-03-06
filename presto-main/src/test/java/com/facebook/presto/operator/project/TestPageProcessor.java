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

import com.facebook.airlift.testing.TestingTicker;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LazyBlock;
import com.facebook.presto.common.block.VariableWidthBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.CompletedWork;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.sql.gen.ExpressionProfiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createMapType;
import static com.facebook.presto.block.BlockAssertions.createSlicesBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.RowType.withDefaultFieldNames;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.executor.PrioritizedSplitRunner.SPLIT_RUN_QUANTA;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.PageAssertions.createPageWithRandomData;
import static com.facebook.presto.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static com.facebook.presto.operator.project.PageProcessor.MAX_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.operator.project.PageProcessor.MIN_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.operator.project.SelectedPositions.positionsList;
import static com.facebook.presto.operator.project.SelectedPositions.positionsRange;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.join;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestPageProcessor
{
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("test-%s"));

    @Test
    public void testProjectNoColumns()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), ImmutableList.of(), OptionalInt.of(MAX_BATCH_SIZE));

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        Page outputPage = outputPages.get(0).orElse(null);
        assertEquals(outputPage.getChannelCount(), 0);
        assertEquals(outputPage.getPositionCount(), inputPage.getPositionCount());
    }

    @Test
    public void testFilterNoColumns()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new TestingPageFilter(positionsRange(0, 50))), ImmutableList.of());

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION.getSqlFunctionProperties(), new DriverYieldSignal(), memoryContext, inputPage);
        assertEquals(memoryContext.getBytes(), 0);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        Page outputPage = outputPages.get(0).orElse(null);
        assertEquals(outputPage.getChannelCount(), 0);
        assertEquals(outputPage.getPositionCount(), 50);
    }

    @Test
    public void testPartialFilter()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.of(new TestingPageFilter(positionsRange(25, 50))),
                ImmutableList.of(createInputPageProjectionWithOutputs(0, BIGINT, 0)),
                OptionalInt.of(MAX_BATCH_SIZE));

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        assertPageEquals(ImmutableList.of(BIGINT), outputPages.get(0).orElse(null), new Page(createLongSequenceBlock(25, 75)));
    }

    @Test
    public void testPartialFilterAsList()
    {
        // Primitive types

        testPartialFilterAsList(BOOLEAN, 100, 0.5f, 0.5f, false, ImmutableList.of());
        testPartialFilterAsList(REAL, 100, 0.5f, 0.5f, false, ImmutableList.of());
        testPartialFilterAsList(BIGINT, 100, 0.5f, 0.5f, false, ImmutableList.of());
        testPartialFilterAsList(VARCHAR, 100, 0.5f, 0.5f, false, ImmutableList.of());

        testPartialFilterAsList(BOOLEAN, 100, 0.5f, 0.5f, true, ImmutableList.of());
        testPartialFilterAsList(REAL, 100, 0.5f, 0.5f, true, ImmutableList.of());
        testPartialFilterAsList(BIGINT, 100, 0.5f, 0.5f, true, ImmutableList.of());
        testPartialFilterAsList(VARCHAR, 100, 0.5f, 0.5f, true, ImmutableList.of());

        // Complext types

        testPartialFilterAsList(new ArrayType(BIGINT), 100, 0.5f, 0.5f, false, ImmutableList.of());
        testPartialFilterAsList(new ArrayType(VARCHAR), 100, 0.5f, 0.5f, false, ImmutableList.of());
        testPartialFilterAsList(createMapType(BIGINT, VARCHAR), 100, 0.5f, 0.5f, false, ImmutableList.of());
        testPartialFilterAsList(withDefaultFieldNames(ImmutableList.of(BIGINT, withDefaultFieldNames(ImmutableList.of(BOOLEAN, VARCHAR)))), 100, 0.5f, 0.5f, false, ImmutableList.of());

        testPartialFilterAsList(new ArrayType(BIGINT), 100, 0.5f, 0.5f, true, ImmutableList.of());
        testPartialFilterAsList(new ArrayType(VARCHAR), 100, 0.5f, 0.5f, true, ImmutableList.of());
        testPartialFilterAsList(createMapType(BIGINT, VARCHAR), 100, 0.5f, 0.5f, true, ImmutableList.of());
        testPartialFilterAsList(withDefaultFieldNames(ImmutableList.of(BIGINT, withDefaultFieldNames(ImmutableList.of(BOOLEAN, VARCHAR)))), 100, 0.5f, 0.5f, true, ImmutableList.of());
    }

    @Test
    public void testSelectAllFilter()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectAllFilter()), ImmutableList.of(createInputPageProjectionWithOutputs(0, BIGINT, 0)), OptionalInt.of(MAX_BATCH_SIZE));

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        assertPageEquals(ImmutableList.of(BIGINT), outputPages.get(0).orElse(null), new Page(createLongSequenceBlock(0, 100)));
    }

    @Test
    public void testSelectNoneFilter()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectNoneFilter()), ImmutableList.of(createInputPageProjectionWithOutputs(0, BIGINT, 0)));

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION.getSqlFunctionProperties(), new DriverYieldSignal(), memoryContext, inputPage);
        assertEquals(memoryContext.getBytes(), 0);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 0);
    }

    @Test
    public void testProjectEmptyPage()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectAllFilter()), ImmutableList.of(createInputPageProjectionWithOutputs(0, BIGINT, 0)));

        Page inputPage = new Page(createLongSequenceBlock(0, 0));

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION.getSqlFunctionProperties(), new DriverYieldSignal(), memoryContext, inputPage);
        assertEquals(memoryContext.getBytes(), 0);

        // output should be one page containing no columns (only a count)
        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 0);
    }

    @Test
    public void testSelectNoneFilterLazyLoad()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectNoneFilter()), ImmutableList.of(createInputPageProjectionWithOutputs(1, BIGINT, 0)));

        // if channel 1 is loaded, test will fail
        Page inputPage = new Page(createLongSequenceBlock(0, 100), new LazyBlock(100, lazyBlock -> {
            throw new AssertionError("Lazy block should not be loaded");
        }));

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION.getSqlFunctionProperties(), new DriverYieldSignal(), memoryContext, inputPage);
        assertEquals(memoryContext.getBytes(), 0);
        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 0);
    }

    @Test
    public void testProjectLazyLoad()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectAllFilter()), ImmutableList.of(new PageProjectionWithOutputs(new LazyPagePageProjection(), new int[] {
                0})), OptionalInt.of(MAX_BATCH_SIZE));

        // if channel 1 is loaded, test will fail
        Page inputPage = new Page(createLongSequenceBlock(0, 100), new LazyBlock(100, lazyBlock -> {
            throw new AssertionError("Lazy block should not be loaded");
        }));

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION.getSqlFunctionProperties(), new DriverYieldSignal(), memoryContext, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        assertPageEquals(ImmutableList.of(BIGINT), outputPages.get(0).orElse(null), new Page(createLongSequenceBlock(0, 100)));
    }

    @Test
    public void testBatchedOutput()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), ImmutableList.of(createInputPageProjectionWithOutputs(0, BIGINT, 0)), OptionalInt.of(MAX_BATCH_SIZE));

        Page inputPage = new Page(createLongSequenceBlock(0, (int) (MAX_BATCH_SIZE * 2.5)));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 3);
        for (int i = 0; i < outputPages.size(); i++) {
            Page actualPage = outputPages.get(i).orElse(null);
            int offset = i * MAX_BATCH_SIZE;
            Page expectedPage = new Page(createLongSequenceBlock(offset, offset + Math.min(inputPage.getPositionCount() - offset, MAX_BATCH_SIZE)));
            assertPageEquals(ImmutableList.of(BIGINT), actualPage, expectedPage);
        }
    }

    @Test
    public void testAdaptiveBatchSize()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), ImmutableList.of(createInputPageProjectionWithOutputs(0, VARCHAR, 0)), OptionalInt.of(MAX_BATCH_SIZE));

        // process large page which will reduce batch size
        Slice[] slices = new Slice[(int) (MAX_BATCH_SIZE * 2.5)];
        Arrays.fill(slices, Slices.allocate(1024));
        Page inputPage = new Page(createSlicesBlock(slices));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, new DriverYieldSignal(), inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        int batchSize = MAX_BATCH_SIZE;
        for (Optional<Page> actualPage : outputPages) {
            Page expectedPage = new Page(createSlicesBlock(Arrays.copyOfRange(slices, 0, batchSize)));
            assertPageEquals(ImmutableList.of(VARCHAR), actualPage.orElse(null), expectedPage);
            if (actualPage.orElseThrow(() -> new AssertionError("page is not present")).getSizeInBytes() > MAX_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize / 2;
            }
        }

        // process small page which will increase batch size
        Arrays.fill(slices, Slices.allocate(128));
        inputPage = new Page(createSlicesBlock(slices));

        output = processAndAssertRetainedPageSize(pageProcessor, new DriverYieldSignal(), inputPage);

        outputPages = ImmutableList.copyOf(output);
        int offset = 0;
        for (Optional<Page> actualPage : outputPages) {
            Page expectedPage = new Page(createSlicesBlock(Arrays.copyOfRange(slices, 0, Math.min(inputPage.getPositionCount() - offset, batchSize))));
            assertPageEquals(ImmutableList.of(VARCHAR), actualPage.orElse(null), expectedPage);
            offset += actualPage.orElseThrow(() -> new AssertionError("page is not present")).getPositionCount();
            if (actualPage.orElseThrow(() -> new AssertionError("page is not present")).getSizeInBytes() < MIN_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize * 2;
            }
        }
    }

    @Test
    public void testOptimisticProcessing()
    {
        InvocationCountPageProjection firstProjection = new InvocationCountPageProjection(new InputPageProjection(0));
        InvocationCountPageProjection secondProjection = new InvocationCountPageProjection(new InputPageProjection(0));
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), ImmutableList.of(new PageProjectionWithOutputs(firstProjection, new int[] {
                0}), new PageProjectionWithOutputs(secondProjection, new int[] {1})), OptionalInt.of(MAX_BATCH_SIZE));

        // process large page which will reduce batch size
        Slice[] slices = new Slice[(int) (MAX_BATCH_SIZE * 2.5)];
        Arrays.fill(slices, Slices.allocate(1024));
        Page inputPage = new Page(createSlicesBlock(slices));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        // batch size will be reduced before the first page is produced until the first block is within the page size bounds
        int batchSize = MAX_BATCH_SIZE;
        while (inputPage.getBlock(0).getRegionSizeInBytes(0, batchSize) > MAX_PAGE_SIZE_IN_BYTES) {
            batchSize /= 2;
        }

        int pageCount = 0;
        while (output.hasNext()) {
            Page actualPage = output.next().orElse(null);
            Block sliceBlock = createSlicesBlock(Arrays.copyOfRange(slices, 0, batchSize));
            Page expectedPage = new Page(sliceBlock, sliceBlock);
            assertPageEquals(ImmutableList.of(VARCHAR, VARCHAR), actualPage, expectedPage);
            pageCount++;

            // batch size will be further reduced to fit within the bounds
            if (actualPage.getSizeInBytes() > MAX_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize / 2;
            }
        }
        // second project is invoked once per output page
        assertEquals(secondProjection.getInvocationCount(), pageCount);

        // the page processor saves the results when the page size is exceeded, so the first projection
        // will be invoked less times
        assertTrue(firstProjection.getInvocationCount() < secondProjection.getInvocationCount());
    }

    @Test
    public void testRetainedSize()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.of(new SelectAllFilter()),
                ImmutableList.of(createInputPageProjectionWithOutputs(0, VARCHAR, 0), createInputPageProjectionWithOutputs(1, VARCHAR, 1)),
                OptionalInt.of(MAX_BATCH_SIZE));

        // create 2 columns X 800 rows of strings with each string's size = 10KB
        // this can force previouslyComputedResults to be saved given the page is 16MB in size
        String value = join("", nCopies(10_000, "a"));
        List<String> values = nCopies(800, value);
        Page inputPage = new Page(createStringsBlock(values), createStringsBlock(values));

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, new DriverYieldSignal(), memoryContext, inputPage);

        // force a compute
        // one block of previouslyComputedResults will be saved given the first column is with 8MB
        output.hasNext();

        // verify we do not count block sizes twice
        // comparing with the input page, the output page also contains an extra instance size for previouslyComputedResults
        assertEquals(memoryContext.getBytes() - ClassLayout.parseClass(VariableWidthBlock.class).instanceSize(), inputPage.getRetainedSizeInBytes());
    }

    @Test
    public void testYieldProjection()
    {
        // each projection can finish without yield
        // while between two projections, there is a yield
        int rows = 128;
        int columns = 20;
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                IntStream.range(0, 20).mapToObj(i -> new PageProjectionWithOutputs(new YieldPageProjection(new InputPageProjection(0)), new int[] {i})).collect(toImmutableList()),
                OptionalInt.of(MAX_BATCH_SIZE));

        Slice[] slices = new Slice[rows];
        Arrays.fill(slices, Slices.allocate(rows));
        Page inputPage = new Page(createSlicesBlock(slices));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, yieldSignal, inputPage);

        // Test yield signal works for page processor.
        // The purpose of this test is NOT to test the yield signal in page projection; we have other tests to cover that.
        // In page processor, we check yield signal after a column has been completely processed.
        // So we would like to set yield signal when the column has just finished processing in order to let page processor capture the yield signal when the block is returned.
        // Also, we would like to reset the yield signal before starting to process the next column in order NOT to yield per position inside the column.
        for (int i = 0; i < columns - 1; i++) {
            assertTrue(output.hasNext());
            assertNull(output.next().orElse(null));
            assertTrue(yieldSignal.isSet());
            yieldSignal.reset();
        }
        assertTrue(output.hasNext());
        Page actualPage = output.next().orElse(null);
        assertNotNull(actualPage);
        assertTrue(yieldSignal.isSet());
        yieldSignal.reset();

        Block[] blocks = new Block[columns];
        Arrays.fill(blocks, createSlicesBlock(Arrays.copyOfRange(slices, 0, rows)));
        Page expectedPage = new Page(blocks);
        assertPageEquals(Collections.nCopies(columns, VARCHAR), actualPage, expectedPage);
        assertFalse(output.hasNext());
    }

    @Test
    public void testExpressionProfiler()
    {
        MetadataManager metadata = createTestMetadataManager();
        CallExpression add10Expression = call(
                ADD.name(),
                metadata.getFunctionAndTypeManager().resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
                BIGINT,
                field(0, BIGINT),
                constant(10L, BIGINT));

        TestingTicker testingTicker = new TestingTicker();
        PageFunctionCompiler functionCompiler = new PageFunctionCompiler(metadata, 0);
        Supplier<PageProjection> projectionSupplier = functionCompiler.compileProjection(SESSION.getSqlFunctionProperties(), add10Expression, Optional.empty());
        PageProjection projection = projectionSupplier.get();
        Page page = new Page(createLongSequenceBlock(1, 11));
        ExpressionProfiler profiler = new ExpressionProfiler(testingTicker, SPLIT_RUN_QUANTA);
        for (int i = 0; i < 100; i++) {
            profiler.start();
            Work<List<Block>> work = projection.project(SESSION.getSqlFunctionProperties(), new DriverYieldSignal(), page, SelectedPositions.positionsRange(0, page.getPositionCount()));
            if (i < 10) {
                // increment the ticker with a large value to mark the expression as expensive
                testingTicker.increment(10, SECONDS);
                profiler.stop(page.getPositionCount());
                assertTrue(profiler.isExpressionExpensive());
            }
            else {
                testingTicker.increment(0, NANOSECONDS);
                profiler.stop(page.getPositionCount());
                assertFalse(profiler.isExpressionExpensive());
            }
            work.process();
        }
    }

    @Test
    public void testIncreasingBatchSize()
    {
        int rows = 1024;

        // We deliberately do not set the ticker, so that the expression is always cheap and the batch size gets doubled until other limits are hit
        TestingTicker testingTicker = new TestingTicker();
        ExpressionProfiler profiler = new ExpressionProfiler(testingTicker, SPLIT_RUN_QUANTA);
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                ImmutableList.of(createInputPageProjectionWithOutputs(0, BIGINT, 0)),
                OptionalInt.of(1),
                profiler);

        Slice[] slices = new Slice[rows];
        Arrays.fill(slices, Slices.allocate(rows));
        Page inputPage = new Page(createSlicesBlock(slices));
        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        long previousPositionCount = 1;
        long totalPositionCount = 0;
        while (totalPositionCount < rows) {
            Optional<Page> page = output.next();
            assertTrue(page.isPresent());
            long positionCount = page.get().getPositionCount();
            totalPositionCount += positionCount;
            // skip the first read && skip the last read, which can be a partial page
            if (positionCount > 1 && totalPositionCount != rows) {
                assertEquals(positionCount, previousPositionCount * 2);
            }
            previousPositionCount = positionCount;
        }
    }

    @Test
    public void testDecreasingBatchSize()
    {
        int rows = 1024;

        // We set the expensive expression threshold to 0, so the expression is always considered expensive and the batch size gets halved until it becomes 1
        TestingTicker testingTicker = new TestingTicker();
        ExpressionProfiler profiler = new ExpressionProfiler(testingTicker, new Duration(0, MILLISECONDS));
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                ImmutableList.of(createInputPageProjectionWithOutputs(0, BIGINT, 0)),
                OptionalInt.of(512),
                profiler);

        Slice[] slices = new Slice[rows];
        Arrays.fill(slices, Slices.allocate(rows));
        Page inputPage = new Page(createSlicesBlock(slices));
        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        long previousPositionCount = 1;
        long totalPositionCount = 0;
        while (totalPositionCount < rows) {
            Optional<Page> page = output.next();
            assertTrue(page.isPresent());
            long positionCount = page.get().getPositionCount();
            totalPositionCount += positionCount;
            // the batch size doesn't get smaller than 1
            if (positionCount > 1 && previousPositionCount != 1) {
                assertEquals(positionCount, previousPositionCount / 2);
            }
            previousPositionCount = positionCount;
        }
    }

    private PageProjectionWithOutputs createInputPageProjectionWithOutputs(int inputChannel, Type type, int outputChannel)
    {
        return new PageProjectionWithOutputs(new InputPageProjection(inputChannel), new int[] {outputChannel});
    }

    private Iterator<Optional<Page>> processAndAssertRetainedPageSize(PageProcessor pageProcessor, Page inputPage)
    {
        return processAndAssertRetainedPageSize(pageProcessor, new DriverYieldSignal(), inputPage);
    }

    private Iterator<Optional<Page>> processAndAssertRetainedPageSize(PageProcessor pageProcessor, DriverYieldSignal yieldSignal, Page inputPage)
    {
        return processAndAssertRetainedPageSize(pageProcessor, yieldSignal, newSimpleAggregatedMemoryContext(), inputPage);
    }

    private Iterator<Optional<Page>> processAndAssertRetainedPageSize(PageProcessor pageProcessor, DriverYieldSignal yieldSignal, AggregatedMemoryContext memoryContext, Page inputPage)
    {
        Iterator<Optional<Page>> output = pageProcessor.process(
                SESSION.getSqlFunctionProperties(),
                yieldSignal,
                memoryContext.newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                inputPage);
        assertEquals(memoryContext.getBytes(), 0);
        return output;
    }

    private void testPartialFilterAsList(Type type, int positionCount, float primitiveNullRate, float nestedNullRate, boolean useBlockView, List<BlockAssertions.Encoding> wrappings)
    {
        int[] positions = IntStream.range(0, positionCount / 2).map(x -> x * 2).toArray();
        PageProcessor pageProcessor = new PageProcessor(
                Optional.of(new TestingPageFilter(positionsList(positions, 0, positionCount / 2))),
                ImmutableList.of(createInputPageProjectionWithOutputs(0, BIGINT, 0)),
                OptionalInt.of(MAX_BATCH_SIZE));

        Page inputPage = createPageWithRandomData(ImmutableList.of(type), positionCount, false, false, primitiveNullRate, nestedNullRate, useBlockView, wrappings);

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        assertPageEquals(ImmutableList.of(type), outputPages.get(0).orElse(null), new Page(inputPage.getBlock(0).copyPositions(positions, 0, positionCount / 2)));
    }

    private static class InvocationCountPageProjection
            implements PageProjection
    {
        protected final PageProjection delegate;
        private int invocationCount;

        public InvocationCountPageProjection(PageProjection delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public boolean isDeterministic()
        {
            return delegate.isDeterministic();
        }

        @Override
        public InputChannels getInputChannels()
        {
            return delegate.getInputChannels();
        }

        @Override
        public Work<List<Block>> project(SqlFunctionProperties properties, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
        {
            setInvocationCount(getInvocationCount() + 1);
            return delegate.project(properties, yieldSignal, page, selectedPositions);
        }

        public int getInvocationCount()
        {
            return invocationCount;
        }

        public void setInvocationCount(int invocationCount)
        {
            this.invocationCount = invocationCount;
        }
    }

    private class YieldPageProjection
            extends InvocationCountPageProjection
    {
        public YieldPageProjection(PageProjection delegate)
        {
            super(delegate);
        }

        @Override
        public Work<List<Block>> project(SqlFunctionProperties properties, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
        {
            return new YieldPageProjectionWork(properties, yieldSignal, page, selectedPositions);
        }

        private class YieldPageProjectionWork
                implements Work<List<Block>>
        {
            private final DriverYieldSignal yieldSignal;
            private final Work<List<Block>> work;

            public YieldPageProjectionWork(SqlFunctionProperties properties, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
            {
                this.yieldSignal = yieldSignal;
                this.work = delegate.project(properties, yieldSignal, page, selectedPositions);
            }

            @Override
            public boolean process()
            {
                assertTrue(work.process());
                yieldSignal.setWithDelay(1, executor);
                yieldSignal.forceYieldForTesting();
                return true;
            }

            @Override
            public List<Block> getResult()
            {
                return work.getResult();
            }
        }
    }

    public static class LazyPagePageProjection
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
            return new InputChannels(0, 1);
        }

        @Override
        public Work<List<Block>> project(SqlFunctionProperties properties, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
        {
            return new CompletedWork<>(ImmutableList.of(page.getBlock(0).getLoadedBlock()));
        }
    }

    public static class TestingPageFilter
            implements PageFilter
    {
        private final SelectedPositions selectedPositions;

        public TestingPageFilter(SelectedPositions selectedPositions)
        {
            this.selectedPositions = selectedPositions;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0);
        }

        @Override
        public SelectedPositions filter(SqlFunctionProperties properties, Page page)
        {
            return selectedPositions;
        }
    }

    public static class SelectAllFilter
            implements PageFilter
    {
        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0);
        }

        @Override
        public SelectedPositions filter(SqlFunctionProperties properties, Page page)
        {
            return positionsRange(0, page.getPositionCount());
        }
    }

    private static class SelectNoneFilter
            implements PageFilter
    {
        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0);
        }

        @Override
        public SelectedPositions filter(SqlFunctionProperties properties, Page page)
        {
            return positionsRange(0, 0);
        }
    }
}
