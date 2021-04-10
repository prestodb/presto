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
package com.facebook.presto.operator;

import com.facebook.presto.SequencePageBuilder;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LazyBlock;
import com.facebook.presto.common.block.LazyBlockLoader;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.index.PageRecordSet;
import com.facebook.presto.operator.project.CursorProcessor;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.operator.project.PageProjectionWithOutputs;
import com.facebook.presto.operator.project.TestPageProcessor.LazyPagePageProjection;
import com.facebook.presto.operator.project.TestPageProcessor.SelectAllFilter;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.toValues;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestScanFilterAndProjectOperator
        extends AbstractTestFunctions
{
    private static final TableHandle TESTING_TABLE_HANDLE = new TableHandle(
            new ConnectorId("test"),
            new ConnectorTableHandle() {},
            new ConnectorTransactionHandle() {},
            Optional.empty());

    private final MetadataManager metadata = createTestMetadataManager();
    private final ExpressionCompiler expressionCompiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    public TestScanFilterAndProjectOperator()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @Test
    public void testPageSource()
    {
        final Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();

        List<RowExpression> projections = ImmutableList.of(field(0, VARCHAR));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(driverContext.getSession().getSqlFunctionProperties(), Optional.empty(), projections, "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(driverContext.getSession().getSqlFunctionProperties(), Optional.empty(), projections);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (session, split, table, columns) -> new FixedPageSource(ImmutableList.of(input)),
                cursorProcessor,
                pageProcessor,
                TESTING_TABLE_HANDLE,
                ImmutableList.of(),
                ImmutableList.of(VARCHAR),
                Optional.empty(),
                new DataSize(0, BYTE),
                0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        MaterializedResult expected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), toPages(operator));

        assertEquals(actual.getRowCount(), expected.getRowCount());
        assertEquals(actual, expected);
    }

    @Test
    public void testPageSourceMergeOutput()
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .build();

        RowExpression filter = call(
                EQUAL.name(),
                createTestMetadataManager().getFunctionAndTypeManager().resolveOperator(EQUAL, fromTypes(BIGINT, BIGINT)),
                BOOLEAN,
                field(0, BIGINT),
                constant(10L, BIGINT));
        List<RowExpression> projections = ImmutableList.of(field(0, BIGINT));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(TEST_SESSION.getSqlFunctionProperties(), Optional.of(filter), projections, "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(TEST_SESSION.getSqlFunctionProperties(), Optional.of(filter), projections);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (session, split, table, columns) -> new FixedPageSource(input),
                cursorProcessor,
                pageProcessor,
                TESTING_TABLE_HANDLE,
                ImmutableList.of(),
                ImmutableList.of(BIGINT),
                Optional.empty(),
                new DataSize(64, KILOBYTE),
                2);

        SourceOperator operator = factory.createOperator(newDriverContext());
        operator.addSplit(new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        List<Page> actual = toPages(operator);
        assertEquals(actual.size(), 1);

        List<Page> expected = rowPagesBuilder(BIGINT)
                .row(10L)
                .row(10L)
                .row(10L)
                .row(10L)
                .build();

        assertPageEquals(ImmutableList.of(BIGINT), actual.get(0), expected.get(0));
    }

    @Test
    public void testPageSourceLazyLoad()
    {
        Block inputBlock = BlockAssertions.createLongSequenceBlock(0, 100);
        // If column 1 is loaded, test will fail
        Page input = new Page(100, inputBlock, new LazyBlock(100, lazyBlock -> {
            throw new AssertionError("Lazy block should not be loaded");
        }));
        DriverContext driverContext = newDriverContext();

        List<RowExpression> projections = ImmutableList.of(field(0, VARCHAR));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(driverContext.getSession().getSqlFunctionProperties(), Optional.empty(), projections, "key");
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectAllFilter()), ImmutableList.of(new PageProjectionWithOutputs(new LazyPagePageProjection(), new int[] {0})));

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (session, split, table, columns) -> new SinglePagePageSource(input),
                cursorProcessor,
                () -> pageProcessor,
                TESTING_TABLE_HANDLE,
                ImmutableList.of(),
                ImmutableList.of(BIGINT),
                Optional.empty(),
                new DataSize(0, BYTE),
                0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        MaterializedResult expected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(BIGINT), ImmutableList.of(new Page(inputBlock)));
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(BIGINT), toPages(operator));

        assertEquals(actual.getRowCount(), expected.getRowCount());
        assertEquals(actual, expected);
    }

    @Test
    public void testPageSourceLazyBlock()
    {
        // Tests that a page containing a LazyBlock is loaded and its bytes are counted by the operator.
        DriverContext driverContext = newDriverContext();
        List<RowExpression> projections = ImmutableList.of(field(0, BIGINT));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(driverContext.getSession().getSqlFunctionProperties(), Optional.empty(), projections, "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(driverContext.getSession().getSqlFunctionProperties(), Optional.empty(), projections);

        // This Block will be wrapped in a LazyBlock inside the operator on call to getNextPage().
        Block inputBlock = BlockAssertions.createLongSequenceBlock(0, 10);

        CountingLazyPageSource pageSource = new CountingLazyPageSource(ImmutableList.of(new Page(inputBlock)));

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (session, split, table, columns) -> pageSource,
                cursorProcessor,
                pageProcessor,
                TESTING_TABLE_HANDLE,
                ImmutableList.of(),
                ImmutableList.of(BIGINT),
                Optional.empty(),
                new DataSize(0, BYTE),
                0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        MaterializedResult expected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(BIGINT), ImmutableList.of(new Page(inputBlock)));
        Page expectedPage = expected.toPage();
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(BIGINT), toPages(operator));
        Page actualPage = actual.toPage();

        // Assert expected page and actual page are equal.
        assertPageEquals(actual.getTypes(), actualPage, expectedPage);

        // PageSource counting isn't flawed, assert on the test implementation
        assertEquals(pageSource.getCompletedBytes(), expectedPage.getSizeInBytes());
        assertEquals(pageSource.getCompletedPositions(), expectedPage.getPositionCount());

        // Assert operator stats match the expected values
        assertEquals(operator.getOperatorContext().getOperatorStats().getRawInputDataSize().toBytes(), expectedPage.getSizeInBytes());
        assertEquals(operator.getOperatorContext().getOperatorStats().getInputPositions(), expected.getRowCount());
    }

    @Test
    public void testRecordCursorSource()
    {
        final Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();

        List<RowExpression> projections = ImmutableList.of(field(0, VARCHAR));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(driverContext.getSession().getSqlFunctionProperties(), Optional.empty(), projections, "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(driverContext.getSession().getSqlFunctionProperties(), Optional.empty(), projections);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (session, split, table, columns) -> new RecordPageSource(new PageRecordSet(ImmutableList.of(VARCHAR), input)),
                cursorProcessor,
                pageProcessor,
                TESTING_TABLE_HANDLE,
                ImmutableList.of(),
                ImmutableList.of(VARCHAR),
                Optional.empty(),
                new DataSize(0, BYTE),
                0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        MaterializedResult expected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), toPages(operator));

        assertEquals(actual.getRowCount(), expected.getRowCount());
        assertEquals(actual, expected);
    }

    @Test
    public void testPageYield()
    {
        int totalRows = 1000;
        Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(BIGINT), totalRows, 1);
        DriverContext driverContext = newDriverContext();

        // 20 columns; each column is associated with a function that will force yield per projection
        int totalColumns = 20;
        ImmutableList.Builder<SqlScalarFunction> functions = ImmutableList.builder();
        for (int i = 0; i < totalColumns; i++) {
            functions.add(new GenericLongFunction("page_col" + i, value -> {
                driverContext.getYieldSignal().forceYieldForTesting();
                return value;
            }));
        }
        Metadata metadata = functionAssertions.getMetadata();
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        functionAndTypeManager.registerBuiltInFunctions(functions.build());

        // match each column with a projection
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
        ImmutableList.Builder<RowExpression> projections = ImmutableList.builder();
        for (int i = 0; i < totalColumns; i++) {
            projections.add(call("generic_long_page_col", functionAndTypeManager.lookupFunction("generic_long_page_col" + i, fromTypes(BIGINT)), BIGINT, field(0, BIGINT)));
        }
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(driverContext.getSession().getSqlFunctionProperties(), Optional.empty(), projections.build(), "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(driverContext.getSession().getSqlFunctionProperties(), Optional.empty(), projections.build(), false, MAX_BATCH_SIZE);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (session, split, table, columns) -> new FixedPageSource(ImmutableList.of(input)),
                cursorProcessor,
                pageProcessor,
                TESTING_TABLE_HANDLE,
                ImmutableList.of(),
                ImmutableList.of(BIGINT),
                Optional.empty(),
                new DataSize(0, BYTE),
                0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        // In the below loop we yield for every cell: 20 X 1000 times
        // Currently we don't check for the yield signal in the generated projection loop, we only check for the yield signal
        // in the PageProcessor.PositionsPageProcessorIterator::computeNext() method. Therefore, after 20 calls we will have
        // exactly 20 blocks (one for each column) and the PageProcessor will be able to create a Page out of it.
        for (int i = 1; i <= totalRows * totalColumns; i++) {
            driverContext.getYieldSignal().setWithDelay(SECONDS.toNanos(1000), driverContext.getYieldExecutor());
            Page page = operator.getOutput();
            if (i == totalColumns) {
                assertNotNull(page);
                assertEquals(page.getPositionCount(), totalRows);
                assertEquals(page.getChannelCount(), totalColumns);
                for (int j = 0; j < totalColumns; j++) {
                    assertEquals(toValues(BIGINT, page.getBlock(j)), toValues(BIGINT, input.getBlock(0)));
                }
            }
            else {
                assertNull(page);
            }
            driverContext.getYieldSignal().reset();
        }
    }

    @Test
    public void testRecordCursorYield()
    {
        // create a generic long function that yields for projection on every row
        // verify we will yield #row times totally

        // create a table with 15 rows
        int length = 15;
        Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(BIGINT), length, 0);
        DriverContext driverContext = newDriverContext();

        // set up generic long function with a callback to force yield
        Metadata metadata = functionAssertions.getMetadata();
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        functionAndTypeManager.registerBuiltInFunctions(ImmutableList.of(new GenericLongFunction("record_cursor", value -> {
            driverContext.getYieldSignal().forceYieldForTesting();
            return value;
        })));
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));

        List<RowExpression> projections = ImmutableList.of(call(
                "generic_long_record_cursor",
                functionAndTypeManager.lookupFunction("generic_long_record_cursor", fromTypes(BIGINT)),
                BIGINT,
                field(0, BIGINT)));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(driverContext.getSession().getSqlFunctionProperties(), Optional.empty(), projections, "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(driverContext.getSession().getSqlFunctionProperties(), Optional.empty(), projections);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (session, split, table, columns) -> new RecordPageSource(new PageRecordSet(ImmutableList.of(BIGINT), input)),
                cursorProcessor,
                pageProcessor,
                TESTING_TABLE_HANDLE,
                ImmutableList.of(),
                ImmutableList.of(BIGINT),
                Optional.empty(),
                new DataSize(0, BYTE),
                0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        // start driver; get null value due to yield for the first 15 times
        for (int i = 0; i < length; i++) {
            driverContext.getYieldSignal().setWithDelay(SECONDS.toNanos(1000), driverContext.getYieldExecutor());
            assertNull(operator.getOutput());
            driverContext.getYieldSignal().reset();
        }

        // the 16th yield is not going to prevent the operator from producing a page
        driverContext.getYieldSignal().setWithDelay(SECONDS.toNanos(1000), driverContext.getYieldExecutor());
        Page output = operator.getOutput();
        driverContext.getYieldSignal().reset();
        assertNotNull(output);
        assertEquals(toValues(BIGINT, output.getBlock(0)), toValues(BIGINT, input.getBlock(0)));
    }

    private static List<Page> toPages(Operator operator)
    {
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        // read output until input is needed or operator is finished
        int nullPages = 0;
        while (!operator.isFinished()) {
            Page outputPage = operator.getOutput();
            if (outputPage == null) {
                // break infinite loop due to null pages
                assertTrue(nullPages < 1_000_000, "Too many null pages; infinite loop?");
                nullPages++;
            }
            else {
                outputPages.add(outputPage);
                nullPages = 0;
            }
        }

        return outputPages.build();
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    public class SinglePagePageSource
            implements ConnectorPageSource
    {
        private Page page;

        public SinglePagePageSource(Page page)
        {
            this.page = page;
        }

        @Override
        public void close()
        {
            page = null;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getCompletedPositions()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public long getSystemMemoryUsage()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return page == null;
        }

        @Override
        public Page getNextPage()
        {
            Page page = this.page;
            this.page = null;
            return page;
        }
    }

    private static class CountingLazyPageSource
            implements ConnectorPageSource
    {
        private Iterator<Page> pages;
        private long completedBytes;
        private long completedPositions;

        public CountingLazyPageSource(List<Page> pages)
        {
            this.pages = requireNonNull(pages, "pages is null").iterator();
        }

        @Override
        public void close()
        {
            pages = Iterators.forArray();
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public long getSystemMemoryUsage()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return !pages.hasNext();
        }

        @Override
        public Page getNextPage()
        {
            if (isFinished()) {
                return null;
            }

            Page page = pages.next();
            int channelCount = page.getChannelCount();
            Block[] blocks = new Block[channelCount];

            for (int i = 0; i < channelCount; ++i) {
                Block block = page.getBlock(i);
                // Wrap current Block in a LazyBlock.
                blocks[i] = new LazyBlock(block.getPositionCount(), new CountingLazyBlockLoader(block));
            }
            completedPositions += page.getPositionCount();

            return new Page(page.getPositionCount(), blocks);
        }

        @Override
        public long getCompletedBytes()
        {
            return completedBytes;
        }

        @Override
        public long getCompletedPositions()
        {
            return completedPositions;
        }

        private final class CountingLazyBlockLoader
                implements LazyBlockLoader<LazyBlock>
        {
            private Block loaderBlock;

            public CountingLazyBlockLoader(Block block)
            {
                loaderBlock = block;
            }

            @Override
            public final void load(LazyBlock lazyBlock)
            {
                checkState(loaderBlock != null, "loaderBlock already loaded");

                Block loadedBlock = loaderBlock.getLoadedBlock();
                // Increment completed/read bytes for the page source.
                completedBytes += loadedBlock.getSizeInBytes();
                loaderBlock = null;
                lazyBlock.setBlock(loadedBlock);
            }
        }
    }
}
