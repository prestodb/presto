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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LazyBlock;
import com.facebook.presto.common.block.LazyBlockLoader;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.project.CursorProcessor;
import com.facebook.presto.operator.project.CursorProcessorOutput;
import com.facebook.presto.operator.project.MergingPageOutput;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.EmptySplit;
import com.facebook.presto.split.EmptySplitPageSource;
import com.facebook.presto.split.PageSourceProvider;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.MoreFutures.toListenableFuture;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ScanFilterAndProjectOperator
        implements SourceOperator, Closeable
{
    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final PageSourceProvider pageSourceProvider;
    private final TableHandle table;
    private final List<ColumnHandle> columns;
    private final PageBuilder pageBuilder;
    private final CursorProcessor cursorProcessor;
    private final PageProcessor pageProcessor;
    private final SqlFunctionProperties sqlFunctionProperties;
    private final LocalMemoryContext pageSourceMemoryContext;
    private final LocalMemoryContext pageProcessorMemoryContext;
    private final LocalMemoryContext outputMemoryContext;
    private final SettableFuture<?> blocked = SettableFuture.create();
    private final Optional<Supplier<TupleDomain<ColumnHandle>>> dynamicFilterSupplier;
    private final MergingPageOutput mergingOutput;

    private RecordCursor cursor;
    private ConnectorPageSource pageSource;

    private Split split;

    private boolean finishing;

    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;

    protected ScanFilterAndProjectOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            PageSourceProvider pageSourceProvider,
            CursorProcessor cursorProcessor,
            PageProcessor pageProcessor,
            TableHandle table,
            Iterable<ColumnHandle> columns,
            Iterable<Type> types,
            Optional<Supplier<TupleDomain<ColumnHandle>>> dynamicFilterSupplier,
            MergingPageOutput mergingOutput)
    {
        this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
        this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sqlFunctionProperties = operatorContext.getSession().getSqlFunctionProperties();
        this.planNodeId = requireNonNull(sourceId, "sourceId is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.pageSourceMemoryContext = operatorContext.newLocalSystemMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
        this.pageProcessorMemoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
        this.outputMemoryContext = operatorContext.newLocalSystemMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
        this.dynamicFilterSupplier = requireNonNull(dynamicFilterSupplier, "dynamicFilterSupplier is null");
        this.mergingOutput = requireNonNull(mergingOutput, "mergingOutput is null");

        this.pageBuilder = new PageBuilder(ImmutableList.copyOf(requireNonNull(types, "types is null")));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return planNodeId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkState(this.split == null, "Table scan split already set");

        if (finishing) {
            return Optional::empty;
        }

        this.split = split;

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(Suppliers.ofInstance(new SplitOperatorInfo(splitInfo)));
        }
        blocked.set(null);

        if (split.getConnectorSplit() instanceof EmptySplit) {
            pageSource = new EmptySplitPageSource();
        }

        return () -> {
            if (pageSource instanceof UpdatablePageSource) {
                return Optional.of((UpdatablePageSource) pageSource);
            }
            return Optional.empty();
        };
    }

    @Override
    public void noMoreSplits()
    {
        if (split == null) {
            finishing = true;
            mergingOutput.finish();
        }
        blocked.set(null);
    }

    @Override
    public void close()
    {
        finish();
    }

    @Override
    public void finish()
    {
        blocked.set(null);
        if (pageSource != null) {
            try {
                pageSource.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        else if (cursor != null) {
            cursor.close();
        }
        finishing = true;
        mergingOutput.finish();
    }

    @Override
    public final boolean isFinished()
    {
        return finishing && pageBuilder.isEmpty() && mergingOutput.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!blocked.isDone()) {
            return blocked;
        }
        if (pageSource != null) {
            CompletableFuture<?> pageSourceBlocked = pageSource.isBlocked();
            return pageSourceBlocked.isDone() ? NOT_BLOCKED : toListenableFuture(pageSourceBlocked);
        }
        return NOT_BLOCKED;
    }

    @Override
    public final boolean needsInput()
    {
        return false;
    }

    @Override
    public final void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (split == null) {
            return null;
        }

        if (!finishing && pageSource == null && cursor == null) {
            ConnectorPageSource source = pageSourceProvider.createPageSource(operatorContext.getSession(), split, dynamicFilterSupplier.map(table::withDynamicFilter).orElse(table), columns);
            if (source instanceof RecordPageSource) {
                cursor = ((RecordPageSource) source).getCursor();
            }
            else {
                pageSource = source;
            }
        }

        if (pageSource != null) {
            return processPageSource();
        }
        else {
            return processColumnSource();
        }
    }

    private Page processColumnSource()
    {
        DriverYieldSignal yieldSignal = operatorContext.getDriverContext().getYieldSignal();
        if (!finishing && !yieldSignal.isSet()) {
            CursorProcessorOutput output = cursorProcessor.process(sqlFunctionProperties, yieldSignal, cursor, pageBuilder);
            pageSourceMemoryContext.setBytes(cursor.getSystemMemoryUsage());

            recordCursorInputStats(output.getProcessedRows());
            if (output.isNoMoreRows()) {
                finishing = true;
                mergingOutput.finish();
            }
        }

        // only return a page if buffer is full or we are finishing
        Page page = null;
        if (!pageBuilder.isEmpty() && (finishing || pageBuilder.isFull())) {
            page = pageBuilder.build();
            pageBuilder.reset();
        }
        outputMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
        return page;
    }

    private Page processPageSource()
    {
        DriverYieldSignal yieldSignal = operatorContext.getDriverContext().getYieldSignal();
        if (!finishing && mergingOutput.needsInput() && !yieldSignal.isSet()) {
            Page page = pageSource.getNextPage();

            finishing = pageSource.isFinished();
            pageSourceMemoryContext.setBytes(pageSource.getSystemMemoryUsage());

            if (page != null) {
                // update operator stats
                page = recordProcessedInput(page);

                Iterator<Optional<Page>> output = pageProcessor.process(sqlFunctionProperties, yieldSignal, pageProcessorMemoryContext, page);
                mergingOutput.addInput(output);
            }

            // stats update
            recordInputStats();

            if (finishing) {
                mergingOutput.finish();
            }
        }

        Page result = mergingOutput.getOutput();
        outputMemoryContext.setBytes(mergingOutput.getRetainedSizeInBytes() + pageProcessorMemoryContext.getBytes());
        return result;
    }

    private final class RecordingLazyBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private LazyBlock delegateLazyBlock;

        private RecordingLazyBlockLoader(LazyBlock delegateLazyBlock)
        {
            this.delegateLazyBlock = requireNonNull(delegateLazyBlock, "delegateLazyBlock is null");
        }

        @Override
        public void load(LazyBlock block)
        {
            checkState(delegateLazyBlock != null, "delegateLazyBlock already loaded");
            Block loadedBlock = delegateLazyBlock.getLoadedBlock();
            delegateLazyBlock = null;
            // Position count already recorded for lazy blocks, input bytes are not
            recordInputStats();
            block.setBlock(loadedBlock);
        }
    }

    private void recordCursorInputStats(long positionCount)
    {
        checkState(cursor != null, "cursor is null");
        long endCompletedBytes = cursor.getCompletedBytes();
        long endReadTimeNanos = cursor.getReadTimeNanos();
        long inputBytes = endCompletedBytes - completedBytes;
        operatorContext.recordProcessedInput(inputBytes, positionCount);
        operatorContext.recordRawInputWithTiming(inputBytes, positionCount, endReadTimeNanos - readTimeNanos);
        completedBytes = endCompletedBytes;
        readTimeNanos = endReadTimeNanos;
    }

    private void recordInputStats()
    {
        checkState(pageSource != null, "pageSource is null");
        long endCompletedBytes = pageSource.getCompletedBytes();
        long endCompletedPositions = pageSource.getCompletedPositions();
        long endReadTimeNanos = pageSource.getReadTimeNanos();
        long inputBytes = endCompletedBytes - completedBytes;
        long positionCount = endCompletedPositions - completedPositions;
        operatorContext.recordProcessedInput(inputBytes, positionCount);
        operatorContext.recordRawInputWithTiming(inputBytes, positionCount, endReadTimeNanos - readTimeNanos);
        operatorContext.updateStats(pageSource.getRuntimeStats());
        completedBytes = endCompletedBytes;
        completedPositions = endCompletedPositions;
        readTimeNanos = endReadTimeNanos;
    }

    private Page recordProcessedInput(Page page)
    {
        long blockSizeSum = 0L;
        Block[] blocks = null;
        for (int i = 0; i < page.getChannelCount(); ++i) {
            Block block = page.getBlock(i);
            // account processed bytes from lazy blocks only when they are loaded
            if (block instanceof LazyBlock && !((LazyBlock) block).isLoaded()) {
                if (blocks == null) {
                    blocks = copyOfPageBlocks(page);
                }
                blocks[i] = new LazyBlock(page.getPositionCount(), new RecordingLazyBlockLoader((LazyBlock) block));
            }
            else {
                blockSizeSum += block.getSizeInBytes();
            }
        }

        return (blocks == null) ? page : new Page(page.getPositionCount(), blocks);
    }

    private static Block[] copyOfPageBlocks(Page page)
    {
        Block[] blocks = new Block[page.getChannelCount()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = page.getBlock(i);
        }
        return blocks;
    }

    public static class ScanFilterAndProjectOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Supplier<CursorProcessor> cursorProcessor;
        private final Supplier<PageProcessor> pageProcessor;
        private final PlanNodeId sourceId;
        private final PageSourceProvider pageSourceProvider;
        private final TableHandle table;
        private final List<ColumnHandle> columns;
        private final List<Type> types;
        private final Optional<Supplier<TupleDomain<ColumnHandle>>> dynamicFilterSupplier;
        private final DataSize minOutputPageSize;
        private final int minOutputPageRowCount;
        private boolean closed;

        public ScanFilterAndProjectOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PlanNodeId sourceId,
                PageSourceProvider pageSourceProvider,
                Supplier<CursorProcessor> cursorProcessor,
                Supplier<PageProcessor> pageProcessor,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                List<Type> types,
                Optional<Supplier<TupleDomain<ColumnHandle>>> dynamicFilterSupplier,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
            this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.types = requireNonNull(types, "types is null");
            this.dynamicFilterSupplier = requireNonNull(dynamicFilterSupplier, "dynamicFilterSupplier is null");
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, ScanFilterAndProjectOperator.class.getSimpleName());
            return new ScanFilterAndProjectOperator(
                    operatorContext,
                    sourceId,
                    pageSourceProvider,
                    cursorProcessor.get(),
                    pageProcessor.get(),
                    table,
                    columns,
                    types,
                    dynamicFilterSupplier,
                    new MergingPageOutput(types, minOutputPageSize.toBytes(), minOutputPageRowCount));
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }
}
