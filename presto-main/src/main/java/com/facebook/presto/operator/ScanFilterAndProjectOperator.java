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

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.project.CursorProcessor;
import com.facebook.presto.operator.project.CursorProcessorOutput;
import com.facebook.presto.operator.project.DictionaryAwarePageFilter;
import com.facebook.presto.operator.project.MergingPageOutput;
import com.facebook.presto.operator.project.PageFilter;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.operator.project.SelectedPositions;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PageSourceOptions;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.EmptySplit;
import com.facebook.presto.split.EmptySplitPageSource;
import com.facebook.presto.split.PageSourceProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.isAriaScanEnabled;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

public class ScanFilterAndProjectOperator
        implements SourceOperator, Closeable
{
    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final PageSourceProvider pageSourceProvider;
    private final List<ColumnHandle> columns;
    private final PageBuilder pageBuilder;
    private final CursorProcessor cursorProcessor;
    private final PageProcessor pageProcessor;
    private final LocalMemoryContext pageSourceMemoryContext;
    private final LocalMemoryContext pageProcessorMemoryContext;
    private final LocalMemoryContext outputMemoryContext;
    private final SettableFuture<?> blocked = SettableFuture.create();
    private final MergingPageOutput mergingOutput;

    private RecordCursor cursor;
    private ConnectorPageSource pageSource;

    private Split split;

    private boolean finishing;

    private long completedBytes;
    private long readTimeNanos;

    private boolean filterAndProjectPushedDown;
    private boolean reusePages;

    protected ScanFilterAndProjectOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            PageSourceProvider pageSourceProvider,
            CursorProcessor cursorProcessor,
            PageProcessor pageProcessor,
            Iterable<ColumnHandle> columns,
            Iterable<Type> types,
            MergingPageOutput mergingOutput)
    {
        this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
        this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.planNodeId = requireNonNull(sourceId, "sourceId is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.pageSourceMemoryContext = operatorContext.newLocalSystemMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
        this.pageProcessorMemoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
        this.outputMemoryContext = operatorContext.newLocalSystemMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
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
            operatorContext.setInfoSupplier(() -> new SplitOperatorInfo(splitInfo));
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

    private static class FilterExpression
            extends PageSourceOptions.FilterFunction
    {
        final ConnectorSession session;
        final PageFilter filter;

        FilterExpression(ConnectorSession session, PageFilter filter)
        {
            super(filter.getInputChannels().getInputChannels().stream().mapToInt(Integer::intValue).toArray(), 1);
            this.session = session;
            this.filter = filter;
        }

        @Override
        public boolean isDeterministic()
        {
            return filter.isDeterministic();
        }

        @Override
        public int filter(Page page, int[] outputRows, PageSourceOptions.ErrorSet errorSet)
        {
            int positionCount = page.getPositionCount();
            if (outputRows.length < positionCount) {
                throw new IllegalArgumentException("outputRows too small");
            }
            try {
                SelectedPositions positions = filter.filter(session, page);
                int offset = positions.getOffset();
                int size = positions.size();
                if (!positions.isList()) {
                    for (int i = 0; i < size; i++) {
                        outputRows[i] = i + offset;
                    }
                }
                else {
                    int[] positionsArray = positions.getPositions();
                    for (int i = 0; i < size; i++) {
                        outputRows[i] = positionsArray[i] + offset;
                    }
                }
                return size;
            }
            catch (RuntimeException e) {
                /* Ignore the errirm try next row by row and track errors */
            }

            PageFilter rowFilter = filter;
            if (filter instanceof DictionaryAwarePageFilter) {
                DictionaryAwarePageFilter dictionaryFilter = (DictionaryAwarePageFilter) filter;
                rowFilter = dictionaryFilter.getFilter();
            }
            int numTrue = 0;
            for (int i = 0; i < positionCount; i++) {
                try {
                    if (rowFilter.filter(session, page, i)) {
                        outputRows[numTrue++] = i;
                    }
                }
                catch (RuntimeException e) {
                    outputRows[numTrue++] = i;
                    errorSet.addError(i, positionCount, e);
                }
            }
            return numTrue;
        }
    }

    @Override
    public Page getOutput()
    {
        if (split == null) {
            return null;
        }

        if (!finishing && pageSource == null && cursor == null) {
            ConnectorPageSource source = pageSourceProvider.createPageSource(operatorContext.getSession(), split, columns);
            if (source instanceof RecordPageSource) {
                cursor = ((RecordPageSource) source).getCursor();
            }
            else {
                pageSource = source;
                setupAria();
            }
        }
        if (pageSource != null) {
            return processPageSource();
        }
        else {
            return processColumnSource();
        }
    }

    private void setupAria()
    {
        if (!isAriaScanEnabled(operatorContext.getSession())) {
            return;
        }

        int[] projectionPushdownChannels = pageProcessor.getIdentityInputToOutputChannel();
        boolean projectionPushedDown = projectionPushdownChannels != null;

        int[] projectionInputChannels = pageProcessor.getOutputChannels();
        int maxChannel = maxChannel(projectionInputChannels);

        List<PageFilter> pageFilters = pageProcessor.getFilterWithoutTupleDomain();
        FilterExpression[] filters = new FilterExpression[pageFilters.size()];
        for (int i = 0; i < pageFilters.size(); i++) {
            filters[i] = new FilterExpression(operatorContext.getSession().toConnectorSession(), pageFilters.get(i));
            maxChannel = Math.max(maxChannel, maxChannel(filters[i].getInputChannels()));
        }

        int[] channels = new int[maxChannel + 1];
        Arrays.fill(channels, -1);
        populateChannels(channels, projectionInputChannels);
        for (int i = 0; i < filters.length; i++) {
            populateChannels(channels, filters[i].getInputChannels());
        }

        PageSourceOptions options = new PageSourceOptions(
                channels,
                projectionPushdownChannels == null ? channels : projectionPushdownChannels,
                reusePages,
                filters,
                mergingOutput.getMinPageSizeInBytes());

        boolean filterPushedDown = pageSource.pushdownFilterAndProjection(options);
        if (filterPushedDown && projectionPushedDown) {
            filterAndProjectPushedDown = true;
        }
        else if (filterPushedDown && !projectionPushedDown) {
            pageProcessor.setFilterIsPushedDown();
        }
    }

    private static void populateChannels(int[] allChannels, int[] selectedChannels)
    {
        for (int channel : selectedChannels) {
            allChannels[channel] = channel;
        }
    }

    private static int maxChannel(int[] channels)
    {
        int maxChannel = -1;
        for (int channel : channels) {
            maxChannel = Math.max(maxChannel, channel);
        }
        return maxChannel;
    }

    private Page processColumnSource()
    {
        DriverYieldSignal yieldSignal = operatorContext.getDriverContext().getYieldSignal();
        if (!finishing && !yieldSignal.isSet()) {
            CursorProcessorOutput output = cursorProcessor.process(operatorContext.getSession().toConnectorSession(), yieldSignal, cursor, pageBuilder);
            pageSourceMemoryContext.setBytes(cursor.getSystemMemoryUsage());

            long bytesProcessed = cursor.getCompletedBytes() - completedBytes;
            long elapsedNanos = cursor.getReadTimeNanos() - readTimeNanos;
            operatorContext.recordRawInputWithTiming(bytesProcessed, elapsedNanos);
            // TODO: derive better values for cursors
            operatorContext.recordProcessedInput(bytesProcessed, output.getProcessedRows());
            completedBytes = cursor.getCompletedBytes();
            readTimeNanos = cursor.getReadTimeNanos();
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
                if (!filterAndProjectPushedDown) {
                    Block[] blocks = new Block[page.getChannelCount()];
                    for (int i = 0; i < blocks.length; i++) {
                        if (page.getBlock(i) != null) {
                            blocks[i] = page.getBlock(i);
                        }
                        else {
                            Block nullValueBlock = pageBuilder.getType(i).createBlockBuilder(null, 1) .appendNull() .build();
                            blocks[i] = new RunLengthEncodedBlock(nullValueBlock, page.getPositionCount());
                        }
                    }
                    page = new Page(page.getPositionCount(), blocks);
                }
                page = recordProcessedInput(page);

                // update operator stats
                long endCompletedBytes = pageSource.getCompletedBytes();
                long endReadTimeNanos = pageSource.getReadTimeNanos();
                operatorContext.recordRawInputWithTiming(endCompletedBytes - completedBytes, endReadTimeNanos - readTimeNanos);
                completedBytes = endCompletedBytes;
                readTimeNanos = endReadTimeNanos;
                if (filterAndProjectPushedDown) {
                    return page;
                }

                Iterator<Optional<Page>> output = pageProcessor.process(operatorContext.getSession().toConnectorSession(), yieldSignal, pageProcessorMemoryContext, page);
                mergingOutput.addInput(output);
            }

            if (finishing) {
                mergingOutput.finish();
            }
        }

        Page result = mergingOutput.getOutput();
        outputMemoryContext.setBytes(mergingOutput.getRetainedSizeInBytes() + pageProcessorMemoryContext.getBytes());
        return result;
    }

    private Page recordProcessedInput(Page page)
    {
        operatorContext.recordProcessedInput(0, page.getPositionCount());
        // account processed bytes from lazy blocks only when they are loaded
        Block[] blocks = new Block[page.getChannelCount()];
        for (int i = 0; i < page.getChannelCount(); ++i) {
            Block block = page.getBlock(i);
            if (block instanceof LazyBlock) {
                LazyBlock delegateLazyBlock = (LazyBlock) block;
                blocks[i] = new LazyBlock(page.getPositionCount(), lazyBlock -> {
                    Block loadedBlock = delegateLazyBlock.getLoadedBlock();
                    operatorContext.recordProcessedInput(loadedBlock.getSizeInBytes(), 0L);
                    lazyBlock.setBlock(loadedBlock);
                });
            }
            else {
                operatorContext.recordProcessedInput(block.getSizeInBytes(), 0L);
                blocks[i] = block;
            }
        }
        return new Page(page.getPositionCount(), blocks);
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
        private final List<ColumnHandle> columns;
        private final List<Type> types;
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
                Iterable<ColumnHandle> columns,
                List<Type> types,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
            this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.types = requireNonNull(types, "types is null");
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
                    columns,
                    types,
                    new MergingPageOutput(types, minOutputPageSize.toBytes(), minOutputPageRowCount));
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    @Override
    public boolean retainsInputPages()
    {
        return false;
    }

    @Override
    public void enableOutputPageReuse()
    {
        reusePages = true;
    }
}
