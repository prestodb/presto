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

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.facebook.presto.sql.analyzer.FeaturesConfig.ProcessingOptimization.COLUMNAR;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.ProcessingOptimization.COLUMNAR_DICTIONARY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.ProcessingOptimization.DISABLED;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

public class ScanFilterAndProjectOperator
        implements SourceOperator, Closeable
{
    private static final int ROWS_PER_PAGE = 16384;

    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final PageSourceProvider pageSourceProvider;
    private final List<Type> types;
    private final List<ColumnHandle> columns;
    private final PageBuilder pageBuilder;
    private final CursorProcessor cursorProcessor;
    private final PageProcessor pageProcessor;
    private final LocalMemoryContext pageSourceMemoryContext;
    private final LocalMemoryContext pageBuilderMemoryContext;
    private final SettableFuture<?> blocked = SettableFuture.create();
    private final String processingOptimization;

    private RecordCursor cursor;
    private ConnectorPageSource pageSource;

    private Split split;
    private Page currentPage;
    private int currentPosition;

    private boolean finishing;

    private long completedBytes;
    private long readTimeNanos;

    protected ScanFilterAndProjectOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            PageSourceProvider pageSourceProvider,
            CursorProcessor cursorProcessor,
            PageProcessor pageProcessor,
            Iterable<ColumnHandle> columns,
            Iterable<Type> types)
    {
        this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
        this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.planNodeId = requireNonNull(sourceId, "sourceId is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.pageSourceMemoryContext = operatorContext.getSystemMemoryContext().newLocalMemoryContext();
        this.pageBuilderMemoryContext = operatorContext.getSystemMemoryContext().newLocalMemoryContext();
        this.processingOptimization = getProcessingOptimization(operatorContext, pageProcessor.isFiltering());

        this.pageBuilder = new PageBuilder(getTypes());
    }

    private String getProcessingOptimization(OperatorContext operatorContext, boolean hasFilter)
    {
        String processingOptimization = SystemSessionProperties.getProcessingOptimization(operatorContext.getSession());
        if (!hasFilter && processingOptimization.equals(FeaturesConfig.ProcessingOptimization.DISABLED)) {
            return FeaturesConfig.ProcessingOptimization.COLUMNAR;
        }
        else {
            return processingOptimization;
        }
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
        }
        blocked.set(null);
    }

    @Override
    public final List<Type> getTypes()
    {
        return types;
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
                throw Throwables.propagate(e);
            }
        }
        else if (cursor != null) {
            cursor.close();
        }
        finishing = true;
    }

    @Override
    public final boolean isFinished()
    {
        if (pageSource != null && pageSource.isFinished() && currentPage == null) {
            finishing = true;
        }

        return finishing && pageBuilder.isEmpty();
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

        if (!finishing) {
            if ((pageSource == null) && (cursor == null)) {
                ConnectorPageSource source = pageSourceProvider.createPageSource(operatorContext.getSession(), split, columns);
                if (source instanceof RecordPageSource) {
                    cursor = ((RecordPageSource) source).getCursor();
                }
                else {
                    pageSource = source;
                }
            }

            if (cursor != null) {
                int rowsProcessed = cursorProcessor.process(operatorContext.getSession().toConnectorSession(), cursor, ROWS_PER_PAGE, pageBuilder);

                pageSourceMemoryContext.setBytes(cursor.getSystemMemoryUsage());

                long bytesProcessed = cursor.getCompletedBytes() - completedBytes;
                long elapsedNanos = cursor.getReadTimeNanos() - readTimeNanos;
                operatorContext.recordGeneratedInput(bytesProcessed, rowsProcessed, elapsedNanos);
                completedBytes = cursor.getCompletedBytes();
                readTimeNanos = cursor.getReadTimeNanos();

                if (rowsProcessed == 0) {
                    finishing = true;
                }
            }
            else {
                if (currentPage == null) {
                    currentPage = pageSource.getNextPage();

                    if (currentPage != null) {
                        // update operator stats
                        long endCompletedBytes = pageSource.getCompletedBytes();
                        long endReadTimeNanos = pageSource.getReadTimeNanos();
                        operatorContext.recordGeneratedInput(endCompletedBytes - completedBytes, currentPage.getPositionCount(), endReadTimeNanos - readTimeNanos);
                        completedBytes = endCompletedBytes;
                        readTimeNanos = endReadTimeNanos;
                    }

                    currentPosition = 0;
                }

                if (currentPage != null) {
                    switch (processingOptimization) {
                        case COLUMNAR: {
                            Page page = pageProcessor.processColumnar(operatorContext.getSession().toConnectorSession(), currentPage, getTypes());
                            currentPage = null;
                            currentPosition = 0;
                            return page;
                        }
                        case COLUMNAR_DICTIONARY: {
                            Page page = pageProcessor.processColumnarDictionary(operatorContext.getSession().toConnectorSession(), currentPage, getTypes());
                            currentPage = null;
                            currentPosition = 0;
                            return page;
                        }
                        case DISABLED: {
                            currentPosition = pageProcessor.process(operatorContext.getSession().toConnectorSession(), currentPage, currentPosition, currentPage.getPositionCount(), pageBuilder);
                            if (currentPosition == currentPage.getPositionCount()) {
                                currentPage = null;
                                currentPosition = 0;
                            }
                            break;
                        }
                        default:
                            throw new IllegalStateException(String.format("Found unexpected value %s for processingOptimization", processingOptimization));
                    }
                }

                pageSourceMemoryContext.setBytes(pageSource.getSystemMemoryUsage());
            }
        }

        // only return a full page if buffer is full or we are finishing
        if (pageBuilder.isEmpty() || (!finishing && !pageBuilder.isFull())) {
            pageBuilderMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();

        pageBuilderMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
        return page;
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
        private boolean closed;

        public ScanFilterAndProjectOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PlanNodeId sourceId,
                PageSourceProvider pageSourceProvider,
                Supplier<CursorProcessor> cursorProcessor,
                Supplier<PageProcessor> pageProcessor,
                Iterable<ColumnHandle> columns,
                List<Type> types)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
            this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
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
                    types);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }
}
