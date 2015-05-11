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
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

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
    private final SettableFuture<?> blocked = SettableFuture.create();

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
        this.cursorProcessor = checkNotNull(cursorProcessor, "cursorProcessor is null");
        this.pageProcessor = checkNotNull(pageProcessor, "pageProcessor is null");
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.planNodeId = checkNotNull(sourceId, "sourceId is null");
        this.pageSourceProvider = checkNotNull(pageSourceProvider, "pageSourceManager is null");
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));

        this.pageBuilder = new PageBuilder(getTypes());
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
        checkNotNull(split, "split is null");
        checkState(this.split == null, "Table scan split already set");

        if (finishing) {
            return Optional::empty;
        }

        this.split = split;

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(() -> splitInfo);
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
        if (!finishing) {
            createSourceIfNecessary();
        }

        if (pageSource != null && pageSource.isFinished() && currentPage == null) {
            finishing = true;
        }

        return finishing && pageBuilder.isEmpty();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return blocked;
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
        if (!finishing) {
            createSourceIfNecessary();

            if (cursor != null) {
                int rowsProcessed = cursorProcessor.process(operatorContext.getSession().toConnectorSession(), cursor, ROWS_PER_PAGE, pageBuilder);
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
                    currentPosition = pageProcessor.process(operatorContext.getSession().toConnectorSession(), currentPage, currentPosition, currentPage.getPositionCount(), pageBuilder);
                    if (currentPosition == currentPage.getPositionCount()) {
                        currentPage = null;
                        currentPosition = 0;
                    }
                }
            }
        }

        // only return a full page if buffer is full or we are finishing
        if (pageBuilder.isEmpty() || (!finishing && !pageBuilder.isFull())) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private void createSourceIfNecessary()
    {
        if ((split != null) && (pageSource == null) && (cursor == null)) {
            ConnectorPageSource source = pageSourceProvider.createPageSource(split, columns);
            if (source instanceof RecordPageSource) {
                cursor = ((RecordPageSource) source).getCursor();
            }
            else {
                pageSource = source;
            }
        }
    }

    public static class ScanFilterAndProjectOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final CursorProcessor cursorProcessor;
        private final PageProcessor pageProcessor;
        private final PlanNodeId sourceId;
        private final PageSourceProvider pageSourceProvider;
        private final List<ColumnHandle> columns;
        private final List<Type> types;
        private boolean closed;

        public ScanFilterAndProjectOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                PageSourceProvider pageSourceProvider,
                CursorProcessor cursorProcessor,
                PageProcessor pageProcessor,
                Iterable<ColumnHandle> columns,
                List<Type> types)
        {
            this.operatorId = operatorId;
            this.cursorProcessor = checkNotNull(cursorProcessor, "cursorProcessor is null");
            this.pageProcessor = checkNotNull(pageProcessor, "pageProcessor is null");
            this.sourceId = checkNotNull(sourceId, "sourceId is null");
            this.pageSourceProvider = checkNotNull(pageSourceProvider, "pageSourceProvider is null");
            this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
            this.types = checkNotNull(types, "types is null");
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, ScanFilterAndProjectOperator.class.getSimpleName());
            return new ScanFilterAndProjectOperator(
                    operatorContext,
                    sourceId,
                    pageSourceProvider,
                    cursorProcessor,
                    pageProcessor,
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
