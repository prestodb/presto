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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static com.facebook.presto.operator.FinishedPageSource.FINISHED_PAGE_SOURCE;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TableScanOperator
        implements SourceOperator, Closeable
{
    public static class TableScanOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final PageSourceProvider pageSourceProvider;
        private final List<Type> types;
        private final List<ColumnHandle> columns;
        private boolean closed;

        public TableScanOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                PageSourceProvider pageSourceProvider,
                List<Type> types,
                Iterable<ColumnHandle> columns)
        {
            this.operatorId = operatorId;
            this.sourceId = checkNotNull(sourceId, "sourceId is null");
            this.types = checkNotNull(types, "types is null");
            this.pageSourceProvider = checkNotNull(pageSourceProvider, "pageSourceManager is null");
            this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, TableScanOperator.class.getSimpleName());
            return new TableScanOperator(
                    operatorContext,
                    sourceId,
                    pageSourceProvider,
                    types,
                    columns);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final PageSourceProvider pageSourceProvider;
    private final List<Type> types;
    private final List<ColumnHandle> columns;
    private final SettableFuture<?> blocked;

    @GuardedBy("this")
    private ConnectorPageSource source;

    private long completedBytes;
    private long readTimeNanos;

    public TableScanOperator(
            OperatorContext operatorContext,
            PlanNodeId planNodeId,
            PageSourceProvider pageSourceProvider,
            List<Type> types,
            Iterable<ColumnHandle> columns)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.planNodeId = checkNotNull(planNodeId, "planNodeId is null");
        this.types = checkNotNull(types, "types is null");
        this.pageSourceProvider = checkNotNull(pageSourceProvider, "pageSourceManager is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
        this.blocked = SettableFuture.create();
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
    public synchronized void addSplit(Split split)
    {
        checkNotNull(split, "split is null");
        checkState(getSource() == null, "Table scan split already set");

        source = pageSourceProvider.createPageSource(split, columns);

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(Suppliers.ofInstance(splitInfo));
        }
        blocked.set(null);
    }

    @Override
    public synchronized void noMoreSplits()
    {
        if (source == null) {
            source = FINISHED_PAGE_SOURCE;
        }
    }

    private synchronized ConnectorPageSource getSource()
    {
        return source;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public synchronized void close()
    {
        finish();
    }

    @Override
    public void finish()
    {
        ConnectorPageSource delegate = getSource();
        if (delegate == null) {
            return;
        }
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean isFinished()
    {
        ConnectorPageSource delegate = getSource();
        return delegate != null && delegate.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ConnectorPageSource delegate = getSource();
        if (delegate != null) {
            return NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        ConnectorPageSource delegate = getSource();
        if (delegate == null) {
            return null;
        }

        Page page = delegate.getNextPage();
        if (page != null) {
            // assure the page is in memory before handing to another operator
            page.assureLoaded();

            // update operator stats
            long endCompletedBytes = delegate.getCompletedBytes();
            long endReadTimeNanos = delegate.getReadTimeNanos();
            operatorContext.recordGeneratedInput(endCompletedBytes - completedBytes, page.getPositionCount(), endReadTimeNanos - readTimeNanos);
            completedBytes = endCompletedBytes;
            readTimeNanos = endReadTimeNanos;
        }

        return page;
    }
}
