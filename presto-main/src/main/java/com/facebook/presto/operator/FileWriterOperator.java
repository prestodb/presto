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

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.PageSinkManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.facebook.airlift.concurrent.MoreFutures.toListenableFuture;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.PageSinkContext.defaultContext;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static java.util.Objects.requireNonNull;

public class FileWriterOperator
        implements Operator
{
    public static class FileWriterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PageSinkManager pageSinkManager;
        private final Session session;
        private boolean closed;
        private final String path;
        private final List<String> columnsNames;
        private final List<Type> columnTypes;
        private final Function<Page, Page> pagePreprocessor;

        public FileWriterOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PageSinkManager pageSinkManager,
                Session session,
                String path,
                List<String> columnNames,
                List<Type> columnTypes,
                Function<Page, Page> pagePreprocessor)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
            this.session = session;
            this.path = requireNonNull(path, "path is null");
            this.columnsNames = requireNonNull(columnNames, "columnsNames is null");
            this.columnTypes = requireNonNull(columnTypes, "columnsTypes is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableWriterOperator.class.getSimpleName());
            return new FileWriterOperator(
                    context,
                    createPageSink(),
                    pagePreprocessor);
        }

        private ConnectorPageSink createPageSink()
        {
            return pageSinkManager.createPageSink(session, path, columnsNames, columnTypes, defaultContext());
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new FileWriterOperatorFactory(operatorId, planNodeId, pageSinkManager, session, path, columnsNames, columnTypes, pagePreprocessor);
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private final ConnectorPageSink pageSink;
    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private State state = State.RUNNING;
    private long rowCount;
    private boolean closed;
    private final Function<Page, Page> pagePreprocessor;

    public FileWriterOperator(
            OperatorContext operatorContext,
            ConnectorPageSink pageSink,
            Function<Page, Page> pagePreprocessor)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pageSink = requireNonNull(pageSink, "pageSink is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        ListenableFuture<?> currentlyBlocked = blocked;
        ListenableFuture<?> blockedOnFinish = NOT_BLOCKED;
        if (state == State.RUNNING) {
            state = State.FINISHING;
            blockedOnFinish = toListenableFuture(pageSink.finish());
        }
        this.blocked = allAsList(currentlyBlocked, blockedOnFinish);
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED && blocked.isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        if (state != State.RUNNING || !blocked.isDone()) {
            return false;
        }
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(needsInput(), "Operator does not need input");

        if (page.getChannelCount() <= 0) {
            return;
        }
        page = pagePreprocessor.apply(page);

        CompletableFuture<?> future = pageSink.appendPage(page);
        blocked = toListenableFuture(future);
        rowCount += page.getPositionCount();
    }

    @Override
    public Page getOutput()
    {
        if (!blocked.isDone()) {
            return null;
        }

        if (state != State.FINISHING) {
            return null;
        }

        state = State.FINISHED;
        BlockBuilder rowsBuilder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(rowsBuilder, rowCount);
        return new Page(1, rowsBuilder.build());
    }
}
