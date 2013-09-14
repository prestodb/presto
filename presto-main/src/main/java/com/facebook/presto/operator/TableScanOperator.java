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
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TableScanOperator
        implements SourceOperator
{
    public static class TableScanOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final DataStreamProvider dataStreamProvider;
        private final List<TupleInfo> tupleInfos;
        private final List<ColumnHandle> columns;
        private boolean closed;

        public TableScanOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                DataStreamProvider dataStreamProvider,
                List<TupleInfo> tupleInfos,
                Iterable<ColumnHandle> columns)
        {
            this.operatorId = operatorId;
            this.sourceId = checkNotNull(sourceId, "sourceId is null");
            this.tupleInfos = checkNotNull(tupleInfos, "tupleInfos is null");
            this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
            this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, TableScanOperator.class.getSimpleName());
            return new TableScanOperator(
                    operatorContext,
                    sourceId,
                    dataStreamProvider,
                    tupleInfos,
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
    private final DataStreamProvider dataStreamProvider;
    private final List<TupleInfo> tupleInfos;
    private final List<ColumnHandle> columns;

    @GuardedBy("this")
    private Operator source;

    public TableScanOperator(
            OperatorContext operatorContext,
            PlanNodeId planNodeId,
            DataStreamProvider dataStreamProvider,
            List<TupleInfo> tupleInfos,
            Iterable<ColumnHandle> columns)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.planNodeId = checkNotNull(planNodeId, "planNodeId is null");
        this.tupleInfos = checkNotNull(tupleInfos, "tupleInfos is null");
        this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
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
    public synchronized void addSplit(final Split split)
    {
        checkNotNull(split, "split is null");
        checkState(getSource() == null, "Table scan split already set");

        source = dataStreamProvider.createNewDataStream(operatorContext, split, columns);

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(Suppliers.ofInstance(splitInfo));
        }
    }

    @Override
    public synchronized void noMoreSplits()
    {
        if (source == null) {
            source = new FinishedOperator(operatorContext, tupleInfos);
        }
    }

    private synchronized Operator getSource()
    {
        return source;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public void finish()
    {
        Operator delegate = getSource();
        if (delegate == null) {
            return;
        }
        delegate.finish();
    }

    @Override
    public boolean isFinished()
    {
        Operator delegate = getSource();
        return delegate != null && delegate.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
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
        Operator delegate = getSource();
        if (delegate == null) {
            return null;
        }
        return delegate.getOutput();
    }
}
