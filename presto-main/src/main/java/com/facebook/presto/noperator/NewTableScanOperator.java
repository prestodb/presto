/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NewTableScanOperator
        implements NewSourceOperator
{
    public static class NewTableScanOperatorFactory
            implements NewSourceOperatorFactory
    {
        private final PlanNodeId sourceId;
        private final DataStreamProvider dataStreamProvider;
        private final List<TupleInfo> tupleInfos;
        private final List<ColumnHandle> columns;
        private boolean closed;

        public NewTableScanOperatorFactory(PlanNodeId sourceId, DataStreamProvider dataStreamProvider, List<TupleInfo> tupleInfos, Iterable<ColumnHandle> columns)
        {
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
        public NewSourceOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
        {
            checkState(!closed, "Factory is already closed");
            return new NewTableScanOperator(sourceId, operatorStats, dataStreamProvider, tupleInfos, columns);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final PlanNodeId planNodeId;
    private final OperatorStats operatorStats;
    private final DataStreamProvider dataStreamProvider;
    private final List<TupleInfo> tupleInfos;
    private final List<ColumnHandle> columns;

    @GuardedBy("this")
    private NewOperator source;

    public NewTableScanOperator(PlanNodeId planNodeId,
            OperatorStats operatorStats,
            DataStreamProvider dataStreamProvider,
            List<TupleInfo> tupleInfos,
            Iterable<ColumnHandle> columns)
    {
        this.planNodeId = planNodeId;
        this.operatorStats = checkNotNull(operatorStats, "operatorStats is null");
        this.tupleInfos = checkNotNull(tupleInfos, "tupleInfos is null");
        this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
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

        source = dataStreamProvider.createNewDataStream(split, columns);
    }

    @Override
    public synchronized void noMoreSplits()
    {
        if (source == null) {
            source = new FinishedOperator(tupleInfos);
        }
    }

    private synchronized NewOperator getSource()
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
        NewOperator delegate = getSource();
        if (delegate == null) {
            return;
        }
        delegate.finish();
    }

    @Override
    public boolean isFinished()
    {
        NewOperator delegate = getSource();
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
        NewOperator delegate = getSource();
        if (delegate == null) {
            return null;
        }
        return delegate.getOutput();
    }

}
