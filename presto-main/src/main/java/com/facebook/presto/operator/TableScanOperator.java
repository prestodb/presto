/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.spi.Split;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class TableScanOperator
        implements SourceOperator
{
    private final DataStreamProvider dataStreamProvider;
    private final List<TupleInfo> tupleInfos;
    private final List<ColumnHandle> columns;
    private Operator delegate;

    public TableScanOperator(DataStreamProvider dataStreamProvider, List<TupleInfo> tupleInfos, Iterable<ColumnHandle> columns)
    {
        Preconditions.checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        Preconditions.checkNotNull(columns, "columns is null");

        this.tupleInfos = tupleInfos;
        this.dataStreamProvider = dataStreamProvider;
        this.columns = ImmutableList.copyOf(columns);
    }

    @Override
    public synchronized void addSplit(Split split)
    {
        Preconditions.checkState(delegate == null, "Table scan split already set");
        delegate = dataStreamProvider.createDataStream(split, columns);
    }

    @Override
    public void noMoreSplits()
    {
    }

    @Override
    public int getChannelCount()
    {
        return tupleInfos.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        Operator delegate;
        synchronized (this) {
            delegate = this.delegate;
        }
        if (delegate == null) {
            // No splits were added
            return PageIterators.emptyIterator(tupleInfos);
        }
        return delegate.iterator(operatorStats);
    }

    public RecordCursor cursor()
    {
        RecordProjectOperator delegate;
        synchronized (this) {
            Preconditions.checkState(this.delegate != null, "Table scan split must be set");
            delegate = (RecordProjectOperator) this.delegate;
        }
        return delegate.cursor();
    }
}
