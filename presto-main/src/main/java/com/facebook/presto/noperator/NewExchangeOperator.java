/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NewExchangeOperator
        implements NewSourceOperator
{
    public static class NewExchangeOperatorFactory
            implements NewSourceOperatorFactory
    {
        private final PlanNodeId sourceId;
        private final Supplier<ExchangeClient> exchangeClientSupplier;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public NewExchangeOperatorFactory(PlanNodeId sourceId, Supplier<ExchangeClient> exchangeClientSupplier, List<TupleInfo> tupleInfos)
        {
            this.sourceId = sourceId;
            this.exchangeClientSupplier = exchangeClientSupplier;
            this.tupleInfos = tupleInfos;
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

            return new NewExchangeOperator(sourceId, exchangeClientSupplier.get(), tupleInfos);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final PlanNodeId sourceId;
    private final ExchangeClient exchangeClient;
    private final List<TupleInfo> tupleInfos;

    public NewExchangeOperator(PlanNodeId sourceId, ExchangeClient exchangeClient, List<TupleInfo> tupleInfos)
    {
        this.sourceId = checkNotNull(sourceId, "sourceId is null");
        this.exchangeClient = checkNotNull(exchangeClient, "exchangeClient is null");
        this.tupleInfos = checkNotNull(tupleInfos, "tupleInfos is null");
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public void addSplit(Split split)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof RemoteSplit, "split is not a remote split");

        URI location = ((RemoteSplit) split).getLocation();
        exchangeClient.addLocation(location);
    }

    @Override
    public void noMoreSplits()
    {
        exchangeClient.noMoreLocations();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public void finish()
    {
        exchangeClient.close();
    }

    @Override
    public boolean isFinished()
    {
        return exchangeClient.isClosed();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = exchangeClient.isBlocked();
        if (blocked.isDone()) {
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
        Page page = exchangeClient.pollPage();
        return page;
    }
}
