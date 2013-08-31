package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class InMemoryExchangeSourceOperator
        implements NewOperator
{
    public static class InMemoryExchangeSourceOperatorFactory
            implements NewOperatorFactory
    {
        private final InMemoryExchange inMemoryExchange;
        private boolean closed;

        public InMemoryExchangeSourceOperatorFactory(InMemoryExchange inMemoryExchange)
        {
            this.inMemoryExchange = inMemoryExchange;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return inMemoryExchange.getTupleInfos();
        }

        @Override
        public NewOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
        {
            checkState(!closed, "Factory is already closed");
            return new InMemoryExchangeSourceOperator(inMemoryExchange);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final InMemoryExchange exchange;

    public InMemoryExchangeSourceOperator(InMemoryExchange exchange)
    {
        this.exchange = exchange;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return exchange.getTupleInfos();
    }

    @Override
    public void finish()
    {
        exchange.finish();
    }

    @Override
    public boolean isFinished()
    {
        return exchange.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = exchange.waitForNotEmpty();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        return exchange.removePage();
    }
}
