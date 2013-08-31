package com.facebook.presto.noperator;

import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class InMemoryExchangeSourceOperator
        implements NewOperator
{
    public static class InMemoryExchangeSourceOperatorFactory
            implements NewOperatorFactory
    {
        private final int operatorId;
        private final InMemoryExchange inMemoryExchange;
        private boolean closed;

        public InMemoryExchangeSourceOperatorFactory(int operatorId, InMemoryExchange inMemoryExchange)
        {
            this.operatorId = operatorId;
            this.inMemoryExchange = inMemoryExchange;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return inMemoryExchange.getTupleInfos();
        }

        @Override
        public NewOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, InMemoryExchangeSourceOperator.class.getSimpleName());
            return new InMemoryExchangeSourceOperator(operatorContext, inMemoryExchange);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final InMemoryExchange exchange;

    public InMemoryExchangeSourceOperator(OperatorContext operatorContext, InMemoryExchange exchange)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.exchange = checkNotNull(exchange, "exchange is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
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
