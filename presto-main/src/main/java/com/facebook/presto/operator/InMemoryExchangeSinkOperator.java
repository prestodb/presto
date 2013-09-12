package com.facebook.presto.operator;

import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class InMemoryExchangeSinkOperator
        implements NewOperator
{
    private final OperatorContext operatorContext;
    private final InMemoryExchange inMemoryExchange;
    private boolean finished;

    InMemoryExchangeSinkOperator(OperatorContext operatorContext, InMemoryExchange inMemoryExchange)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.inMemoryExchange = checkNotNull(inMemoryExchange, "inMemoryExchange is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return inMemoryExchange.getTupleInfos();
    }

    @Override
    public void finish()
    {
        if (!finished) {
            finished = true;
            inMemoryExchange.sinkFinished();
        }
    }

    @Override
    public boolean isFinished()
    {
        if (!finished) {
            finished = inMemoryExchange.isFinishing();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return !isFinished();
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finished, "Already finished");
        inMemoryExchange.addPage(page);
        operatorContext.recordGeneratedOutput(page.getDataSize(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
